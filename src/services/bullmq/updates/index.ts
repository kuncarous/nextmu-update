import { AwsCrc32 } from '@aws-crypto/crc32';
import { ppath } from '@yarnpkg/fslib';
import { ZipFS } from '@yarnpkg/libzip';
import * as BullMQ from 'bullmq';
import fs, { promises as fsAsync } from 'fs';
import fsExtra from 'fs-extra';
import moment from 'moment';
import { ObjectId } from 'mongodb';
import path, { resolve } from 'node:path';
import { promisify } from 'node:util';
import { v4 as uuidv4 } from 'uuid-mongodb';
import zlib from 'zlib';
import { VersionState } from '~/proto/update_pb';
import { getMongoClient } from '~/services/mongodb/client';
import { IMDBUpdateFile } from '~/services/mongodb/schemas/updates/files';
import { IMDBVersion } from '~/services/mongodb/schemas/updates/versions';
import { fixedProgress } from '~/shared';
import {
    incomingFolders,
    incomingUpdatesPath,
    processedUpdatesPath,
    UpdateTypeLookup,
} from '~/shared/update';
import { RedisConnection } from '../../redis';
import { UpdateJobData } from './types';

export const UpdatesQueueName =
    process.env.UPDATES_QUEUE_NAME ?? 'updatesQueueDev';
export const UpdatesQueue = new BullMQ.Queue<UpdateJobData>(UpdatesQueueName, {
    connection: RedisConnection,
});

const UseSourceLocalStorage =
    (process.env.UPDATE_SOURCE_PROVIDER || 'local') == 'local';
const UseOutputLocalStorage =
    (process.env.UPDATE_OUTPUT_PROVIDER || 'local') == 'local';
const SourceLocalStorageDir =
    process.env.UPDATE_SOURCE_DIRECTORY || '../updates-in';
const OutputLocalStorageDir =
    process.env.UPDATE_OUTPUT_DIRECTORY || '../updates-out';
const UpdatePrefixDir = process.env.UPDATE_PREFIX_DIR || '';

const fsReadFileAsync = promisify(fs.readFile);
const fsWriteFileAsync = promisify(fs.writeFile);
const fsReaddirAsync = promisify(fs.readdir);

const zlibDeflateAsync = (buffer: zlib.InputType): Promise<Buffer> => {
    return new Promise<Buffer>((resolve, reject) => {
        zlib.deflate(
            buffer,
            {
                level: 9,
            },
            (error, result) => {
                if (error) {
                    reject(error);
                } else {
                    resolve(result);
                }
            },
        );
    });
};

const deleteDirectory = async (path: string) => {
    if (!fs.existsSync(path)) return;
    try {
        await fsAsync.rm(path, { recursive: true, force: true });
    } catch (error) {
        throw new Error(
            `Failed : failed to clean output path (${path})|(${error})`,
        );
    }
};

interface IFileInfo {
    // Before process
    localPath: string;

    // After process info
    filename?: string;
    extension?: string;
    crc32?: string;
    fileSize?: number;
    packedSize?: number;
}

const enumerateFiles = async (
    path: string,
    relativePath: string = '',
): Promise<IFileInfo[]> => {
    const files: IFileInfo[] = [];
    if (!fs.existsSync(path)) return files;

    const dirents = await fsReaddirAsync(path, { withFileTypes: true });
    for (const dirent of dirents) {
        if (dirent.isFile()) {
            files.push({
                localPath: relativePath + dirent.name,
            });
        }
    }
    for (const dirent of dirents) {
        if (dirent.isDirectory()) {
            files.push(
                ...(await enumerateFiles(
                    path + dirent.name + '/',
                    relativePath + dirent.name + '/',
                )),
            );
        }
    }

    return files;
};

async function makeDirectory(directory: string, recursive: boolean = false) {
    try {
        await fsAsync.mkdir(directory, { recursive });
    } catch (e) {
        /* empty */
    }
}

async function unzipFile(filename: string, outputDir: string) {
    try {
        const path = ppath.resolve(filename);
        const fs = new ZipFS(path, { create: false, readOnly: true });
        const files = fs.getAllFiles();
        await makeDirectory(outputDir, true);
        for (const filePath of files) {
            const stat = await fs.statPromise(filePath);
            const outputPath = resolve(outputDir, './' + filePath.toString());
            if (stat.isDirectory()) {
                await makeDirectory(outputPath);
                continue;
            }
            const data = Buffer.from(
                await fs.readFilePromise(filePath, 'base64'),
                'base64',
            );
            await fsAsync.writeFile(outputPath, data);
        }
    } catch (err) {
        console.log(err);
    }
}

const reportProcessFiles = (
    job: BullMQ.Job,
    processedCount: number,
    filesCount: number,
    progress: number[],
) => {
    job.updateProgress(
        fixedProgress(
            progress[0] + progress[1] * (processedCount / filesCount),
        ),
    );
};

const processUpdateJob = async (job: BullMQ.Job<UpdateJobData>) => {
    const { versionId } = job.data;
    const uploadPath = versionId.toUpperCase() + '/';
    const incomingPath = incomingUpdatesPath + uploadPath;
    const processedPath = processedUpdatesPath + uploadPath;

    await deleteDirectory(incomingPath);
    await deleteDirectory(processedPath);
    await fsExtra.ensureDir(processedPath);

    try {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const downloadProgress = [0, 20];
        const processProgress = [20, 50];
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const uploadProgress = [70, 20];

        if (UseSourceLocalStorage) {
            const inputZip = resolve(SourceLocalStorageDir, `${versionId}.zip`);
            unzipFile(inputZip, incomingUpdatesPath);
        } else {
            throw new Error(`not implemented yet`);
        }
        /*await downloadFromBucket(
            process.env.GCP_UPDATE_IN!,
            uploadPath,
            incomingUpdatesPath,
            (progress) => {
                job.updateProgress(fixedProgress(downloadProgress[0] + downloadProgress[1] * progress));
            }
        );*/

        const filesList: Array<IFileInfo[]> = new Array<IFileInfo[]>(
            incomingFolders.length,
        );
        let filesCount = 0;
        let processedCount = 0;
        let reportCounter = 0;

        for (let n = 0; n < incomingFolders.length; ++n) {
            filesList[n] = await enumerateFiles(
                incomingPath + incomingFolders[n] + '/',
            );
            filesCount += filesList[n].length;
        }

        if (filesCount === 0) {
            throw new Error(`Failed : empty update folder`);
        }

        /* Process files */
        for (let n = 0; n < UpdateTypeLookup.length; ++n) {
            const files = filesList[n];
            const basePath = incomingPath + incomingFolders[n] + '/';

            for (let j = 0; j < files.length; ++j) {
                const file = files[j];
                const filePath = basePath + file.localPath;
                const fileBuffer = await fsReadFileAsync(filePath);
                const crc32 = new AwsCrc32();
                crc32.update(fileBuffer);
                const fileCrc32 = await crc32.digest();
                if (!fileCrc32) {
                    throw new Error(`Failed : failed to calculate CRC-32`);
                }

                const compressedBuffer = await zlibDeflateAsync(fileBuffer);
                file.crc32 = Buffer.from(fileCrc32).toString('hex');
                file.filename = (uuidv4() + `_${file.crc32}`).toUpperCase();
                file.extension = '.eupdz';
                file.fileSize = fileBuffer.length;
                file.packedSize = compressedBuffer.length;

                const dest = path
                    .join(processedPath, file.filename + file.extension)
                    .replace(/\\/g, '/');
                const destDir = dest.substring(0, dest.lastIndexOf('/'));
                await fsExtra.ensureDir(destDir);
                await fsWriteFileAsync(
                    processedPath + file.filename + file.extension,
                    compressedBuffer,
                );

                ++processedCount;
                if (++reportCounter >= 100) {
                    reportProcessFiles(
                        job,
                        processedCount,
                        filesCount,
                        processProgress,
                    );
                }
            }
        }

        reportProcessFiles(job, processedCount, filesCount, processProgress);

        if (UseOutputLocalStorage) {
            const outputDir = resolve(
                OutputLocalStorageDir,
                UpdatePrefixDir,
                uploadPath,
            );
            await fsAsync.rename(processedPath, outputDir);
        } else {
            throw new Error(`not implemented yet`);
        }
        /*await uploadToBucket(
            process.env.GCP_UPDATE_OUT!,
            processedPath,
            uploadPath,
            _.flatten(filesList).map(
                file => `${file.filename}${file.extension}`
            ),
            (progress) => {
                job.updateProgress(fixedProgress(uploadProgress[0] + uploadProgress[1] * progress));
            }
        );*/

        /* Insert files to database update table */
        const client = await getMongoClient();
        if (!client) {
            throw new Error(`getMongoClient failed`);
        }

        const createdAt = moment().toDate();
        const versionOid = new ObjectId(versionId);
        const files: IMDBUpdateFile[] = [];
        for (let n = 0; n < UpdateTypeLookup.length; ++n) {
            const categoryFiles = filesList[n];
            files.push(
                ...categoryFiles.map<IMDBUpdateFile>((file) => ({
                    versionId: versionOid,
                    category: UpdateTypeLookup[n],
                    fileName: file.filename!,
                    extension: file.extension!,
                    localPath: file.localPath!,
                    packedSize: file.packedSize!,
                    fileSize: file.fileSize!,
                    crc32: file.crc32!,
                    createdAt,
                })),
            );
        }

        const session = client.startSession();
        session.startTransaction();
        try {
            const filesColl = client
                .db('updates')
                .collection<IMDBUpdateFile>('files');
            await filesColl.insertMany(files, {
                session,
            });

            const versionsColl = client
                .db('updates')
                .collection<IMDBVersion>('versions');
            await versionsColl.updateOne(
                {
                    _id: versionOid,
                },
                {
                    $set: {
                        state: VersionState.READY,
                    },
                },
                {
                    session,
                },
            );

            await session.commitTransaction();
        } catch (error) {
            await session.abortTransaction();
            throw error;
        } finally {
            await session.endSession();
        }
    } finally {
        await deleteDirectory(incomingPath);
        await deleteDirectory(processedPath);
    }
};

export const UpdatesWorker =
    Number(process.env.UPDATES_QUEUE_PROCESS ?? 1) > 0
        ? new BullMQ.Worker(UpdatesQueueName, processUpdateJob, {
              connection: RedisConnection,
          })
        : null;

export * from './types';
