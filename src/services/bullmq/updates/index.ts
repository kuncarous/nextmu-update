import { AwsCrc32 } from '@aws-crypto/crc32';
import { ppath } from '@yarnpkg/fslib';
import { ZipFS } from '@yarnpkg/libzip';
import * as BullMQ from 'bullmq';
import fsExtra from 'fs-extra';
import moment from 'moment';
import { ObjectId } from 'mongodb';
import fs, {
    createReadStream,
    createWriteStream,
    promises as fsAsync,
} from 'node:fs';
import { mkdir, rm, stat } from 'node:fs/promises';
import path, { resolve } from 'node:path';
import MUUID from 'uuid-mongodb';
import zlib from 'zlib';
import { UploadState } from '~/proto/nextmu/v1/UploadState';
import { VersionState } from '~/proto/nextmu/v1/VersionState';
import { getMongoClient } from '~/services/mongodb/client';
import { IMDBUpdateFile } from '~/services/mongodb/schemas/updates/files';
import { IMDBUpload } from '~/services/mongodb/schemas/updates/uploads';
import { IMDBVersion } from '~/services/mongodb/schemas/updates/versions';
import {
    deleteUploadChunks,
    setUploadState,
    setVersionState,
} from '~/services/mongodb/update';
import {
    deleteFolder,
    downloadFile,
    downloadFolder,
    uploadFile,
    uploadFolder,
} from '~/services/storage';
import { StorageType } from '~/services/storage/enums';
import { fixedProgress } from '~/shared';
import {
    UpdateTypeLookup,
    incomingFolders,
    incomingFoldersRegexes,
    incomingUpdatesPath,
    processedUpdatesPath,
} from '~/shared/update';
import {
    FileInfo,
    enumerateFiles,
    getInputFolder,
    getUploadFile,
} from '~/utils';
import { calculateFileHash } from '~/utils/hash';
import { RedisConnection } from '../../redis';
import {
    IProcessPublishVersionJobData,
    IProcessUploadVersionJobData,
    UpdateJobData,
    UpdateServiceJobType,
} from './types';

export const UpdatesQueueName =
    process.env.UPDATES_QUEUE_NAME ?? 'updatesQueueDev';
export const UpdatesQueue = new BullMQ.Queue<UpdateJobData>(UpdatesQueueName, {
    connection: RedisConnection,
});

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

interface FileInfoExt extends FileInfo {
    // After process info
    filename?: string;
    extension?: string;
    crc32?: string;
    fileSize?: number;
    packedSize?: number;
}

async function makeDirectory(directory: string, recursive: boolean = false) {
    try {
        await fsAsync.mkdir(directory, { recursive });
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
    } catch (_) {
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
            progress[0] +
                (progress[1] - progress[0]) * (processedCount / filesCount),
        ),
    );
};

const getJobProgress = (low: number, high: number, progress: number) =>
    low + (high - low) * progress;

const processUploadVersion = async (
    job: BullMQ.Job<UpdateJobData>,
    data: IProcessUploadVersionJobData,
) => {
    const { versionId, uploadId, concurrentId } = data;
    const _uploadId = new ObjectId(uploadId);
    const _concurrentId = new ObjectId(concurrentId);

    const client = await getMongoClient();
    if (!client) {
        throw new Error(`getMongoClient failed`);
    }

    const uploadsColl = client.db('updates').collection<IMDBUpload>('uploads');

    const upload = await uploadsColl.findOne({
        _id: _uploadId,
        concurrentId: new ObjectId(concurrentId),
    });
    if (upload == null || upload.versionId.equals(versionId) == false) {
        return;
    }

    await setUploadState(
        _uploadId,
        _concurrentId,
        UploadState.PROCESSING,
        UploadState.PENDING,
    );

    const uploadPath = `uploads/${versionId.toUpperCase()}-${uploadId.toUpperCase()}-${concurrentId.toUpperCase()}/`;
    const incomingPath = path.join(incomingUpdatesPath, uploadPath);
    const processedPath = path.join(processedUpdatesPath, uploadPath);

    await deleteDirectory(incomingPath);
    await deleteDirectory(processedPath);
    await mkdir(incomingPath, { recursive: true });
    await mkdir(processedPath, { recursive: true });

    try {
        const downloadProgress = [0, 50];
        const processProgress = [50, 90];
        const uploadProgress = [90, 100];

        const filesPrefix = getInputFolder(uploadId, upload.hash, concurrentId);
        await downloadFolder(StorageType.Input, filesPrefix, incomingPath, {
            onProgress: (progress) =>
                job.updateProgress(
                    getJobProgress(
                        downloadProgress[0],
                        downloadProgress[1],
                        progress,
                    ),
                ),
        });

        const files: FileInfoExt[] = await enumerateFiles(incomingPath);
        for (const file of files) {
            const stats = await stat(path.join(incomingPath, file.path));
            file.fileSize = stats.size;
        }

        const readSize = files.reduce(
            (previousValue, currentValue) =>
                previousValue + currentValue.fileSize!,
            0,
        );
        let writtenSize = 0;

        const filename = path.join(processedPath, 'update.zip');
        const writeStream = createWriteStream(filename, 'binary');
        try {
            for (const file of files) {
                const readStream = createReadStream(file.fullPath, 'binary');

                await new Promise<void>((resolve, reject) => {
                    readStream.on('data', (chunk) => {
                        if (typeof chunk === 'string') return;
                        writtenSize += chunk.byteLength;
                        job.updateProgress(
                            getJobProgress(
                                processProgress[0],
                                processProgress[1],
                                writtenSize / readSize,
                            ),
                        );
                    });
                    readStream.on('error', (err) => reject(err));
                    readStream.on('end', () => resolve());

                    readStream.pipe(writeStream, { end: false });
                });
            }

            writeStream.end();
        } catch (e) {
            writeStream.end();
            await rm(filename);
            throw e;
        }

        const calculatedHash = await calculateFileHash(filename);
        if (calculatedHash !== upload.hash) {
            return;
        }

        await uploadFile(
            StorageType.Input,
            filename,
            getUploadFile(versionId),
            {
                onProgress: (progress) =>
                    job.updateProgress(
                        getJobProgress(
                            uploadProgress[0],
                            uploadProgress[1],
                            progress,
                        ),
                    ),
            },
        );
        await setUploadState(
            _uploadId,
            _concurrentId,
            UploadState.READY,
            UploadState.PROCESSING,
        );
        await deleteFolder(StorageType.Input, filesPrefix);
        await deleteUploadChunks(_uploadId);
    } finally {
        await deleteDirectory(incomingPath);
        await deleteDirectory(processedPath);
    }
};

const matchRegexes = (path: string, regexes: RegExp[]) => {
    for (const regex of regexes) {
        const match = path.match(regex);
        if (match) return match[1];
    }
    return null;
};

const processPublishVersion = async (
    job: BullMQ.Job<UpdateJobData>,
    data: IProcessPublishVersionJobData,
) => {
    const { versionId } = data;
    const _versionId = new ObjectId(versionId);
    const uploadPath = `publish/${versionId.toUpperCase()}/`;
    const incomingPath = path.join(incomingUpdatesPath, uploadPath);
    const decompressPath = path.join(
        incomingUpdatesPath,
        uploadPath,
        'decompressed',
    );
    const processedPath = path.join(processedUpdatesPath, uploadPath);

    const client = await getMongoClient();
    if (!client) {
        throw new Error(`getMongoClient failed`);
    }

    const versionsColl = client
        .db('updates')
        .collection<IMDBVersion>('versions');

    const version = await versionsColl.findOne({
        _id: _versionId,
    });
    if (version == null || version.state === VersionState.READY) {
        return;
    }

    await setVersionState(
        _versionId,
        VersionState.PROCESSING,
        VersionState.PENDING,
    );

    await deleteDirectory(incomingPath);
    await deleteDirectory(processedPath);
    await mkdir(decompressPath, { recursive: true });
    await mkdir(processedPath, { recursive: true });

    try {
        const downloadProgress = [0, 20];
        const processProgress = [20, 50];
        const uploadProgress = [50, 90];

        const versionFile = path.join(incomingPath, 'update.zip');
        await downloadFile(
            StorageType.Input,
            getUploadFile(versionId),
            versionFile,
            {
                onProgress: (progress) =>
                    job.updateProgress(
                        getJobProgress(
                            downloadProgress[0],
                            downloadProgress[1],
                            progress,
                        ),
                    ),
            },
        );

        await unzipFile(versionFile, decompressPath);

        const filesList: Array<FileInfoExt[]> = new Array<FileInfoExt[]>(
            incomingFolders.length,
        );
        let filesCount = 0;
        let processedCount = 0;
        let reportCounter = 0;

        // List Files
        {
            const availableFiles = await enumerateFiles(decompressPath);

            for (let n = 0; n < incomingFolders.length; ++n)
                filesList[n] = [];
    
            for (const file of availableFiles) {
                for (let n = incomingFolders.length - 1; n >= 0; --n) {
                    const match = matchRegexes(file.path, incomingFoldersRegexes[n]);
                    if (!match) continue;
                    filesList[n].push({ ...file, filename: match });
                    ++filesCount;
                }
            }
        }

        if (filesCount === 0) {
            throw new Error(`Failed : empty update folder`);
        }

        /* Process files */
        for (let n = 0; n < UpdateTypeLookup.length; ++n) {
            const files = filesList[n];

            for (let j = 0; j < files.length; ++j) {
                const file = files[j];
                const fileBuffer = await fsAsync.readFile(file.fullPath);
                const crc32 = new AwsCrc32();
                crc32.update(fileBuffer);
                const fileCrc32 = await crc32.digest();
                if (!fileCrc32) {
                    throw new Error(`Failed : failed to calculate CRC-32`);
                }

                const compressedBuffer = await zlibDeflateAsync(fileBuffer);
                file.crc32 = Buffer.from(fileCrc32).toString('hex');
                file.filename = (MUUID.v4() + `_${file.crc32}`).toUpperCase();
                file.extension = '.eupdz';
                file.fileSize = fileBuffer.length;
                file.packedSize = compressedBuffer.length;

                const dest = path
                    .join(processedPath, file.filename + file.extension)
                    .replace(/\\/g, '/');
                const destDir = dest.substring(0, dest.lastIndexOf('/'));
                await fsExtra.ensureDir(destDir);
                await fsAsync.writeFile(
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
                    reportCounter = 0;
                }
            }
        }

        reportProcessFiles(job, processedCount, filesCount, processProgress);

        await uploadFolder(StorageType.Output, decompressPath, uploadPath, {
            onProgress: (progress) =>
                job.updateProgress(
                    getJobProgress(
                        uploadProgress[0],
                        uploadProgress[1],
                        progress,
                    ),
                ),
        });

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
                    localPath: file.path!,
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

            await setVersionState(
                _versionId,
                VersionState.READY,
                VersionState.PROCESSING,
                session,
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

const processUpdateJob = async (job: BullMQ.Job<UpdateJobData>) => {
    const { type, data } = job.data;
    switch (type) {
        case UpdateServiceJobType.ProcessUploadVersion:
            return processUploadVersion(job, data);
        case UpdateServiceJobType.ProcessPublishVersion:
            return processPublishVersion(job, data);
    }
};

export const UpdatesWorker =
    Number(process.env.UPDATES_QUEUE_PROCESS ?? 1) > 0
        ? new BullMQ.Worker(UpdatesQueueName, processUpdateJob, {
              connection: RedisConnection,
          })
        : null;

export * from './types';
