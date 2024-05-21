import * as Google from '@google-cloud/storage';
import { createReadStream, createWriteStream, rmSync } from 'node:fs';
import { mkdir, rm, stat } from 'node:fs/promises';
import path from 'node:path';
import { Readable } from 'node:stream';
import { logger } from '~/logger';
import { enumerateFiles, fileStats } from '~/utils';
import { StorageProvider, StorageType, StorageTypes } from '../enums';
import {
    EmptyFolderError,
    InvalidBucketError,
    InvalidConfigError,
    InvalidFileError,
} from '../errors';
import { FileInfoExt, IStorageOptions } from '../types';

interface IStorageConfig {
    bucket: string;
    subPath: string;
    client: Google.Storage;
}
const storageConfigs = new Map<StorageType, IStorageConfig>();
const loadStorageConfigs = () => {
    for (const storageType of StorageTypes) {
        if (
            process.env[`${storageType}_STORAGE_PROVIDER`] !==
            StorageProvider.GCP
        )
            continue;
        const projectId = process.env[`${storageType}_STORAGE_PROJECT_ID`];
        const keyFile = process.env[`${storageType}_STORAGE_KEY_FILE`];
        const useDefaultCredentials = !projectId || !keyFile;
        storageConfigs.set(storageType, {
            bucket: process.env[`${storageType}_STORAGE_BUCKET`]!,
            subPath: process.env[`${storageType}_STORAGE_SUBPATH`]!,
            client: new Google.Storage(
                useDefaultCredentials == false
                    ? {
                          projectId,
                          keyFile,
                      }
                    : undefined,
            ),
        });
        if (useDefaultCredentials) {
            logger.warn(
                `GCP default credentials will be used for ${storageType.toLowerCase()}.`,
            );
        }
    }
};

const getStorageConfig = (storageType: StorageType) => {
    return storageConfigs.get(storageType);
};

// check environment variables
{
    const errors = [];
    for (const storageType of StorageTypes) {
        if (
            process.env[`${storageType}_STORAGE_PROVIDER`] !==
            StorageProvider.GCP
        )
            continue;
        if (!process.env[`${storageType}_STORAGE_BUCKET`])
            errors.push(`${storageType}_STORAGE_BUCKET isn't configured`);
        if (!process.env[`${storageType}_STORAGE_SUBPATH`])
            errors.push(`${storageType}_STORAGE_SUBPATH isn't configured`);
    }
    if (errors.length > 0) throw new Error(errors.join('\n'));
    loadStorageConfigs();
}

export const deleteFolder = async (storageType: StorageType, dir: string) => {
    try {
        const storageConfig = getStorageConfig(storageType);
        if (storageConfig == null) throw new InvalidConfigError(storageType);

        dir = path.join(storageConfig.subPath, dir).replaceAll('\\', '/');

        const bucket = storageConfig.client.bucket(process.env.STORAGE_BUCKET!);
        if ((await bucket.exists())[0] == false)
            throw new InvalidBucketError(process.env.STORAGE_BUCKET!);

        await bucket.deleteFiles({
            prefix: dir,
            force: true,
        });
    } catch (e) {
        logger.error(`Services.Storage.GCP.deleteFolder failed : ${e}`);
        throw e;
    }
};

export const downloadFile = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    const storageConfig = getStorageConfig(storageType);
    if (storageConfig == null) throw new InvalidConfigError(storageType);

    dest = path.resolve(dest);

    const bucket = storageConfig.client.bucket(process.env.STORAGE_BUCKET!);
    if ((await bucket.exists())[0] == false)
        throw new InvalidBucketError(process.env.STORAGE_BUCKET!);

    const file = bucket.file(
        path.join(storageConfig.subPath, source).replaceAll('\\', '/'),
    );
    if ((await file.exists())[0] == false)
        throw new InvalidFileError(file.name);

    const directory = dest.substring(0, dest.lastIndexOf(path.sep) + 1);
    await mkdir(directory, { recursive: true });

    try {
        const readSize = Number((await file.getMetadata())[0].size ?? 0);
        let writtenSize = 0;

        const readStream = file.createReadStream();
        const writeStream = createWriteStream(dest, 'binary');

        try {
            await new Promise<void>((resolve, reject) => {
                readStream.on('data', (chunk: Buffer) => {
                    writtenSize += chunk.byteLength;
                    options?.onProgress(writtenSize / readSize);
                });
                readStream.on('error', (err) => {
                    reject(err);
                });
                readStream.on('end', () => {
                    writeStream.end();
                    options?.onProgress(1.0);
                    resolve();
                });

                readStream.pipe(writeStream);
            });
        } catch (e) {
            writeStream.end();
            await rm(dest);
            throw e;
        }
    } catch (e) {
        logger.error(`Services.Storage.GCP.downloadFile failed : ${e}`);
        throw e;
    }
};

export const downloadFolder = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    const storageConfig = getStorageConfig(storageType);
    if (storageConfig == null) throw new InvalidConfigError(storageType);

    const bucket = storageConfig.client.bucket(process.env.STORAGE_BUCKET!);
    if ((await bucket.exists())[0] == false)
        throw new InvalidBucketError(process.env.STORAGE_BUCKET!);

    try {
        const [cloudFiles] = await bucket.getFiles({
            prefix: path
                .join(storageConfig.subPath, source)
                .replaceAll('\\', '/'),
        });

        const files: FileInfoExt[] = [];
        for (const file of cloudFiles) {
            files.push({
                fullPath: file.name,
                path: file.name.substring(source.length),
            });
        }

        for (const file of files) {
            const [metadata] = await bucket.file(file.fullPath).getMetadata();
            file.size = Number(metadata.size ?? 0);
        }

        const readSize = files.reduce(
            (previousValue, currentValue) => previousValue + currentValue.size!,
            0,
        );
        let writtenSize = 0;

        const maxParallel = 10;
        const promises: Promise<void>[] = [];

        for (const file of files) {
            const filename = path.join(dest, file.path).replaceAll('\\', '/');

            const bucketFile = bucket.file(file.fullPath);

            const readStream = bucketFile.createReadStream();
            const writeStream = createWriteStream(filename);

            const promise = new Promise<void>((resolve, reject) => {
                readStream.on('data', (chunk: Buffer) => {
                    writtenSize += chunk.byteLength;
                    options?.onProgress(writtenSize / readSize);
                });
                readStream.on('error', (err) => {
                    reject(err);
                });
                readStream.on('end', () => {
                    writeStream.end();
                    resolve();
                });

                readStream.pipe(writeStream);
            });
            promise.catch(() => {
                writeStream.end();
                rmSync(filename);
            });
            promise.finally(() => {
                promises.splice(promises.indexOf(promise), 1);
            });
            promises.push(promise);

            if (promises.length >= maxParallel) {
                await Promise.any([...promises]);
            }
        }

        if (promises.length > 0) {
            await Promise.all([...promises]);
        }
    } catch (e) {
        logger.error(`Services.Storage.GCP.downloadFolder failed : ${e}`);
        throw e;
    }
};

export const uploadFile = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    const storageConfig = getStorageConfig(storageType);
    if (storageConfig == null) throw new InvalidConfigError(storageType);

    source = path.resolve(source);
    const sourceStats = await fileStats(source);
    if (sourceStats == null || sourceStats.isFile() === false)
        throw new InvalidFileError(source);

    const bucket = storageConfig.client.bucket(process.env.STORAGE_BUCKET!);
    if ((await bucket.exists())[0] == false)
        throw new InvalidBucketError(process.env.STORAGE_BUCKET!);

    try {
        const readSize = sourceStats.size;
        let writtenSize = 0;

        const file = bucket.file(
            path.join(storageConfig.subPath, dest).replaceAll('\\', '/'),
        );

        const readStream = createReadStream(source);
        const writeStream = file.createWriteStream({
            contentType: 'application/octet-stream',
        });

        try {
            await new Promise<void>((resolve, reject) => {
                readStream.on('data', (chunk: Buffer) => {
                    writtenSize += chunk.byteLength;
                    options?.onProgress(writtenSize / readSize);
                });
                readStream.on('error', (err) => {
                    reject(err);
                });
                readStream.on('end', () => {
                    writeStream.end();
                    options?.onProgress(1.0);
                    resolve();
                });

                readStream.pipe(writeStream);
            });
        } catch (e) {
            writeStream.end();
            await file.delete().catch(() => {});
            throw e;
        }
    } catch (e) {
        logger.error(`Services.Storage.GCP.uploadFile failed : ${e}`);
        throw e;
    }
};

export const uploadBuffer = async (
    storageType: StorageType,
    source: Buffer,
    dest: string,
    options?: IStorageOptions,
) => {
    const storageConfig = getStorageConfig(storageType);
    if (storageConfig == null) throw new InvalidConfigError(storageType);

    const bucket = storageConfig.client.bucket(process.env.STORAGE_BUCKET!);
    if ((await bucket.exists())[0] == false)
        throw new InvalidBucketError(process.env.STORAGE_BUCKET!);

    try {
        const readSize = source.byteLength;
        let writtenSize = 0;

        const file = bucket.file(
            path.join(storageConfig.subPath, dest).replaceAll('\\', '/'),
        );

        const readStream = Readable.from(source);
        const writeStream = file.createWriteStream({
            contentType: 'application/octet-stream',
        });

        try {
            await new Promise<void>((resolve, reject) => {
                readStream.on('data', (chunk: Buffer) => {
                    writtenSize += chunk.byteLength;
                    options?.onProgress(writtenSize / readSize);
                });
                readStream.on('error', (err) => {
                    reject(err);
                });
                readStream.on('end', () => {
                    writeStream.end();
                    options?.onProgress(1.0);
                    resolve();
                });

                readStream.pipe(writeStream);
            });
        } catch (e) {
            writeStream.end();
            await file.delete().catch(() => {});
            throw e;
        }
    } catch (e) {
        logger.error(`Services.Storage.GCP.uploadFile failed : ${e}`);
        throw e;
    }
};

export const uploadFolder = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    const storageConfig = getStorageConfig(storageType);
    if (storageConfig == null) throw new InvalidConfigError(storageType);

    source = path.resolve(source);

    const files: FileInfoExt[] = await enumerateFiles(source);
    if (files.length === 0) throw new EmptyFolderError(source);

    const bucket = storageConfig.client.bucket(process.env.STORAGE_BUCKET!);
    if ((await bucket.exists())[0] == false)
        throw new InvalidBucketError(process.env.STORAGE_BUCKET!);

    for (const file of files) {
        const stats = await stat(file.fullPath);
        file.size = stats.size;
    }

    const readSize = files.reduce(
        (previousValue, currentValue) => previousValue + currentValue.size!,
        0,
    );
    let writtenSize = 0;

    try {
        const maxParallel = 10;
        const promises: Promise<void>[] = [];

        for (const file of files) {
            const filename = path
                .join(storageConfig.subPath, dest, file.path)
                .replaceAll('\\', '/');

            const bucketFile = bucket.file(filename);

            const readStream = createReadStream(file.fullPath);
            const writeStream = bucketFile.createWriteStream({
                contentType: 'application/octet-stream',
            });

            const promise = new Promise<void>((resolve, reject) => {
                readStream.on('data', (chunk: Buffer) => {
                    writtenSize += chunk.byteLength;
                    options?.onProgress(writtenSize / readSize);
                });
                readStream.on('error', (err) => {
                    reject(err);
                });
                readStream.on('end', () => {
                    writeStream.end();
                    resolve();
                });

                readStream.pipe(writeStream);
            });
            promise.catch(() => {
                writeStream.end();
                bucketFile.delete().catch(() => {});
            });
            promise.finally(() => {
                promises.splice(promises.indexOf(promise), 1);
            });
            promises.push(promise);

            if (promises.length >= maxParallel) {
                await Promise.any([...promises]);
            }
        }

        if (promises.length > 0) {
            await Promise.all([...promises]);
        }
    } catch (e) {
        logger.error(`Services.Storage.GCP.uploadFolder failed : ${e}`);
        throw e;
    }
};
