import * as AWS from '@aws-sdk/client-s3';
import type { AwsCredentialIdentity } from '@smithy/types';
import { createReadStream } from 'node:fs';
import { mkdir, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { logger } from '~/logger';
import { enumerateFiles, fileStats } from '~/utils';
import { StorageProvider, StorageType, StorageTypes } from '../enums';
import {
    EmptyFolderError,
    InvalidConfigError,
    InvalidFileError,
} from '../errors';
import { FileInfoExt, IStorageOptions } from '../types';

const getAwsCredentials = (
    accessKeyId?: string,
    secretAccessKey?: string,
): AwsCredentialIdentity | undefined => {
    if (!accessKeyId || !secretAccessKey) return undefined;
    return {
        accessKeyId,
        secretAccessKey,
    };
};

const getAwsRegion = (region?: string): string | undefined => {
    return region || undefined;
};

interface IStorageConfig {
    bucket: string;
    subPath: string;
    client: AWS.S3Client;
}
const storageConfigs = new Map<StorageType, IStorageConfig>();
const loadStorageConfigs = () => {
    for (const storageType of StorageTypes) {
        if (
            process.env[`${storageType}_STORAGE_PROVIDER`] !==
            StorageProvider.AWS
        )
            continue;
        const accessKeyId = process.env[`${storageType}_ACCESS_KEY_ID`];
        const secretAccessKey = process.env[`${storageType}_SECRET_ACCESS_KEY`];
        const region = process.env[`${storageType}_REGION`];
        const useDefaultCredentials = !accessKeyId || !secretAccessKey;
        storageConfigs.set(storageType, {
            bucket: process.env[`${storageType}_STORAGE_BUCKET`]!,
            subPath: process.env[`${storageType}_STORAGE_SUBPATH`]!,
            client: new AWS.S3Client({
                credentials: getAwsCredentials(accessKeyId, secretAccessKey),
                region: getAwsRegion(region),
            }),
        });
        if (useDefaultCredentials) {
            logger.warn(
                `AWS default credentials will be used for ${storageType.toLowerCase()}.`,
            );
        }
        if (!region) {
            logger.warn(
                `AWS default region will be used for ${storageType.toLowerCase()}.`,
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
            StorageProvider.AWS
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

        let continuationToken: string | undefined = undefined;
        do {
            const resultFiles: AWS.ListObjectsV2CommandOutput =
                await storageConfig.client.send(
                    new AWS.ListObjectsV2Command({
                        Bucket: process.env.STORAGE_BUCKET!,
                        Prefix: dir,
                        ContinuationToken: continuationToken,
                    }),
                );

            if (
                resultFiles.Contents != null &&
                resultFiles.Contents.length > 0
            ) {
                await storageConfig.client.send(
                    new AWS.DeleteObjectsCommand({
                        Bucket: process.env.STORAGE_BUCKET!,
                        Delete: {
                            Objects: resultFiles.Contents.map((o) => ({
                                Key: o.Key!,
                            })),
                        },
                    }),
                );
            }

            continuationToken = resultFiles.NextContinuationToken;
        } while (continuationToken);
    } catch (e) {
        logger.error(`Services.Storage.AWS.deleteFolder failed : ${e}`);
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
    source = path.join(storageConfig.subPath, source).replaceAll('\\', '/');

    try {
        const result = await storageConfig.client.send(
            new AWS.GetObjectCommand({
                Bucket: process.env.STORAGE_BUCKET!,
                Key: source,
            }),
        );
        if (result.Body == null) throw new InvalidFileError(source);

        const directory = dest.substring(0, dest.lastIndexOf(path.sep) + 1);
        await mkdir(directory, { recursive: true });
        await writeFile(dest, await result.Body.transformToByteArray());
        options?.onProgress(1.0);
    } catch (e) {
        if (e instanceof AWS.NoSuchKey) throw new InvalidFileError(source);
        logger.error(`Services.Storage.AWS.downloadFile failed : ${e}`);
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

    try {
        const files: FileInfoExt[] = [];

        source = path.join(storageConfig.subPath, source).replaceAll('\\', '/');
        dest = path.resolve(dest);

        let continuationToken: string | undefined = undefined;
        do {
            const resultFiles: AWS.ListObjectsV2CommandOutput =
                await storageConfig.client.send(
                    new AWS.ListObjectsV2Command({
                        Bucket: process.env.STORAGE_BUCKET!,
                        Prefix: source,
                        ContinuationToken: continuationToken,
                    }),
                );

            if (resultFiles.Contents != null) {
                for (const file of resultFiles.Contents) {
                    files.push({
                        fullPath: file.Key!,
                        path: file.Key!.substring(source.length),
                        size: file.Size!,
                    });
                }
            }

            continuationToken = resultFiles.NextContinuationToken;
        } while (continuationToken);

        const readSize = files.reduce(
            (previousValue, currentValue) => previousValue + currentValue.size!,
            0,
        );
        let writtenSize = 0;

        const maxParallel = 10;
        const promises: Promise<void>[] = [];

        for (const file of files) {
            const filename = path.resolve(dest, file.path);
            const directory = filename.substring(
                0,
                filename.lastIndexOf(path.sep) + 1,
            );
            await mkdir(directory, { recursive: true });

            const promise = storageConfig.client
                .send(
                    new AWS.GetObjectCommand({
                        Bucket: process.env.STORAGE_BUCKET,
                        Key: file.fullPath,
                    }),
                )
                .then(async (response) => {
                    await writeFile(
                        filename,
                        await response.Body!.transformToByteArray(),
                    );
                });
            promise.finally(() => {
                writtenSize += file.size!;
                options?.onProgress(writtenSize / readSize);
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
        logger.error(`Services.Storage.AWS.downloadFolder failed : ${e}`);
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

    dest = path.join(storageConfig.subPath, dest).replaceAll('\\', '/');

    const readStream = createReadStream(source);
    try {
        await storageConfig.client.send(
            new AWS.PutObjectCommand({
                Bucket: process.env.STORAGE_BUCKET!,
                Key: dest,
                Body: readStream,
                ContentType: 'application/octet-stream',
            }),
        );
        options?.onProgress(1.0);
    } catch (e) {
        logger.error(`Services.Storage.AWS.uploadFile failed : ${e}`);
        throw e;
    } finally {
        readStream.close();
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

    dest = path.join(storageConfig.subPath, dest).replaceAll('\\', '/');

    try {
        await storageConfig.client.send(
            new AWS.PutObjectCommand({
                Bucket: process.env.STORAGE_BUCKET!,
                Key: dest,
                Body: source,
                ContentType: 'application/octet-stream',
            }),
        );
        options?.onProgress(1.0);
    } catch (e) {
        logger.error(`Services.Storage.AWS.uploadFile failed : ${e}`);
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
        const promises: Promise<AWS.PutObjectCommandOutput>[] = [];

        for (const file of files) {
            const filename = path.join(dest, file.path);

            const readStream = createReadStream(file.fullPath);
            const promise = storageConfig.client.send(
                new AWS.PutObjectCommand({
                    Bucket: process.env.STORAGE_BUCKET,
                    Key: filename,
                    Body: readStream,
                    ContentType: 'application/octet-stream',
                }),
            );
            promise.finally(() => {
                writtenSize += file.size!;
                options?.onProgress(writtenSize / readSize);
                promises.splice(promises.indexOf(promise), 1);
                readStream.close();
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
        logger.error(`Services.Storage.AWS.uploadFolder failed : ${e}`);
        throw e;
    }
};
