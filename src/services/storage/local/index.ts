import { createReadStream, createWriteStream, rmSync } from 'node:fs';
import { mkdir, rm, stat } from 'node:fs/promises';
import path from 'node:path';
import { Readable } from 'node:stream';
import { logger } from '~/logger';
import { enumerateFiles, fileStats } from '~/utils';
import { StorageProvider, StorageType, StorageTypes } from '../enums';
import {
    EmptyFolderError,
    InvalidConfigError,
    InvalidFileError,
} from '../errors';
import { FileInfoExt, IStorageOptions } from '../types';

interface IStorageConfig {
    path: string;
}
const storageConfigs = new Map<StorageType, IStorageConfig>();
const loadStorageConfigs = () => {
    for (const storageType of StorageTypes) {
        if (
            process.env[`${storageType}_STORAGE_PROVIDER`] !==
            StorageProvider.Local
        )
            continue;
        storageConfigs.set(storageType, {
            path: path.resolve(
                process.env[`${storageType}_STORAGE_BUCKET`]!,
                process.env[`${storageType}_STORAGE_SUBPATH`]!,
            ),
        });
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

        const directory = path.join(storageConfig.path, dir);
        await rm(directory, { recursive: true });
    } catch (e) {
        logger.error(`Services.Storage.Local.deleteFolder failed : ${e}`);
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
    const directory = dest.substring(0, dest.lastIndexOf(path.sep) + 1);
    await mkdir(directory, { recursive: true });

    const sourcePath = path.join(storageConfig.path, source);
    const sourceStats = await fileStats(sourcePath);
    if (sourceStats == null || sourceStats.isFile() === false)
        throw new InvalidFileError(sourcePath);

    const readSize = sourceStats.size;
    let writtenSize = 0;

    const readStream = createReadStream(sourcePath, 'binary');
    const writeStream = createWriteStream(dest, 'binary');

    try {
        await new Promise<void>((resolve, reject) => {
            readStream.on('data', (chunk) => {
                if (typeof chunk === 'string') return;
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
};

export const downloadFolder = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    const storageConfig = getStorageConfig(storageType);
    if (storageConfig == null) throw new InvalidConfigError(storageType);

    source = path.join(storageConfig.path, source);

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

    const maxParallel = 10;
    const promises: Promise<void>[] = [];
    for (const file of files) {
        const filename = path.join(dest, file.path);
        const directory = filename.substring(
            0,
            filename.lastIndexOf(path.sep) + 1,
        );

        await mkdir(directory, { recursive: true });

        const readStream = createReadStream(file.fullPath, 'binary');
        const writeStream = createWriteStream(filename, 'binary');
        const promise = new Promise<void>((resolve, reject) => {
            readStream.on('data', (chunk) => {
                if (typeof chunk === 'string') return;
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
            writeStream.destroy();
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
        await Promise.all(promises);
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

    const destPath = path.join(storageConfig.path, dest);
    const directory = destPath.substring(0, destPath.lastIndexOf(path.sep) + 1);
    await mkdir(directory, { recursive: true });

    const readSize = sourceStats.size;
    let writtenSize = 0;

    const readStream = createReadStream(source, 'binary');
    const writeStream = createWriteStream(destPath, 'binary');

    try {
        await new Promise<void>((resolve, reject) => {
            readStream.on('data', (chunk) => {
                if (typeof chunk === 'string') return;
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
        await rm(destPath);
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

    const destPath = path.join(storageConfig.path, dest);
    const directory = destPath.substring(0, destPath.lastIndexOf(path.sep) + 1);
    await mkdir(directory, { recursive: true });

    const readSize = source.byteLength;
    let writtenSize = 0;

    const readStream = Readable.from(source);
    const writeStream = createWriteStream(destPath, 'binary');

    try {
        await new Promise<void>((resolve, reject) => {
            readStream.on('data', (chunk) => {
                if (typeof chunk === 'string') return;
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
        await rm(destPath);
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

    const maxParallel = 10;
    const promises: Promise<void>[] = [];
    for (const file of files) {
        const filename = path.join(storageConfig.path, dest, file.path);
        const directory = filename.substring(
            0,
            filename.lastIndexOf(path.sep) + 1,
        );

        await mkdir(directory, { recursive: true });

        const readStream = createReadStream(file.fullPath, 'binary');
        const writeStream = createWriteStream(filename, 'binary');
        const promise = new Promise<void>((resolve, reject) => {
            readStream.on('data', (chunk) => {
                if (typeof chunk === 'string') return;
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
        await Promise.all(promises);
    }
};
