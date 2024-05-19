/*global NodeJS*/
import { existsSync } from 'node:fs';
import { readdir, stat } from 'node:fs/promises';
import type {
    Timestamp,
    Timestamp__Output,
} from '~/proto/google/protobuf/Timestamp';
import type { IVersion } from '~/services/mongodb/schemas/updates/versions';

export const toTimestamp = (date: Date): Timestamp => {
    const time = date.getTime();
    const seconds = Math.floor(time * 0.001);
    const milliseconds = time - seconds * 1000;
    const nanoseconds = milliseconds * 1000000;
    return {
        seconds: seconds,
        nanos: nanoseconds,
    };
};

export const fromTimestamp = (timestamp: Timestamp__Output): Date => {
    const milliseconds =
        Number(timestamp.seconds) * 1000 + timestamp.nanos * 0.000001;
    return new Date(milliseconds);
};

export const getVersionAsString = (version: IVersion) =>
    `${version.major}.${version.minor}.${version.revision}`;

export type FileInfo = {
    path: string;
};
export const enumerateFiles = async (
    path: string,
    relativePath: string = '',
): Promise<FileInfo[]> => {
    const files: FileInfo[] = [];
    if (!existsSync(path)) return files;

    const dirents = await readdir(path, { withFileTypes: true });
    for (const dirent of dirents) {
        if (dirent.isFile()) {
            files.push({
                path: relativePath + dirent.name,
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

export const isNodeError = (error: unknown): error is NodeJS.ErrnoException =>
    error instanceof Error;

export const fileStats = async (filename: string) => {
    try {
        const stats = await stat(filename);
        return stats;
    } catch (error) {
        if (isNodeError(error) && error.code === 'ENOENT') return null;
        else throw error;
    }
};

export const getInputFolder = (versionId: string, uploadId: string) =>
    `${versionId.toUpperCase()}/${uploadId.toUpperCase()}/`;
export const getUploadFile = (versionId: string) => `${versionId.toUpperCase()}.zip`;
export const getOutputFolder = (versionId: string) =>
    `${versionId.toUpperCase()}/`;
