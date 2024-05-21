/*global NodeJS*/
import { existsSync } from 'node:fs';
import { readdir, stat } from 'node:fs/promises';
import path from 'node:path';
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
    fullPath: string;
    path: string;
};
export const enumerateFiles = async (
    directory: string,
    relative: string = '',
): Promise<FileInfo[]> => {
    const files: FileInfo[] = [];
    if (!existsSync(directory)) return files;

    const dirents = await readdir(directory, { withFileTypes: true });
    for (const dirent of dirents) {
        if (dirent.isFile() == false) continue;
        files.push({
            fullPath: path.join(directory, dirent.name),
            path: path.join(relative, dirent.name),
        });
    }
    for (const dirent of dirents) {
        if (dirent.isDirectory()) {
            files.push(
                ...(await enumerateFiles(
                    path.join(directory, dirent.name),
                    path.join(relative, dirent.name),
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

export const getInputFolder = (uploadId: string, hash: string) =>
    `${uploadId.toUpperCase()}/${hash.toUpperCase()}/`;
export const getUploadFile = (versionId: string) =>
    `${versionId.toUpperCase()}.zip`;
export const getOutputFolder = (versionId: string) =>
    `${versionId.toUpperCase()}/`;
