import { ObjectId } from 'mongodb';

export interface IMDBUpdateFile {
    versionId: ObjectId;
    category: number;

    fileName: string;
    extension: string;
    localPath: string;

    packedSize: number;
    fileSize: number;

    crc32: string;

    createdAt: Date;
}
