import { ObjectId } from 'mongodb';

export interface IMDBUpload {
    versionId: ObjectId;
    concurrentId: ObjectId;

    hash: string;
    type: string;
    fileSize: number;
    chunkSize: number;
    chunksCount: number;

    createdAt: Date;
    updatedAt: Date;
}
