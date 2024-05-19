import { ObjectId } from 'mongodb';

export interface IMDBUploadChunk {
    uploadId: ObjectId;
    concurrentId: ObjectId;
    offset: number;
    size: number;
    createdAt: Date;
}
