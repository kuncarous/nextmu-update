import { ObjectId } from 'mongodb';
import { UploadState__Output as UploadState } from '~/proto/nextmu/v1/UploadState';

export interface IMDBUpload {
    versionId: ObjectId;
    concurrentId: ObjectId;

    state: UploadState;
    hash: string;
    type: string;
    fileSize: number;
    chunkSize: number;
    chunksCount: number;

    createdAt: Date;
    updatedAt: Date;
}
