import { ClientSession, ObjectId } from 'mongodb';
import { logger } from '~/logger';
import { UploadState__Output as UploadState } from '~/proto/nextmu/v1/UploadState';
import { VersionState } from '~/proto/nextmu/v1/VersionState';
import { getMongoClient } from './client';
import { IMDBUploadChunk } from './schemas/updates/chunks';
import { IMDBUpload } from './schemas/updates/uploads';
import { IMDBVersion } from './schemas/updates/versions';

export const setVersionState = async (
    versionId: ObjectId,
    state: VersionState,
    checkState?: VersionState,
    session?: ClientSession,
) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const versionsColl = client
            .db('updates')
            .collection<IMDBVersion>('versions');

        await versionsColl.updateOne(
            {
                _id: versionId,
                ...(checkState != null
                    ? {
                          state: checkState,
                      }
                    : {}),
            },
            {
                $set: {
                    state,
                },
            },
            {
                session,
            },
        );
    } catch (error) {
        logger.error(
            `[ERROR] MongoDB.Update.setVersionState failed : ${error}`,
        );
        throw error;
    }
};

export const setUploadState = async (
    uploadId: ObjectId,
    concurrentId: ObjectId,
    state: UploadState,
    checkState?: UploadState,
) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const uploadsColl = client
            .db('updates')
            .collection<IMDBUpload>('uploads');

        await uploadsColl.updateOne(
            {
                _id: uploadId,
                concurrentId,
                ...(checkState != null
                    ? {
                          state: checkState,
                      }
                    : {}),
            },
            {
                $set: {
                    state,
                },
            },
        );
    } catch (error) {
        logger.error(`[ERROR] MongoDB.Update.setUploadState failed : ${error}`);
        throw error;
    }
};

export const deleteUploadChunks = async (uploadId: ObjectId) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const chunksColl = client
            .db('updates')
            .collection<IMDBUploadChunk>('chunks');

        await chunksColl.deleteMany({
            uploadId,
        });
    } catch (error) {
        logger.error(
            `[ERROR] MongoDB.Update.deleteUploadChunks failed : ${error}`,
        );
        throw error;
    }
};
