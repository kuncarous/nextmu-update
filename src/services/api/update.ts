import { StatusCodes } from 'http-status-codes';
import { ObjectId, WithId } from 'mongodb';
import { logger } from '~/logger';
import { StartUploadVersionResponse__Output } from '~/proto/nextmu/v1/StartUploadVersionResponse';
import { UploadVersionChunkResponse__Output } from '~/proto/nextmu/v1/UploadVersionChunkResponse';
import { VersionState } from '~/proto/nextmu/v1/VersionState';
import { VersionType } from '~/proto/nextmu/v1/VersionType';
import { client as redis } from '~/services/redis';
import {
    OperatingSystemLookup,
    PlatformLookup,
    TextureLookup,
    UpdatesCategories,
    updateCacheDuration,
} from '~/shared';
import { ResponseError } from '~/shared/error';
import type {
    IRetrieveUpdateResponse,
    OperatingSystems,
    TextureFormat,
} from '~/types/api/v1';
import { UpdateServiceJobType, UpdatesQueue } from '../bullmq';
import { getMongoClient } from '../mongodb/client';
import { IMDBUploadChunk } from '../mongodb/schemas/updates/chunks';
import type { IMDBUpdateFile } from '../mongodb/schemas/updates/files';
import { IMDBUpload } from '../mongodb/schemas/updates/uploads';
import type { IMDBVersion } from '../mongodb/schemas/updates/versions';
import { deleteFolder, uploadBuffer } from '../storage';
import { StorageType } from '../storage/enums';

export const createVersion = async (type: VersionType, description: string) => {
    const client = await getMongoClient();
    if (!client) {
        throw new ResponseError(
            StatusCodes.SERVICE_UNAVAILABLE,
            'service unavailable.',
            `service unavailable, failed to connect mongodb.`,
        );
    }

    try {
        const versionsColl = client
            .db('updates')
            .collection<IMDBVersion>('versions');

        const versionId = new ObjectId();
        const currentDate = new Date();
        const versionField =
            type === VersionType.MAJOR
                ? 'version.major'
                : type === VersionType.MINOR
                  ? 'version.minor'
                  : 'version.revision';
        await versionsColl
            .aggregate<WithId<IMDBVersion>>([
                {
                    $sort: {
                        'version.major': -1,
                        'version.minor': -1,
                        'version.revision': -1,
                    },
                },
                {
                    $limit: 1,
                },
                {
                    $facet: {
                        current: [
                            {
                                $match: {
                                    _id: {
                                        $ne: null,
                                    },
                                },
                            },
                        ],
                    },
                },
                {
                    $project: {
                        versions: {
                            $cond: {
                                if: {
                                    $eq: [
                                        {
                                            $size: '$current',
                                        },
                                        0,
                                    ],
                                },
                                then: [
                                    {
                                        version: {
                                            major: 0,
                                            minor: 0,
                                            revision: 0,
                                        },
                                    },
                                ],
                                else: '$current',
                            },
                        },
                    },
                },
                {
                    $unwind: '$versions',
                },
                {
                    $replaceRoot: {
                        newRoot: '$versions',
                    },
                },
                {
                    $set: {
                        _id: versionId,
                        [versionField]: {
                            $sum: [`$${versionField}`, 1],
                        },
                        description,
                        state: VersionState.PENDING,
                        createdAt: currentDate,
                        updatedAt: currentDate,
                    },
                },
                {
                    $merge: {
                        into: 'versions',
                        on: '_id',
                        whenMatched: 'keepExisting',
                        whenNotMatched: 'insert',
                    },
                },
            ])
            .tryNext();

        const result = await versionsColl.findOne({ _id: versionId });
        if (result == null) {
            throw new ResponseError(
                StatusCodes.INTERNAL_SERVER_ERROR,
                'service failed.',
                `service failed, couldn't retrieve created version.`,
            );
        }

        return {
            id: result._id,
            version: result.version,
        };
    } catch (error) {
        throw new ResponseError(
            StatusCodes.SERVICE_UNAVAILABLE,
            'service unavailable.',
            `service unavailable, failed to connect mongodb.`,
        );
    }
};

export const editVersion = async (versionId: ObjectId, description: string) => {
    const client = await getMongoClient();
    if (!client) {
        throw new ResponseError(
            StatusCodes.SERVICE_UNAVAILABLE,
            'service unavailable.',
            `service unavailable, failed to connect mongodb.`,
        );
    }

    const versionsColl = client
        .db('updates')
        .collection<IMDBVersion>('versions');
    const result = await versionsColl.updateOne(
        {
            _id: versionId,
        },
        {
            $set: {
                description,
                updatedAt: new Date(),
            },
        },
    );

    return { success: result.modifiedCount > 0 };
};

export const getVersions = async (offset: number, count: number) => {
    const client = await getMongoClient();
    if (!client) {
        throw new ResponseError(
            StatusCodes.SERVICE_UNAVAILABLE,
            'service unavailable.',
            `service unavailable, failed to connect mongodb.`,
        );
    }

    const versionsColl = client
        .db('updates')
        .collection<IMDBVersion>('versions');
    const filesColl = client.db('updates').collection<IMDBVersion>('files');

    const versionsResult = await versionsColl
        .aggregate<{
            count: number;
            data: (WithId<IMDBVersion> & { filesCount: number })[];
        }>([
            {
                $facet: {
                    count: [{ $count: 'value' }],
                    data: [
                        {
                            $sort: { createdAt: -1 },
                        },
                        {
                            $skip: offset,
                        },
                        {
                            $limit: count,
                        },
                    ],
                },
            },
            {
                $unwind: '$count',
            },
            {
                $set: { count: '$count.value' },
            },
        ])
        .toArray();

    const deferredData = versionsResult.length > 0 ? versionsResult[0] : null;

    if (deferredData == null) return { data: [], count: 0 };

    const versionsMap = new Map(
        deferredData.data.map((version, index) => {
            version.filesCount ??= 0;
            return [version._id!.toHexString(), index];
        }),
    );
    const filesCount = await filesColl
        .aggregate<{
            _id: ObjectId;
            filesCount: number;
        }>([
            {
                $match: {
                    versionId: {
                        $in: deferredData.data.map((v) => v._id!),
                    },
                },
            },
            {
                $group: {
                    _id: '$versionId',
                    filesCount: {
                        $sum: 1,
                    },
                },
            },
        ])
        .toArray();

    filesCount.forEach((f) => {
        deferredData.data[versionsMap.get(f._id.toHexString())!].filesCount =
            f.filesCount;
    });

    return deferredData;
};

export const getVersion = async (versionId: ObjectId) => {
    const client = await getMongoClient();
    if (!client) {
        throw new ResponseError(
            StatusCodes.SERVICE_UNAVAILABLE,
            'service unavailable.',
            `service unavailable, failed to connect mongodb.`,
        );
    }

    const versionsColl = client
        .db('updates')
        .collection<IMDBVersion & { filesCount: number }>('versions');
    const filesColl = client.db('updates').collection<IMDBUpdateFile>('files');

    const version = await versionsColl.findOne({
        _id: versionId,
    });
    if (version == null) {
        throw new ResponseError(
            StatusCodes.NOT_FOUND,
            'invalid version.',
            `invalid version, not found in database.`,
        );
    }

    const filesResult = await filesColl
        .aggregate<{
            _id: ObjectId;
            filesCount: number;
        }>([
            {
                $match: {
                    versionId: {
                        $eq: version._id!,
                    },
                },
            },
            {
                $group: {
                    _id: '$versionId',
                    filesCount: {
                        $sum: 1,
                    },
                },
            },
        ])
        .toArray();

    version.filesCount ??= 0;
    if (filesResult.length > 0) {
        version.filesCount = filesResult[0].filesCount;
    }

    return version;
};

export const processVersion = async (id: string) => {
    const jobId = `version-${id}`;
    let job = await UpdatesQueue.getJob(jobId);
    if (job) {
        if (await job.isFailed()) {
            await job.remove();
            job = undefined;
        }
    }

    if (!job) {
        job = await UpdatesQueue.add(
            'processUpdate',
            {
                type: UpdateServiceJobType.ProcessPublishVersion,
                data: {
                    versionId: id,
                },
            },
            {
                jobId,
                removeOnComplete: true,
            },
        );
    }
};

const formatUpdateKey = (
    source: string,
    target: string,
    os: OperatingSystems,
    texture: TextureFormat,
) => {
    return `update-${source}-${target}-${os}-${texture}`;
};

export const getUpdateFiles = async (
    major: number,
    minor: number,
    revision: number,
    os: OperatingSystems,
    texture: TextureFormat,
): Promise<IRetrieveUpdateResponse> => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const versionsColl = client
            .db('updates')
            .collection<IMDBVersion>('versions');
        const versions = await versionsColl
            .find({
                $and: [
                    {
                        state: { $eq: VersionState.READY },
                    },
                    {
                        $or: [
                            {
                                'version.major': { $gt: major },
                            },
                            {
                                'version.major': { $eq: major },
                                'version.minor': { $gt: minor },
                            },
                            {
                                'version.major': { $eq: major },
                                'version.minor': { $eq: minor },
                                'version.revision': { $gt: revision },
                            },
                        ],
                    },
                ],
            })
            .sort({
                createdAt: 1,
            })
            .toArray();

        if (versions.length === 0) {
            return {
                version: `${major}.${minor}.${revision}`,
                files: [],
            };
        }

        const sourceVersion = versions[0];
        const lastVersion = versions[versions.length - 1];
        const updateKey = formatUpdateKey(
            `${[sourceVersion.version.major, sourceVersion.version.minor, sourceVersion.version.revision].join('.')}`,
            `${[lastVersion.version.major, lastVersion.version.minor, lastVersion.version.revision].join('.')}`,
            os,
            texture,
        );
        const updateData = await redis.get(updateKey);
        if (updateData != null) return JSON.parse(updateData);

        interface IUpdateFile {
            version: WithId<IMDBVersion>;
            file: IMDBUpdateFile;
        }

        const versionsMap = new Map(
            versions.map((version) => {
                return [version._id!.toHexString(), version];
            }),
        );
        const filesColl = client
            .db('updates')
            .collection<IMDBUpdateFile>('files');
        const filesMap = new Map<string, IUpdateFile>();
        const filesCursor = filesColl.find({
            $and: [
                {
                    versionId: {
                        $in: versions.map((version) => version._id!),
                    },
                },
                {
                    $or: [
                        {
                            category: {
                                $eq: UpdatesCategories.General,
                            },
                        },
                        {
                            category: {
                                $eq: PlatformLookup[os],
                            },
                        },
                        {
                            category: {
                                $eq: OperatingSystemLookup[os],
                            },
                        },
                        {
                            category: {
                                $eq: TextureLookup[texture],
                            },
                        },
                    ],
                },
            ],
        });

        for await (const file of filesCursor) {
            const version = versionsMap.get(file.versionId.toHexString())!;
            const fileData = filesMap.get(file.localPath);
            if (fileData == null) {
                filesMap.set(file.localPath, {
                    version,
                    file,
                });
            } else if (fileData.version.createdAt < version.createdAt) {
                fileData.version = version;
                fileData.file = file;
            }
        }

        const result = {
            version: `${lastVersion.version.major}.${lastVersion.version.minor}.${lastVersion.version.revision}`,
            files: Array.from(filesMap.values()).map((f) => ({
                UrlPath: f.version._id.toHexString().toUpperCase(),
                LocalPath: f.file.localPath,
                Filename: f.file.fileName,
                Extension: f.file.extension,
                PackedSize: f.file.packedSize,
                OriginalSize: f.file.fileSize,
                CRC32: f.file.crc32,
            })),
        };

        await redis.setex(
            updateKey,
            updateCacheDuration,
            JSON.stringify(result),
        );

        return result;
    } catch (error) {
        logger.error(`[ERROR] getUpdateFiles failed : ${error}`);
        throw error;
    }
};

export const startUploadVersion = async (
    id: ObjectId,
    hash: string,
    type: string,
    chunkSize: number,
    fileSize: number,
): Promise<StartUploadVersionResponse__Output> => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const uploadsColl = client
            .db('updates')
            .collection<IMDBUpload>('uploads');

        const currentDate = new Date();
        const uploadId = new ObjectId();
        const concurrentId = new ObjectId();
        const result = await uploadsColl.findOneAndUpdate(
            {
                versionId: id,
            },
            [
                {
                    $set: {
                        concurrentId: {
                            $cond: {
                                if: {
                                    $and: [
                                        {
                                            $eq: [hash, '$hash'],
                                        },
                                        {
                                            $eq: [chunkSize, '$chunkSize'],
                                        },
                                    ],
                                },
                                then: '$concurrentId',
                                else: concurrentId,
                            },
                        },
                        hash,
                        fileSize,
                        chunkSize,
                        chunksCount: Math.ceil(fileSize / chunkSize),
                    },
                    $setOnInsert: {
                        _id: uploadId,
                        versionId: id,
                        hash,
                        type,
                        fileSize,
                        chunkSize,
                        chunksCount: Math.ceil(fileSize / chunkSize),
                        createdAt: currentDate,
                        updatedAt: currentDate,
                    },
                },
            ],
            {
                upsert: true,
                returnDocument: 'before',
            },
        );

        if (result == null) {
            return {
                id: uploadId.toHexString(),
                existingChunks: [],
            };
        }

        const chunksColl = client
            .db('updates')
            .collection<IMDBUploadChunk>('upload_chunks');

        if (result.hash !== hash || result.chunkSize !== chunkSize) {
            await chunksColl.deleteMany({ uploadId: result._id, concurrentId });
            await deleteFolder(
                StorageType.Input,
                `${id.toHexString()}/${hash}/`,
            );
        }

        const chunks = await chunksColl
            .find(
                {
                    uploadId: result._id,
                },
                {
                    projection: {
                        offset: 1,
                        index: 1,
                    },
                },
            )
            .toArray();

        return {
            id: (result?._id ?? uploadId).toHexString(),
            existingChunks: chunks
                .sort((a, b) => a.offset - b.offset)
                .map((c) => ({ offset: c.offset, size: c.size })),
        };
    } catch (error) {
        logger.error(`[ERROR] startUploadVersion failed : ${error}`);
        throw error;
    }
};

export const uploadVersionChunk = async (
    uploadId: ObjectId,
    concurrentId: ObjectId,
    offset: number,
    data: Buffer,
): Promise<UploadVersionChunkResponse__Output> => {
    try {
        if (data.byteLength === 0) {
            throw new ResponseError(
                StatusCodes.BAD_REQUEST,
                'empty data buffer',
            );
        }

        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const uploadsColl = client
            .db('updates')
            .collection<IMDBUpload>('uploads');
        const chunksColl = client
            .db('updates')
            .collection<IMDBUploadChunk>('chunks');

        const upload = await uploadsColl.findOne({
            _id: uploadId,
            concurrentId,
        });
        if (upload == null) {
            throw new ResponseError(
                StatusCodes.BAD_REQUEST,
                'invalid upload id or concurrent id',
            );
        }

        if (offset >= upload.chunksCount) {
            throw new ResponseError(
                StatusCodes.BAD_REQUEST,
                'invalid chunk offset',
            );
        }

        if (offset == upload.chunksCount - 1) {
            if (
                upload.fileSize - upload.chunkSize * (upload.chunksCount - 1) !=
                data.byteLength
            ) {
                throw new ResponseError(
                    StatusCodes.BAD_REQUEST,
                    'invalid chunk size',
                );
            }
        } else {
            if (upload.chunkSize != data.byteLength) {
                throw new ResponseError(
                    StatusCodes.BAD_REQUEST,
                    'invalid chunk size',
                );
            }
        }

        await uploadBuffer(
            StorageType.Input,
            data,
            `${upload._id.toHexString()}/${upload.hash}/${offset}.data`,
        );

        await chunksColl.insertOne({
            uploadId,
            concurrentId,
            offset,
            size: data.length,
            createdAt: new Date(),
        });

        const chunksCount = await chunksColl.countDocuments({
            uploadId,
            concurrentId,
        });

        return {
            finished: upload.chunksCount === chunksCount,
        };
    } catch (error) {
        logger.error(`[ERROR] uploadVersionChunk failed : ${error}`);
        throw error;
    }
};
