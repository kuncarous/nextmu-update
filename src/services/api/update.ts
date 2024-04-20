import { StatusCodes } from 'http-status-codes';
import moment from 'moment';
import { ObjectId } from 'mongodb';
import { logger } from '~/logger';
import { VersionState, VersionType } from '~/proto/update_pb';
import { client as redis } from '~/services/redis';
import {
    OperatingSystemLookup,
    PlatformLookup,
    TextureLookup,
    UpdatesCategories,
    updateCacheDuration,
} from '~/shared';
import { ResponseError } from '~/shared/error';
import {
    IRetrieveUpdateResponse,
    OperatingSystems,
    TextureFormat,
} from '~/types/api/v1';
import { UpdatesQueue } from '../bullmq';
import { getMongoClient } from '../mongodb/client';
import { IMDBUpdateFile } from '../mongodb/schemas/updates/files';
import { IMDBVersion } from '../mongodb/schemas/updates/versions';
import { lockUpdateTransaction } from '../mongodb/update';

interface IVersion {
    major: number;
    minor: number;
    revision: number;
}
const getVersionAsString = (version: IVersion) =>
    `${version.major}.${version.minor}.${version.revision}`;

export const createVersion = async (type: VersionType, description: string) => {
    const client = await getMongoClient();
    if (!client) {
        throw new ResponseError(
            StatusCodes.SERVICE_UNAVAILABLE,
            'service unavailable.',
            `service unavailable, failed to connect mongodb.`,
        );
    }

    const session = client.startSession();
    session.startTransaction();

    try {
        await lockUpdateTransaction(client, session);

        const versionsColl = client
            .db('updates')
            .collection<IMDBVersion>('versions');

        const [version] = await versionsColl
            .find(
                {},
                {
                    session,
                    sort: {
                        createdAt: -1,
                    },
                    limit: 1,
                },
            )
            .toArray();

        const createdAt = moment();
        const versionToInsert: IMDBVersion = {
            version:
                version != null
                    ? {
                          major:
                              type === VersionType.MAJOR
                                  ? version.version.major + 1
                                  : version.version.major,
                          minor:
                              type === VersionType.MINOR
                                  ? version.version.minor + 1
                                  : version.version.minor,
                          revision:
                              type === VersionType.REVISION
                                  ? version.version.revision + 1
                                  : version.version.revision,
                      }
                    : {
                          major: 1,
                          minor: 0,
                          revision: 0,
                      },
            description,
            state: VersionState.PENDING,
            createdAt: createdAt.toDate(),
            updatedAt: createdAt.toDate(),
        };

        const insertedVersion = await versionsColl.insertOne(versionToInsert, {
            session,
        });

        await session.commitTransaction();

        return {
            id: insertedVersion.insertedId.id,
            version: getVersionAsString(versionToInsert.version),
        };
    } catch (error) {
        await session.abortTransaction();
        throw new ResponseError(
            StatusCodes.SERVICE_UNAVAILABLE,
            'service unavailable.',
            `service unavailable, failed to connect mongodb.`,
        );
    } finally {
        await session.endSession();
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
            data: IMDBVersion[];
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
        deferredData.data.map((version, index) => [
            version._id!.toHexString(),
            index,
        ]),
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

    return {
        data: deferredData.data.map((v) => ({
            id: v._id!.id,
            version: getVersionAsString(v.version),
            description: v.description,
            state: v.state,
            filesCount: v.filesCount ?? 0,
            createdAt: v.createdAt,
            updatedAt: v.updatedAt,
        })),
        count: deferredData.count,
    };
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
        .collection<IMDBVersion>('versions');
    const filesColl = client.db('updates').collection<IMDBVersion>('files');

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

    const filesCount = await filesColl
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

    filesCount.forEach((f) => {
        version.filesCount = f.filesCount;
    });

    return {
        id: version._id!.id,
        version: getVersionAsString(version.version),
        description: version.description,
        state: version.state,
        filesCount: version.filesCount ?? 0,
        createdAt: version.createdAt,
        updatedAt: version.updatedAt,
    };
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
                versionId: id,
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
            version: IMDBVersion;
            file: IMDBUpdateFile;
        }

        const versionsMap = new Map(
            versions.map((version) => {
                version.id = version._id!.toHexString().toUpperCase();
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
                UrlPath: f.version.id!,
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
