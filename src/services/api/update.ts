import { logger } from "~/logger";
import { OperatingSystemLookup, OperatingSystems, PlatformLookup, TextureFormat, TextureLookup, UpdatesCategories, VersionState, VersionType, updateCacheDuration } from "~/shared";
import { getMongoClient } from "../mongodb/client";
import { lockUpdateTransaction } from "../mongodb/update";
import { IMDBVersion } from "../mongodb/schemas/updates/versions";
import moment from "moment";
import { ObjectId } from "mongodb";
import { IMDBUpdateFile } from "../mongodb/schemas/updates/files";
import { client as redis } from "~/services/redis";

interface ICreateVersionResult {
    id?: string;
    version?: {
        major: number;
        minor: number;
        revision: number;
    };
}
export const createVersion = async (
    type: VersionType,
    description: string,
) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const session = client.startSession();
        session.startTransaction();

        try {
            await lockUpdateTransaction(client, session);

            const versionsColl = client.db("updates").collection<IMDBVersion>("versions");

            const [version] = await (
                versionsColl
                .find(
                    {},
                    {
                        session,
                        sort: {
                            createdAt: -1,
                        },
                        limit: 1,
                    }
                )
                .toArray()
            );

            const createdAt = moment();
            const versionToInsert: IMDBVersion = {
                version: (
                    version != null
                    ? {
                        major: (
                            type === VersionType.Major
                            ? version.version.major + 1
                            : version.version.major
                        ),
                        minor: (
                            type === VersionType.Minor
                            ? version.version.minor + 1
                            : version.version.minor
                        ),
                        revision: (
                            type === VersionType.Revision
                            ? version.version.revision + 1
                            : version.version.revision
                        ),
                    } : {
                        major: 1,
                        minor: 0,
                        revision: 0,
                    }
                ),
                description,
                state: VersionState.Pending,
                createdAt: createdAt.toDate(),
                updatedAt: createdAt.toDate(),
            };

            const insertedVersion = await versionsColl.insertOne(
                versionToInsert,
                {
                    session,
                }
            );

            await session.commitTransaction();

            return {
                id: insertedVersion.insertedId.toHexString(),
                version: versionToInsert.version,
            };
        } catch(error) {
            await session.abortTransaction();
            throw(error);
        } finally {
            await session.endSession();
        }
    } catch(error) {
        logger.error(`[ERROR] createVersion failed : ${error}`);
        throw(error);
    }
}

export const editVersion = async (
    id: string,
    description: string,
) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const versionsColl = client.db("updates").collection<IMDBVersion>("versions");
        const result = await versionsColl.updateOne(
            {
                _id: new ObjectId(id),
            },
            {
                $set: {
                    description,
                    updatedAt: new Date(),
                },
            },
        );

        return result.modifiedCount > 0;
    } catch(error) {
        logger.error(`[ERROR] editVersion failed : ${error}`);
        throw(error);
    }
}

export const getVersions = async (
    offset: number,
    count: number,
) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const versionsColl = client.db("updates").collection<IMDBVersion>("versions");
        const filesColl = client.db("updates").collection<IMDBVersion>("files");

        const versionsResult = await (
            versionsColl
            .aggregate<
                {
                    count: number;
                    data: IMDBVersion[];
                }
            >(
                [
                    {
                        $facet: {
                            count: [{ $count: "value" }],
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
                        }
                    },
                    {
                        $unwind: "$count",
                    },
                    {
                        $set: { count: "$count.value" },
                    },
                ]
            )
            .toArray()
        );

        const deferredData = versionsResult.length > 0 ? versionsResult[0] : null;

        if (deferredData == null) return { data: [], count: 0 };

        const versionsMap = new Map(deferredData.data.map((version, index) => [version._id!.toHexString(), index]));
        const filesCount = await (
            filesColl
            .aggregate<
                {
                    _id: ObjectId;
                    filesCount: number;
                }
            >(
                [
                    {
                        $match: {
                            versionId: {
                                $in: deferredData.data.map(v => v._id!),
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
                ]
            )
            .toArray()
        );

        filesCount.forEach(
            (f) => {
                deferredData.data[versionsMap.get(f._id.toHexString())!].filesCount = f.filesCount;
            }
        );

        return (
            {
                data: deferredData.data.map(
                    v => ({
                        id: v._id!.toHexString(),
                        version: v.version,
                        description: v.description,
                        state: v.state,
                        filesCount: v.filesCount ?? 0,
                        createdAt: v.createdAt,
                        updatedAt: v.updatedAt,
                    })
                ),
                count: deferredData.count,
            }
        );
    } catch(error) {
        logger.error(`[ERROR] getVersions failed : ${error}`);
        throw(error);
    }
}

export const getVersion = async (
    versionId: string,
) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const versionsColl = client.db("updates").collection<IMDBVersion>("versions");
        const filesColl = client.db("updates").collection<IMDBVersion>("files");

        const version = await versionsColl.findOne({ _id: new ObjectId(versionId) });
        if (version == null) return { version: null, filesCount: 0 };

        const filesCount = await (
            filesColl
            .aggregate<
                {
                    _id: ObjectId;
                    filesCount: number;
                }
            >(
                [
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
                ]
            )
            .toArray()
        );

        filesCount.forEach(
            (f) => {
                version.filesCount = f.filesCount;
            }
        );

        return (
            {
                version: {
                    id: version._id!.toHexString(),
                    version: version.version,
                    description: version.description,
                    state: version.state,
                    filesCount: version.filesCount ?? 0,
                    createdAt: version.createdAt,
                    updatedAt: version.updatedAt,
                },
            }
        );
    } catch(error) {
        logger.error(`[ERROR] getVersion failed : ${error}`);
        throw(error);
    }
}

const formatUpdateKey = (
    source: string,
    target: string,
    os: OperatingSystems,
    texture: TextureFormat,
) => {
    return `update-${source}-${target}-${os}-${texture}`;
}

export const getUpdateFiles = async (
    major: number,
    minor: number,
    revision: number,
    os: OperatingSystems,
    texture: TextureFormat,
) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const versionsColl = client.db("updates").collection<IMDBVersion>("versions");
        const versions = await (
            versionsColl
            .find(
                {
                    $and: [
                        {
                            state: { $eq: VersionState.Ready },
                        },
                        {
                            $or: [
                                {
                                    "version.major": { $gt: major },
                                },
                                {
                                    "version.major": { $eq: major },
                                    "version.minor": { $gt: minor },
                                },
                                {
                                    "version.major": { $eq: major },
                                    "version.minor": { $eq: minor },
                                    "version.revision": { $gt: revision },
                                },
                            ],
                        },
                    ],
                }
            )
            .sort(
                {
                    createdAt: 1,
                }
            )
            .toArray()
        );

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
            texture
        );
        const updateData = await redis.get(updateKey);
        if (updateData != null) return JSON.parse(updateData);

        interface IUpdateFile {
            version: IMDBVersion;
            file: IMDBUpdateFile;
        }

        const versionsMap = new Map(
            versions.map(version => {
                version.id = version._id!.toHexString().toUpperCase();
                return [version._id!.toHexString(), version];
            })
        );
        const filesColl = client.db("updates").collection<IMDBUpdateFile>("files");
        const filesMap = new Map<string, IUpdateFile>();
        const filesCursor = (
            filesColl
            .find(
                {
                    $and: [
                        {
                            versionId: {
                                $in: versions.map(version => version._id!),
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
                }
            )
        );

        for await (const file of filesCursor) {
            const version = versionsMap.get(file.versionId.toHexString())!;
            const fileData = filesMap.get(file.localPath);
            if (fileData == null) {
                filesMap.set(
                    file.localPath,
                    {
                        version,
                        file,
                    }
                );
            } else if (fileData.version.createdAt < version.createdAt) {
                fileData.version = version;
                fileData.file = file;
            }
        }

        const result = {
            version: `${lastVersion.version.major}.${lastVersion.version.minor}.${lastVersion.version.revision}`,
            files: Array.from(filesMap.values()).map(f => ({
                UrlPath: f.version.id!,
                LocalPath: f.file.localPath,
                Filename: f.file.fileName,
                Extension: f.file.extension,
                PackedSize: f.file.packedSize,
                OriginalSize: f.file.fileSize,
                CRC32: f.file.crc32,
            })),
        };

        await redis.setex(updateKey, updateCacheDuration, JSON.stringify(result));

        return result;
    } catch(error) {
        logger.error(`[ERROR] getUpdateFiles failed : ${error}`);
        throw(error);
    }
}