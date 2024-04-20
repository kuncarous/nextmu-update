import moment from 'moment';
import { logger } from '~/logger';
import { getMongoClient } from '../mongodb/client';
import { IMDBServerInfo } from '../mongodb/schemas/updates/servers';

export const addServer = async (
    name: string,
    description: string,
    url: string,
) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const createdAt = moment();
        const serversColl = client
            .db('updates')
            .collection<IMDBServerInfo>('servers');
        await serversColl.insertOne({
            name,
            description,
            url,
            createdAt: createdAt.toDate(),
            updatedAt: createdAt.toDate(),
        });
    } catch (error) {
        logger.error(`[ERROR] addServer failed : ${error}`);
        throw error;
    }
};

export const getServers = async (offset: number, count: number) => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const serversColl = client
            .db('updates')
            .collection<IMDBServerInfo>('servers');

        const serversResult = await serversColl
            .aggregate<{
                count: number;
                data: IMDBServerInfo[];
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

        const deferredData = serversResult.length > 0 ? serversResult[0] : null;
        return deferredData != null
            ? {
                  data: deferredData.data.map((v) => ({
                      id: v._id!.toHexString(),
                      name: v.name,
                      description: v.description,
                      url: v.url,
                      createdAt: v.createdAt,
                      updatedAt: v.updatedAt,
                  })),
                  count: deferredData.count,
              }
            : {
                  data: [],
                  count: 0,
              };
    } catch (error) {
        logger.error(`[ERROR] getServers failed : ${error}`);
        throw error;
    }
};

export const getServersFast = async () => {
    try {
        const client = await getMongoClient();
        if (!client) {
            throw new Error('getMongoClient failed');
        }

        const serversColl = client
            .db('updates')
            .collection<IMDBServerInfo>('servers');

        const serversResult = await serversColl.find().toArray();

        return serversResult;
    } catch (error) {
        logger.error(`[ERROR] getServersFast failed : ${error}`);
        throw error;
    }
};
