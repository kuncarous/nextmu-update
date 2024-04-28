import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { ObjectId, WithId } from 'mongodb';
import { z } from 'zod';
import type { UpdateServiceHandlers } from '~/proto/nextmu/v1/UpdateService';
import type { Version } from '~/proto/nextmu/v1/Version';
import { VersionType } from '~/proto/nextmu/v1/VersionType';
import type { ProtoGrpcType } from '~/proto/update';
import {
    createVersion,
    editVersion,
    getVersion,
    getVersions,
    processVersion,
} from '~/services/api';
import { IMDBVersion } from '~/services/mongodb/schemas/updates/versions';
import { getVersionAsString, toTimestamp } from '~/utils';
import { retrieveAuthMetadata, validateRoles } from '../middlewares/auth';
import { defaultProtoLoaderConfig } from '../utils/config';
import { handlegRpcError } from '../utils/error';

const updateDefinition = protoLoader.loadSync(
    'proto/models/update.proto',
    defaultProtoLoaderConfig,
);
export const updateProto = grpc.loadPackageDefinition(
    updateDefinition,
) as unknown as ProtoGrpcType;

const ZListVersionsRequest = z.object({
    page: z.number().int().min(0),
    size: z.number().int().multipleOf(5).min(5).max(50),
});

const ZFetchVersionRequest = z.object({
    id: z
        .instanceof(Buffer)
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
});

const ZCreateVersionRequest = z.object({
    type: z.coerce
        .number()
        .int()
        .min(VersionType.REVISION)
        .max(VersionType.MAJOR),
    description: z.string().min(1).max(256),
});

const ZEditVersionRequest = z.object({
    id: z
        .instanceof(Buffer)
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
    description: z.string().min(1).max(256),
});

const ZProcessVersionRequest = z.object({
    id: z
        .instanceof(Buffer)
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
});

const ViewRoleValidator = validateRoles(['update:view']);
const EditRoleValidator = validateRoles(['update:edit']);

const parseVersion = (
    version: WithId<IMDBVersion> & { filesCount: number },
): Version => {
    return {
        id: version._id.id,
        version: getVersionAsString(version.version),
        description: version.description,
        state: version.state,
        filesCount: version.filesCount,
        createdAt: toTimestamp(version.createdAt),
        updatedAt: toTimestamp(version.updatedAt),
    };
};

export const updateServiceServer: UpdateServiceHandlers = {
    CreateVersion: async (call, callback) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await EditRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZCreateVersionRequest.safeParse(call.request);
        if (parsed.success == false) {
            return call.emit<grpc.ServiceError>('error', {
                code: grpc.status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { type, description } = parsed.data;
            const result = await createVersion(
                type as VersionType,
                description,
            );

            callback(null, {
                id: result.id.id,
                version: getVersionAsString(result.version),
            });
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.createVersion',
                call,
                callback,
                error,
            );
        }
    },
    EditVersion: async (call, callback) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await EditRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZEditVersionRequest.safeParse(call.request);
        if (parsed.success == false) {
            return call.emit<grpc.ServiceError>('error', {
                code: grpc.status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { id, description } = parsed.data;
            await editVersion(id, description);
            callback(null, {});
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.editVersion',
                call,
                callback,
                error,
            );
        }
    },
    FetchVersion: async (call, callback) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await ViewRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZFetchVersionRequest.safeParse(call.request);
        if (parsed.success == false) {
            return call.emit<grpc.ServiceError>('error', {
                code: grpc.status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { id } = parsed.data;
            const result = await getVersion(id);

            callback(null, {
                version: parseVersion(result),
            });
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.fetchVersion',
                call,
                callback,
                error,
            );
        }
    },
    ListVersions: async (call, callback) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await ViewRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZListVersionsRequest.safeParse(call.request);
        if (parsed.success == false) {
            return call.emit<grpc.ServiceError>('error', {
                code: grpc.status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { page, size } = parsed.data;
            const result = await getVersions(page * size, size);

            callback(null, {
                availableCount: result.count,
                versions: result.data.map((v) => parseVersion(v)),
            });
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.listVersions',
                call,
                callback,
                error,
            );
        }
    },
    ProcessVersion: async (call, callback) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await EditRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZProcessVersionRequest.safeParse(call.request);
        if (parsed.success == false) {
            return call.emit<grpc.ServiceError>('error', {
                code: grpc.status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { id } = parsed.data;
            await processVersion(id.toHexString());
            callback(null, {});
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.processVersion',
                call,
                callback,
                error,
            );
        }
    },
};
