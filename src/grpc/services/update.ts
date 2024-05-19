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
    startUploadVersion,
    uploadVersionChunk,
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
        .string()
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
        .string()
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
    description: z.string().min(1).max(256),
});

const ZStartUploadVersionRequest = z.object({
    id: z
        .string()
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
    hash: z.literal('sha256'),
    type: z.literal('application/zip'),
    chunkSize: z.coerce
        .number()
        .multipleOf(2)
        .min(16 * 1024)
        .max(512 * 1024),
    fileSize: z.coerce.number().min(1 * 1024),
});

const ZUploadVersionChunkRequest = z.object({
    uploadId: z
        .string()
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
    concurrentId: z
        .string()
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
    offset: z.coerce.number().int().min(0),
    data: z.instanceof(Buffer),
});

const ZProcessVersionRequest = z.object({
    id: z
        .string()
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
});

const ViewRoleValidator = validateRoles(['update:view']);
const EditRoleValidator = validateRoles(['update:edit']);

const parseVersion = (
    version: WithId<IMDBVersion> & { filesCount: number },
): Version => {
    return {
        id: version._id.toHexString(),
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
                id: result.id.toHexString(),
                version: getVersionAsString(result.version),
            });
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.CreateVersion',
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
                'UpdateServiceServer.EditVersion',
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
                'UpdateServiceServer.FetchVersion',
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
                'UpdateServiceServer.ListVersions',
                call,
                callback,
                error,
            );
        }
    },
    StartUploadVersion: async (call, callback) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await EditRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZStartUploadVersionRequest.safeParse(call.request);
        if (parsed.success == false) {
            return call.emit<grpc.ServiceError>('error', {
                code: grpc.status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { id, hash, type, chunkSize, fileSize } = parsed.data;
            const response = await startUploadVersion(
                id,
                hash,
                type,
                chunkSize,
                fileSize,
            );

            callback(null, response);
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.StartUploadVersion',
                call,
                callback,
                error,
            );
        }
    },
    UploadVersionChunk: async (call, callback) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await EditRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZUploadVersionChunkRequest.safeParse(call.request);
        if (parsed.success == false) {
            return call.emit<grpc.ServiceError>('error', {
                code: grpc.status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { uploadId, concurrentId, offset, data } = parsed.data;
            const response = await uploadVersionChunk(uploadId, concurrentId, offset, data);

            callback(null, response);
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.UploadVersionChunk',
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
                'UpdateServiceServer.ProcessVersion',
                call,
                callback,
                error,
            );
        }
    },
};
