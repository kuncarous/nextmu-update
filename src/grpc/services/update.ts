import {
    ServiceError,
    UntypedHandleCall,
    handleUnaryCall,
    status,
} from '@grpc/grpc-js';
import * as google_protobuf_empty_pb from 'google-protobuf/google/protobuf/empty_pb';
import { Empty } from 'google-protobuf/google/protobuf/empty_pb';
import { Timestamp } from 'google-protobuf/google/protobuf/timestamp_pb';
import { ObjectId } from 'mongodb';
import { z } from 'zod';
import { IUpdateServiceServer } from '~/proto/update_grpc_pb';
import {
    CreateVersionRequest,
    CreateVersionResponse,
    EditVersionRequest,
    FetchVersionRequest,
    FetchVersionResponse,
    ListVersionsRequest,
    ListVersionsResponse,
    ProcessVersionRequest,
    Version,
    VersionState,
    VersionType,
} from '~/proto/update_pb';
import {
    createVersion,
    editVersion,
    getVersion,
    getVersions,
    processVersion,
} from '~/services/api';
import { retrieveAuthMetadata, validateRoles } from '../middlewares/auth';
import { handlegRpcError } from '../utils/error';

const ZVersion = z.object({
    id: z.instanceof(Uint8Array),
    version: z.string(),
    description: z.string(),
    state: z.nativeEnum(VersionState),
    filesCount: z.coerce.number().int().min(0),
    createdAt: z.coerce.date(),
    updatedAt: z.coerce.date(),
});
type IVersion = z.infer<typeof ZVersion>;

function CreateVersionFromObject(v: IVersion) {
    const result = new Version();
    result.setId(v.id);
    result.setVersion(v.version);
    result.setDescription(v.description);
    result.setState(v.state);
    result.setFilesCount(v.filesCount);
    result.setCreatedAt(Timestamp.fromDate(v.createdAt));
    result.setUpdatedAt(Timestamp.fromDate(v.updatedAt));
    return result;
}

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

const ZProcessVersionRequest = z.object({
    id: z
        .string()
        .refine((v) => ObjectId.isValid(v))
        .transform((v) => new ObjectId(v)),
});

const ViewRoleValidator = validateRoles(['update:view']);
const EditRoleValidator = validateRoles(['update:edit']);

class UpdateServiceServer implements IUpdateServiceServer {
    // eslint-disable-next-line no-undef
    [name: string]: UntypedHandleCall;

    listVersions: handleUnaryCall<ListVersionsRequest, ListVersionsResponse> =
        async (call, callback) => {
            const [auth, error] = await retrieveAuthMetadata(call);
            if (error !== null) return callback(error);

            const roles_error = await ViewRoleValidator(auth!);
            if (error != null) return callback(roles_error);

            const parsed = ZListVersionsRequest.safeParse(
                call.request.toObject(),
            );
            if (parsed.success == false) {
                return call.emit<ServiceError>('error', {
                    code: status.INVALID_ARGUMENT,
                    details: parsed.error.format()._errors.join('\n'),
                });
            }

            try {
                const { page, size } = parsed.data;
                const result = await getVersions(page * size, size);

                const response = new ListVersionsResponse();
                response.setAvailableCount(result.count);
                response.setVersionsList(
                    result.data.map((v) => CreateVersionFromObject(v)),
                );

                callback(null, response);
            } catch (error) {
                handlegRpcError(
                    'UpdateServiceServer.listVersions',
                    call,
                    callback,
                    error,
                );
            }
        };

    fetchVersion: handleUnaryCall<FetchVersionRequest, FetchVersionResponse> =
        async (call, callback) => {
            const [auth, error] = await retrieveAuthMetadata(call);
            if (error !== null) return callback(error);

            const roles_error = await ViewRoleValidator(auth!);
            if (error != null) return callback(roles_error);

            const parsed = ZFetchVersionRequest.safeParse(
                call.request.toObject(),
            );
            if (parsed.success == false) {
                return call.emit<ServiceError>('error', {
                    code: status.INVALID_ARGUMENT,
                    details: parsed.error.format()._errors.join('\n'),
                });
            }

            try {
                const { id } = parsed.data;
                const result = await getVersion(id);

                const response = new FetchVersionResponse();
                response.setVersion(CreateVersionFromObject(result));

                callback(null, response);
            } catch (error) {
                handlegRpcError(
                    'UpdateServiceServer.fetchVersion',
                    call,
                    callback,
                    error,
                );
            }
        };

    createVersion: handleUnaryCall<
        CreateVersionRequest,
        CreateVersionResponse
    > = async (call, callback) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await EditRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZCreateVersionRequest.safeParse(call.request.toObject());
        if (parsed.success == false) {
            return call.emit<ServiceError>('error', {
                code: status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { type, description } = parsed.data;
            const result = await createVersion(type, description);

            const response = new CreateVersionResponse();
            response.setId(result.id);
            response.setVersion(result.version);

            callback(null, response);
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.createVersion',
                call,
                callback,
                error,
            );
        }
    };

    editVersion: handleUnaryCall<EditVersionRequest, Empty> = async (
        call,
        callback,
    ) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await EditRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZEditVersionRequest.safeParse(call.request.toObject());
        if (parsed.success == false) {
            return call.emit<ServiceError>('error', {
                code: status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { id, description } = parsed.data;
            await editVersion(id, description);
            callback(null, new google_protobuf_empty_pb.Empty());
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.editVersion',
                call,
                callback,
                error,
            );
        }
    };

    processVersion: handleUnaryCall<ProcessVersionRequest, Empty> = async (
        call,
        callback,
    ) => {
        const [auth, error] = await retrieveAuthMetadata(call);
        if (error !== null) return callback(error);

        const roles_error = await EditRoleValidator(auth!);
        if (error != null) return callback(roles_error);

        const parsed = ZProcessVersionRequest.safeParse(
            call.request.toObject(),
        );
        if (parsed.success == false) {
            return call.emit<ServiceError>('error', {
                code: status.INVALID_ARGUMENT,
                details: parsed.error.format()._errors.join('\n'),
            });
        }

        try {
            const { id } = parsed.data;
            await processVersion(id.toHexString());
            callback(null, new google_protobuf_empty_pb.Empty());
        } catch (error) {
            handlegRpcError(
                'UpdateServiceServer.processVersion',
                call,
                callback,
                error,
            );
        }
    };
}

export { IUpdateServiceServer, UpdateServiceServer };
