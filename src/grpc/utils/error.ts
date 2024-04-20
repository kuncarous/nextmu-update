import { ServerUnaryCall, sendUnaryData, status } from '@grpc/grpc-js';
import { logger } from '~/logger';
import { ErrorSourceType, ResponseError } from '~/shared/error';

export const handlegRpcError = <RequestType, ResponseType>(
    source: ErrorSourceType | string,
    call: ServerUnaryCall<RequestType, ResponseType>,
    callback: sendUnaryData<ResponseType>,
    error: unknown,
) => {
    if (error instanceof ResponseError) {
        logger.error(
            `[${source}] ${call.getPath()} : ${error.log || error.message}`,
        );
    } else if (error instanceof Error) {
        logger.error(`[${source}] ${call.getPath()} : ${error.message}`);
    }

    if (error instanceof ResponseError) {
        return callback(error.grpc);
    } else {
        return callback({
            code: status.UNAVAILABLE,
            details: `service is unavailable, try again later`,
        });
    }
};
