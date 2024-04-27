import { Server, ServerCredentials } from '@grpc/grpc-js';
import { logger } from '~/logger';
import { updateProto, updateServiceServer } from './services/update';

export const Initialize = async () => {
    const server = new Server({
        'grpc.max_receive_message_length': -1,
        'grpc.max_send_message_length': -1,
    });

    server.addService(
        updateProto.nextmu.v1.UpdateService.service,
        updateServiceServer,
    );
    server.bindAsync(
        `0.0.0.0:${process.env.GRPC_PORT || 8700}`,
        ServerCredentials.createInsecure(),
        (err: Error | null, bindPort: number) => {
            if (err) {
                throw err;
            }

            logger.info(`gRPC:Server:${bindPort}`, new Date().toLocaleString());
        },
    );
};
