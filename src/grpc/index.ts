import { Server, ServerCredentials } from '@grpc/grpc-js';
import { logger } from '~/logger';
import { UpdateServiceService } from '~/proto/update_grpc_pb';
import { UpdateServiceServer } from './services/update';

export const Initialize = async () => {
    const server = new Server({
        'grpc.max_receive_message_length': -1,
        'grpc.max_send_message_length': -1,
    });

    server.addService(UpdateServiceService, new UpdateServiceServer());
    server.bindAsync(
        `0.0.0.0:${process.env.GRPC_PORT || 8787}`,
        ServerCredentials.createInsecure(),
        (err: Error | null, bindPort: number) => {
            if (err) {
                throw err;
            }

            logger.info(`gRPC:Server:${bindPort}`, new Date().toLocaleString());
        },
    );
};
