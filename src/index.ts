import * as gRPC from '~/grpc';
import { logger } from '~/logger';
import app from '~/server';
import './loadenv';

const port = Number(process.env.API_PORT || 8701);
async function start() {
    await gRPC.Initialize();
    app.listen(port, () =>
        logger.info('Express server started on port: ' + port),
    );
}

start();
