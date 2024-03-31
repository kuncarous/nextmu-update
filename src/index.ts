import './loadenv';
import app from '~/server';
import { logger } from '~/logger';

const port = Number(process.env.PORT || 3000);
async function start()
{
    app.listen(port, () => logger.info('Express server started on port: ' + port));
}

start();