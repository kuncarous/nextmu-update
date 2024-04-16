import { getServersFast } from '~/services/api';
import { getUpdateFiles, getVersion, getVersions } from '~/services/api';
import { logger } from '~/logger';
import { Request, Response, Router } from 'express';
import { StatusCodes } from 'http-status-codes';
import ManagerRouter from './manager';
import { ZFetchVersion, ZListVersions, ZRetrieveUpdate } from '~/types/api/v1';

const router = Router();
router.use('/manager', ManagerRouter);

router.get('/version/fetch/:id',
async(req: Request, res: Response) => {
    const parsed = ZFetchVersion.safeParse(req.params);
    if (!parsed.success) {
        return res.status(StatusCodes.BAD_REQUEST).send({});
    }

    try {
        const { id } = parsed.data;
        const version = await getVersion(id);

        return res.status(StatusCodes.OK).json(version);
    } catch (error: any) {
        logger.error(`[Route] Get version error : ${error.message}`);
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send({});
    }
});

router.get('/version/list',
async(req: Request, res: Response) => {
    const parsed = ZListVersions.safeParse(req.query);
    if (!parsed.success) {
        return res.status(StatusCodes.BAD_REQUEST).send({});
    }

    try {
        const { page, size } = parsed.data;
        const versions = await getVersions(page * size, size);

        return res.status(StatusCodes.OK).json(versions);
    } catch (error: any) {
        logger.error(`[Route] List versions error : ${error.message}`);
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send({});
    }
});

router.get('/servers/list',
async (req: Request, res: Response) => {
    try {
        const servers = await getServersFast();
        return res.status(StatusCodes.OK).json({
            servers: servers.map(server => server.url),
        });
    } catch(error) {
        logger.error(`list servers failed (${error})`);
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send({});
    }
});

router.get('/list/:version/:os/:texture/:offset',
async (req: Request, res: Response) => {
    const parsed = ZRetrieveUpdate.safeParse(req.params);
    if (!parsed.success) {
        return res.status(StatusCodes.BAD_REQUEST).send({});
    }

    try {
        const { version, os, texture } = parsed.data;
        const [major, minor, revision] = version.split('.');

        const result = await getUpdateFiles(+major, +minor, +revision, os, texture);
        return res.status(StatusCodes.OK).json(result);
    } catch(error) {
        logger.error(`list updates failed (${error})`);
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send({});
    }
});

export default router;