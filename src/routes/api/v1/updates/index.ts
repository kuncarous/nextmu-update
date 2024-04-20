import { Request, Response, Router } from 'express';
import { StatusCodes } from 'http-status-codes';
import { getServersFast, getUpdateFiles } from '~/services/api';
import { ErrorSourceType, handleError } from '~/shared/error';
import { ZRetrieveUpdateRequest } from '~/types/api/v1';

const router = Router();

router.get('/servers/list', async (req: Request, res: Response) => {
    try {
        const servers = await getServersFast();
        return res.status(StatusCodes.OK).json({
            servers: servers.map((server) => server.url),
        });
    } catch (error: unknown) {
        handleError(ErrorSourceType.Route, req, res, error);
    }
});

router.get(
    '/list/:version/:os/:texture/:offset',
    async (req: Request, res: Response) => {
        const parsed = ZRetrieveUpdateRequest.safeParse(req.params);
        if (!parsed.success) {
            return res.status(StatusCodes.BAD_REQUEST).send({});
        }

        try {
            const { version, os, texture } = parsed.data;
            const [major, minor, revision] = version.split('.');

            const result = await getUpdateFiles(
                +major,
                +minor,
                +revision,
                os,
                texture,
            );
            return res.status(StatusCodes.OK).json(result);
        } catch (error: unknown) {
            handleError(ErrorSourceType.Route, req, res, error);
        }
    },
);

export default router;
