import { logger } from '~/logger';
import { Request, Response, Router } from 'express';
import { StatusCodes } from 'http-status-codes';
import { createVersion, editVersion } from '~/services/api';
import { UpdatesQueue } from '~/services/bullmq';
import { requireAuth, requireRoles } from '~/middlewares/auth';
import { ZCreateVersion, ZEditVersion, ZProcessVersion } from '~/types/api/v1';

const router = Router();

router.use(requireAuth);

router.get('/version/jobs', [
    requireRoles(["update:view"]),
], async(req: Request, res: Response) => {
    try {
        const activeJobs = await UpdatesQueue.getActive();
        const waitingJobs = await UpdatesQueue.getWaiting();

        return res.status(StatusCodes.OK).json(
            [
                ...activeJobs.map((job) => ({
                    id: job.data.versionId,
                    progress: job.progress,
                    waiting: false,
                })),
                ...waitingJobs.map((job) => ({
                    id: job.data.versionId,
                    progress: 0,
                    waiting: true,
                }))
            ]
        );
    } catch (error: any) {
        logger.error(`[Route] Get processing jobs error : ${error.message}`);
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send({});
    }
});

router.post('/version/create', [
    requireRoles(["update:edit"]),
], async (req: Request, res: Response) => {
    const parsed = ZCreateVersion.safeParse(req.body);
    if (!parsed.success) {
        return res.status(StatusCodes.BAD_REQUEST).send({});
    }

    try {
        const { type, description } = parsed.data;
        const result = await createVersion(type, description);

        return res.status(StatusCodes.OK).json(result);
    } catch (error: any) {
        logger.error(`[Route] Create version error : ${error.message}`);
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send({});
    }
});

router.post('/version/edit', [
    requireRoles(["update:edit"]),
], async (req: Request, res: Response) => {
    const parsed = ZEditVersion.safeParse(req.body);
    if (!parsed.success) {
        return res.status(StatusCodes.BAD_REQUEST).send({});
    }

    try {
        const { id, description } = parsed.data;
        const result = await editVersion(id, description);

        return res.status(StatusCodes.OK).json({ success: result });
    } catch (error: any) {
        logger.error(`[Route] Edit version error : ${error.message}`);
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send({});
    }
});

router.put('/version/process', [
    requireRoles(["update:edit"]),
], async (req: Request, res: Response) => {
    const parsed = ZProcessVersion.safeParse(req.body);
    if (!parsed.success) {
        return res.status(StatusCodes.BAD_REQUEST).send({});
    }

    try {
        const { id } = parsed.data;
        const jobId = `version-${id}`;
        let job = await UpdatesQueue.getJob(jobId);
        if (job) {
            if (await job.isFailed()) {
                await job.remove();
                job = undefined;
            }
        }
        
        if (!job) {
            job = await UpdatesQueue.add(
                'processUpdate',
                {
                    versionId: id,
                },
                {
                    jobId,
                    removeOnComplete: true
                }
            );
        }

        return res.status(StatusCodes.OK).send({
            jobId: job.id,
        });
    } catch (error: any) {
        logger.error(`[Route] Process version error : ${error.message}`);
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send({});
    }
});

export default router;
