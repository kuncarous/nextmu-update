import { RequestHandler, Router } from 'express';
import { serverAdapter } from '~/services/bullmq';

const router = Router();
serverAdapter.setBasePath('/bull/dashboard');
router.use('/', serverAdapter.getRouter() as RequestHandler);

export default router;
