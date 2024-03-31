import { serverAdapter } from '~/services/bullmq';
import { RequestHandler, Router } from 'express';

const router = Router();
serverAdapter.setBasePath('/bull/dashboard');
router.use('/', serverAdapter.getRouter() as RequestHandler);

export default router;