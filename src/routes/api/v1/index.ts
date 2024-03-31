import { Router } from 'express';
import UpdatesRouter from './updates';

const router = Router();
router.use('/updates', UpdatesRouter);

export default router;