import { Router } from 'express';
import ApiRouter from './api';
import BullRouter from './bull';

// Init router and path
const router = Router();

// Add sub-routes
router.use('/api', ApiRouter);
router.use('/bull/dashboard', BullRouter);

// Export the base-router
export default router;
