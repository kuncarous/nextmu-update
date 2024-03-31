import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';
import { UpdatesQueue } from './updates';

export const serverAdapter = new ExpressAdapter();
export const bullBoard = createBullBoard(
    {
        queues: [
            new BullMQAdapter(UpdatesQueue),
        ],
        serverAdapter,
    }
);

export * from './updates';