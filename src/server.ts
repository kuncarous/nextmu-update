import express, { RequestHandler } from 'express';
import cookieParser from 'cookie-parser';
import logger from 'morgan';
import cors from 'cors';
import BaseRouter from './routes';
import { permissionErrorHandler } from '~/middlewares/auth';

// Init express
const app = express();

// Add middleware/settings/routes to express.
app.use(express.json());
app.use(express.urlencoded({extended: true}));
app.use(cookieParser());
app.use(cors());

app.use('/', BaseRouter);

// The error handler must be before any other error middleware and after all controllers
app.use(permissionErrorHandler);
app.use(logger('dev') as RequestHandler);

export default app;