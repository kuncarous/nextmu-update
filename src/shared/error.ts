import { ServerErrorResponse, status } from '@grpc/grpc-js';
import { Request, Response } from 'express';
import { StatusCodes } from 'http-status-codes';
import { logger } from '~/logger';

export class ResponseOkWithBody extends Error {
    constructor(
        private _code: StatusCodes | status,
        private _body: unknown,
        options?: ErrorOptions,
    ) {
        super('', options);
    }

    get code() {
        return this._code;
    }

    get body() {
        return this._body;
    }

    get json() {
        return JSON.stringify(this._body);
    }
}

export class ResponseError extends Error {
    constructor(
        private _code: StatusCodes | status,
        private _message: string,
        private _log?: string,
        options?: ErrorOptions,
    ) {
        super(_message, options);
    }

    get code() {
        return this._code;
    }

    get log(): string | undefined {
        return this._log;
    }

    get json() {
        return {
            message: this._message || 'unknown error',
        };
    }

    get grpc(): Partial<ServerErrorResponse> {
        return {
            code: this._code as status,
            details: this._message,
        };
    }
}

export class UnavailableError extends ResponseError {
    constructor(
        _code: StatusCodes | status,
        private _display: string,
        _message: string,
        _log?: string,
        options?: ErrorOptions,
    ) {
        super(_code, _message, _log, options);
    }

    get display() {
        return this._display;
    }
}

export enum ErrorSourceType {
    Route = 'Route',
    Middleware = 'Middleware',
    Provider = 'Provider',
}

export const handleError = (
    source: ErrorSourceType | string,
    req: Request,
    res: Response,
    error: unknown,
) => {
    if (error instanceof ResponseOkWithBody) {
        return res.status(error.code).json(error.body);
    }

    if (error instanceof ResponseError) {
        logger.error(
            `[${source}] ${req.baseUrl + req.path} : ${error.log || error.message}`,
        );
    } else if (error instanceof Error) {
        logger.error(
            `[${source}] ${req.baseUrl + req.path} : ${error.message}`,
        );
    }

    if (res.headersSent) return;
    if (error instanceof ResponseError) {
        return res.status(error.code).json(error.json);
    } else {
        return res.status(StatusCodes.INTERNAL_SERVER_ERROR).json({});
    }
};

export const isResponseError = (err: unknown) => {
    return err instanceof ResponseError || err instanceof ResponseOkWithBody;
};
