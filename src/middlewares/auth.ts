import { NextFunction, Request, Response } from 'express';
import StatusCodes from 'http-status-codes';
import oidc from 'oauth4webapi';

export const authProvider = process.env.OPENID_AUTH_PROVIDER || 'zitadel';

let authorizationServer: oidc.AuthorizationServer | null = null;
export const getAuthorizationServer = async () => {
    try {
        if (authorizationServer != null) return authorizationServer;

        const url = new URL(process.env.OPENID_ISSUER_URL!);
        const response = await oidc.discoveryRequest(url);
        if (!response.ok) {
            return null;
        }

        return await oidc.processDiscoveryResponse(url, response);
    } catch (error) {
        return null;
    }
};

let ApiAuthClient: oidc.Client | null = null;
export const getApiAuthClient = async () => {
    if (ApiAuthClient != null) return ApiAuthClient;
    ApiAuthClient = {
        client_id: process.env.OPENID_CLIENT_ID!,
        client_secret: process.env.OPENID_CLIENT_SECRET!,
        token_endpoint_auth_method: (process.env.OPENID_AUTH_METHOD ||
            'client_secret_basic') as oidc.ClientAuthenticationMethod,
    };
    return ApiAuthClient;
};

type ServerErrorCode =
    | 'service_unavailable'
    | 'credentials_required'
    | 'invalid_token'
    | 'permission_denied'
    | 'not_implemented';
class NServerError extends Error {
    status: number;
    message: string;
    name: string;
    code: ServerErrorCode;
    inner: { message: string };

    constructor(code: ServerErrorCode, error: { message: string }) {
        super(error.message);
        this.name = this.constructor.name;
        this.message = error.message;
        this.code = code;
        this.status = StatusCodes.FORBIDDEN;
        this.inner = error;
    }
}

const ServiceUnavailableError = new NServerError('service_unavailable', {
    message: 'Service unavailable',
});

const CredentialsRequiredError = new NServerError('credentials_required', {
    message: 'Credentials required',
});

const InvalidTokenError = new NServerError('invalid_token', {
    message: 'Invalid authorization received',
});

const PermissionDeniedError = new NServerError('permission_denied', {
    message: 'Permission denied',
});

const NotImplementedError = new NServerError('not_implemented', {
    message: 'Support not implemented',
});

export const requireAuth = async (
    req: Request,
    res: Response,
    next: NextFunction,
) => {
    const values = req.headers.authorization?.split(' ') ?? [];
    if (values.length != 2) return next(CredentialsRequiredError);

    const [type, token] = values;
    if (type !== 'Bearer') return next(InvalidTokenError);

    const authServer = await getAuthorizationServer();
    if (authServer == null) return next(ServiceUnavailableError);

    const client = await getApiAuthClient();
    if (client == null) return next(ServiceUnavailableError);

    const response = await oidc.introspectionRequest(
        authServer,
        client,
        token,
        { additionalParameters: { token_hint_type: 'access_token' } },
    );
    if (!response.ok) return next(ServiceUnavailableError);

    const result = await oidc.processIntrospectionResponse(
        authServer,
        client,
        response,
    );
    if (oidc.isOAuth2Error(result)) return next(PermissionDeniedError);

    req.auth = result;

    return next();
};

export const optionalAuth = async (
    req: Request,
    res: Response,
    next: NextFunction,
) => {
    const values = req.headers.authorization?.split(' ') ?? [];
    if (values.length != 2) return next();

    const [type, token] = values;
    if (type !== 'Bearer') return next(InvalidTokenError);

    const authServer = await getAuthorizationServer();
    if (authServer == null) return next(ServiceUnavailableError);

    const client = await getApiAuthClient();
    if (client == null) return next(ServiceUnavailableError);

    const response = await oidc.introspectionRequest(
        authServer,
        client,
        token,
        { additionalParameters: { token_hint_type: 'access_token' } },
    );
    if (!response.ok) return next(ServiceUnavailableError);

    const result = await oidc.processIntrospectionResponse(
        authServer,
        client,
        response,
    );
    if (oidc.isOAuth2Error(result)) return next(PermissionDeniedError);

    req.auth = result;

    return next();
};

export const requireRoles = (roles: string[]) => {
    if (roles.length == 0) {
        return async (req: Request, res: Response, next: NextFunction) => {
            return next();
        };
    }

    switch (authProvider) {
        case 'zitadel':
            return async (req: Request, res: Response, next: NextFunction) => {
                if (roles.length == 0) return next();
                if (req.auth == null) return next(PermissionDeniedError);
                if (!('urn:zitadel:iam:org:project:roles' in req.auth))
                    return next(PermissionDeniedError);
                const userRoles = Object.keys(
                    req.auth[
                        'urn:zitadel:iam:org:project:roles'
                    ] as oidc.JsonObject,
                );
                for (const role in roles) {
                    if (!userRoles.includes(role))
                        return next(PermissionDeniedError);
                }
                return next();
            };

        default:
            return async (req: Request, res: Response, next: NextFunction) => {
                return next(NotImplementedError);
            };
    }
};

export const permissionErrorHandler = async (
    err: NServerError | Error | undefined,
    req: Request,
    res: Response,
    next: NextFunction,
) => {
    if (err == null || !(err instanceof NServerError)) return next(err);
    switch (err.code) {
        case 'invalid_token':
            return res.status(StatusCodes.UNAUTHORIZED).send({});
        case 'permission_denied':
            return res.status(StatusCodes.FORBIDDEN).send({});
        case 'credentials_required':
            return res.status(StatusCodes.UNAUTHORIZED).send({});
        case 'service_unavailable':
            return res.status(StatusCodes.SERVICE_UNAVAILABLE).send({});
        case 'not_implemented':
            return res.status(StatusCodes.NOT_IMPLEMENTED).send({});
        default:
            next(err);
    }
};
