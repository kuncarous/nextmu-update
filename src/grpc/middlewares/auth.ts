import { ServerErrorResponse, ServerUnaryCall, status } from '@grpc/grpc-js';
import * as oidc from 'oauth4webapi';
import {
    authProvider,
    getApiAuthClient,
    getAuthorizationServer,
} from '~/middlewares/auth';

type IRetrieveAuthResponse = [
    oidc.IntrospectionResponse | null,
    Partial<ServerErrorResponse> | null,
];
export const retrieveAuthMetadata = async <RequestType, ResponseType>(
    call: ServerUnaryCall<RequestType, ResponseType>,
    optional: boolean = false,
): Promise<IRetrieveAuthResponse> => {
    const authorizationMetadata = call.metadata.get('authorization');
    if (authorizationMetadata.length !== 1) {
        return [
            null,
            !optional
                ? {
                      code: status.UNAUTHENTICATED,
                      details:
                          'request is missing authorization token or has more than one',
                  }
                : null,
        ];
    }

    const [authorization] = authorizationMetadata;
    if (typeof authorization !== 'string') {
        return [
            null,
            {
                code: status.UNAUTHENTICATED,
                details: 'authorization metadata data type is invalid',
            },
        ];
    }

    const values = authorization.split(' ') ?? [];
    if (values.length !== 2) {
        return [
            null,
            {
                code: status.UNAUTHENTICATED,
                details: 'authorization metadata format is invalid',
            },
        ];
    }

    const [type, token] = values;
    if (type !== 'Bearer') {
        return [
            null,
            {
                code: status.UNAUTHENTICATED,
                details: "authorization metadata isn't a valid bearer token",
            },
        ];
    }

    const authServer = await getAuthorizationServer();
    if (authServer == null) {
        return [
            null,
            {
                code: status.UNAVAILABLE,
                details: 'authorization server is unavailable, try again later',
            },
        ];
    }

    const client = await getApiAuthClient();
    const response = await oidc.introspectionRequest(
        authServer,
        client,
        token,
        { additionalParameters: { token_hint_type: 'access_token' } },
    );
    if (!response.ok) {
        return [
            null,
            {
                code: status.UNAVAILABLE,
                details: 'authorization server is unavailable, try again later',
            },
        ];
    }

    const result = await oidc.processIntrospectionResponse(
        authServer,
        client,
        response,
    );
    if (oidc.isOAuth2Error(result)) {
        return [
            null,
            {
                code: status.PERMISSION_DENIED,
                details: 'authorization token is invalid or has been refused',
            },
        ];
    }

    return [result, null];
};

type ValidateRolesFunction = (
    auth: oidc.IntrospectionResponse,
) => Promise<Partial<ServerErrorResponse> | null>;
export const validateRoles = (roles: string[]): ValidateRolesFunction => {
    if (roles.length == 0) {
        return async () => {
            return null;
        };
    }

    switch (authProvider) {
        case 'zitadel':
            return async (auth) => {
                if (
                    auth == null ||
                    !('urn:zitadel:iam:org:project:roles' in auth)
                ) {
                    return {
                        code: status.PERMISSION_DENIED,
                        details: "you don't have enough permissions",
                    };
                }

                const userRoles = Object.keys(
                    auth[
                        'urn:zitadel:iam:org:project:roles'
                    ] as oidc.JsonObject,
                );
                for (const role of roles) {
                    if (userRoles.includes(role) == true) continue;
                    return {
                        code: status.PERMISSION_DENIED,
                        details: "you don't have enough permissions",
                    };
                }

                return null;
            };

        default:
            return async () => {
                return {
                    code: status.UNIMPLEMENTED,
                    details: 'auth provider not implemented',
                };
            };
    }
};
