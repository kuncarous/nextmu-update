import oidc from 'oauth4webapi';

declare global {
    namespace Express {
        interface Request {
            auth?: oidc.IntrospectionResponse;
        }
    }
}
