import { ObjectId } from "mongodb";
import z from "zod";
import { OperatingSystems, TextureFormat, VersionType } from '~/shared/update';

export const ZCreateVersion = z.object(
    {
        type: z.coerce.number().int().min(VersionType.Revision).max(VersionType.Major),
        description: z.string().min(1).max(256),
    }
);

export const ZEditVersion = z.object(
    {
        id: z.string().refine(value => ObjectId.isValid(value)),
        description: z.string().min(1).max(256),
    }
);

export const ZProcessVersion = z.object(
    {
        id: z.string().refine(value => ObjectId.isValid(value)),
    }
);

export const ZFetchVersion = z.object(
    {
        id: z.string().refine(value => ObjectId.isValid(value)),
    }
);

export const ZListVersions = z.object(
    {
        page: z.number().int().min(0),
        size: z.coerce.number().int().min(4).max(50),
    }
);

export const ZRetrieveUpdate = z.object(
    {
        version: z.string().regex(/^(\d){1,2}\.(\d){1,3}\.(\d){1,5}$/),
        os: z.coerce.number().int().min(0).max(OperatingSystems.Max),
        texture: z.coerce.number().int().min(0).max(TextureFormat.Max),
    }
);