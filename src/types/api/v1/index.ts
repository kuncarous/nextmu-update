import { ObjectId } from "mongodb";
import { z } from "zod";

export enum OperatingSystems {
    Windows,
    Linux,
    MacOS,
    WindowsMobile,
    Android,
    iOS,
    Max = iOS,
};

export enum TextureFormat {
    eUncompressed,
    BC3,
    BC7,
    ETC2,
    ASTC,
    Max = ASTC,
}

export const VersionRegex = /^(\d){1,2}\.(\d){1,3}\.(\d){1,5}$/;

export const ZRetrieveUpdateRequest = z.object(
    {
        version: z.string().regex(VersionRegex),
        os: z.coerce.number().int().min(0).max(OperatingSystems.Max),
        texture: z.coerce.number().int().min(0).max(TextureFormat.Max),
    }
);
export type IRetrieveUpdateRequest = z.infer<typeof ZRetrieveUpdateRequest>;

export const ZRetrieveUpdateResponse = z.object(
    {
        version: z.string().regex(VersionRegex),
        files: z.array(
            z.object(
                {
                    UrlPath: z.string(),
                    LocalPath: z.string(),
                    Filename: z.string(),
                    Extension: z.string(),
                    PackedSize: z.coerce.number().int().min(0),
                    OriginalSize: z.coerce.number().int().min(0),
                    CRC32: z.string(),
                }
            )
        )
    }
);
export type IRetrieveUpdateResponse = z.infer<typeof ZRetrieveUpdateResponse>;