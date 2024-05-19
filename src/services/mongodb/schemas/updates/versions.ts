import { VersionState } from '~/proto/nextmu/v1/VersionState';

export interface IVersion {
    major: number;
    minor: number;
    revision: number;
}
export interface IMDBVersion {
    version: IVersion;
    description: string;
    state: VersionState;
    file?: string;

    createdAt: Date;
    updatedAt: Date;
}
