import { ObjectId } from 'mongodb';
import { VersionState } from '~/proto/update_pb';

export interface IMDBVersion {
    _id?: ObjectId;
    id?: string;

    version: {
        major: number;
        minor: number;
        revision: number;
    };
    description: string;
    state: VersionState;
    filesCount?: number;

    createdAt: Date;
    updatedAt: Date;
}
