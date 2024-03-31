import { VersionState } from '~/shared/update';
import { ObjectId } from 'mongodb';

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