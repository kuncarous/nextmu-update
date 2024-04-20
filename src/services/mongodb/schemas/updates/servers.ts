import { ObjectId } from 'mongodb';

export interface IMDBServerInfo {
    _id?: ObjectId;
    name: string;
    description: string;
    url: string;
    createdAt: Date;
    updatedAt: Date;
}
