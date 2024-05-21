import * as AWSStorage from './aws';
import { StorageProvider, StorageType } from './enums';
import * as GCPStorage from './gcp';
import * as LocalStorage from './local';
import { IStorageOptions } from './types';

export const deleteFolder = async (storageType: StorageType, dir: string) => {
    switch (process.env[`${storageType}_STORAGE_PROVIDER`]) {
        case StorageProvider.Local:
            return await LocalStorage.deleteFolder(storageType, dir);
        case StorageProvider.AWS:
            return await AWSStorage.deleteFolder(storageType, dir);
        case StorageProvider.GCP:
            return await GCPStorage.deleteFolder(storageType, dir);
        default:
            throw new Error(
                `${storageType}_STORAGE_PROVIDER has an invalid value, please configure it.`,
            );
    }
};

export const downloadFile = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    switch (process.env[`${storageType}_STORAGE_PROVIDER`]) {
        case StorageProvider.Local:
            return await LocalStorage.downloadFile(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.AWS:
            return await AWSStorage.downloadFile(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.GCP:
            return await GCPStorage.downloadFile(
                storageType,
                source,
                dest,
                options,
            );
        default:
            throw new Error(
                `${storageType}_STORAGE_PROVIDER has an invalid value, please configure it.`,
            );
    }
};

export const downloadFolder = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    switch (process.env[`${storageType}_STORAGE_PROVIDER`]) {
        case StorageProvider.Local:
            return await LocalStorage.downloadFolder(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.AWS:
            return await AWSStorage.downloadFolder(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.GCP:
            return await GCPStorage.downloadFolder(
                storageType,
                source,
                dest,
                options,
            );
        default:
            throw new Error(
                `${storageType}_STORAGE_PROVIDER has an invalid value, please configure it.`,
            );
    }
};

export const uploadFile = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    switch (process.env[`${storageType}_STORAGE_PROVIDER`]) {
        case StorageProvider.Local:
            return await LocalStorage.uploadFile(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.AWS:
            return await AWSStorage.uploadFile(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.GCP:
            return await GCPStorage.uploadFile(
                storageType,
                source,
                dest,
                options,
            );
        default:
            throw new Error(
                `${storageType}_STORAGE_PROVIDER has an invalid value, please configure it.`,
            );
    }
};

export const uploadBuffer = async (
    storageType: StorageType,
    source: Buffer,
    dest: string,
    options?: IStorageOptions,
) => {
    switch (process.env[`${storageType}_STORAGE_PROVIDER`]) {
        case StorageProvider.Local:
            return await LocalStorage.uploadBuffer(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.AWS:
            return await AWSStorage.uploadBuffer(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.GCP:
            return await GCPStorage.uploadBuffer(
                storageType,
                source,
                dest,
                options,
            );
        default:
            throw new Error(
                `${storageType}_STORAGE_PROVIDER has an invalid value, please configure it.`,
            );
    }
};

export const uploadFolder = async (
    storageType: StorageType,
    source: string,
    dest: string,
    options?: IStorageOptions,
) => {
    switch (process.env[`${storageType}_STORAGE_PROVIDER`]) {
        case StorageProvider.Local:
            return await LocalStorage.uploadFolder(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.AWS:
            return await AWSStorage.uploadFolder(
                storageType,
                source,
                dest,
                options,
            );
        case StorageProvider.GCP:
            return await GCPStorage.uploadFolder(
                storageType,
                source,
                dest,
                options,
            );
        default:
            throw new Error(
                `${storageType}_STORAGE_PROVIDER has an invalid value, please configure it.`,
            );
    }
};
