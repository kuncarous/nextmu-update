export enum StorageProvider {
    Local = 'local',
    AWS = 'aws',
    GCP = 'gcp',
}

export enum StorageType {
    Input = 'INPUT',
    Output = 'OUTPUT',
}
export const StorageTypes = Object.values(StorageType);