export interface IProcessUploadVersionJobData {
    versionId: string;
    uploadId: string;
    concurrentId: string;
}

export interface IProcessPublishVersionJobData {
    versionId: string;
}

export enum UpdateServiceJobType {
    ProcessUploadVersion,
    ProcessPublishVersion,
}

export type UpdateJobData =
    | {
          type: UpdateServiceJobType.ProcessUploadVersion;
          data: IProcessUploadVersionJobData;
      }
    | {
          type: UpdateServiceJobType.ProcessPublishVersion;
          data: IProcessPublishVersionJobData;
      };
