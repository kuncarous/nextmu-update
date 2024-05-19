import type { FileInfo } from "~/utils";

export interface IStorageOptions {
    onProgress: (progress: number) => void;
}

export type FileInfoExt = FileInfo & {
    size?: number;
};