export class InvalidConfigError extends Error {
    constructor(name: string) {
        super();
        this.name = 'InvalidConfigError';
        this.message = `config doesn't exists : ${name}`;
    }
}

export class InvalidFileError extends Error {
    constructor(filename: string) {
        super();
        this.name = 'InvalidFileError';
        this.message = `file doesn't exists : ${filename}`;
    }
}

export class InvalidBucketError extends Error {
    constructor(bucket: string) {
        super();
        this.name = 'InvalidFileError';
        this.message = `bucket doesn't exists : ${bucket}`;
    }
}

export class EmptyFolderError extends Error {
    constructor(directory: string) {
        super();
        this.name = 'InvalidFileError';
        this.message = `folder doesn't exists : ${directory}`;
    }
}
