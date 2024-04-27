import type {
    Timestamp,
    Timestamp__Output,
} from '~/proto/google/protobuf/Timestamp';
import type { IVersion } from '~/services/mongodb/schemas/updates/versions';

export const toTimestamp = (date: Date): Timestamp => {
    const time = date.getTime();
    const seconds = Math.floor(time * 0.001);
    const milliseconds = time - seconds * 1000;
    const nanoseconds = milliseconds * 1000000;
    return {
        seconds: seconds,
        nanos: nanoseconds,
    };
};

export const fromTimestamp = (timestamp: Timestamp__Output): Date => {
    const milliseconds =
        Number(timestamp.seconds) * 1000 + timestamp.nanos * 0.000001;
    return new Date(milliseconds);
};

export const getVersionAsString = (version: IVersion) =>
    `${version.major}.${version.minor}.${version.revision}`;
