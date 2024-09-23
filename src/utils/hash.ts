import * as crypto from 'node:crypto';
import * as fs from 'node:fs';

export async function calculateFileHash(
    filePath: string,
    algorithm: string = 'sha256',
): Promise<string> {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash(algorithm);
        const stream = fs.createReadStream(filePath);

        stream.on('data', (data) => {
            hash.update(data);
        });

        stream.on('end', () => {
            const sha256 = hash.digest('hex');
            resolve(sha256);
        });

        stream.on('error', (err) => {
            reject(err);
        });
    });
}
