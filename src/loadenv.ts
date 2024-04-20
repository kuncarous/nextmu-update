import commandLineArgs from 'command-line-args';
import dotenv from 'dotenv';

if (!process.env.NODE_ENV) {
    // Setup command line options
    const options = commandLineArgs([
        {
            name: 'env',
            alias: 'e',
            defaultValue: 'development',
            type: String,
        },
    ]);

    // Set the env file
    const result = dotenv.config({
        path: `./env/${options.env}.env`,
    });
    if (result.error) {
        throw result.error;
    }
}
