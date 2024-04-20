/**
 * This is intended to be a basic starting point for linting in your app.
 * It relies on recommended configs out of the box for simplicity, but you can
 * and should modify this configuration to best suit your team's needs.
 */

import js from '@eslint/js';
import tsEslintConfig from '@typescript-eslint/eslint-plugin';
import tsParser from '@typescript-eslint/parser';
import gitignore from 'eslint-config-flat-gitignore';
import eslintConfigPrettier from 'eslint-config-prettier';
import globals from 'globals';

/** @type {import('eslint').Linter.FlatConfig[]} */
export default [
    gitignore(),
    js.configs.recommended,
    {
        languageOptions: {
            ecmaVersion: 'latest',
            sourceType: 'module',
            parserOptions: {
                ecmaFeatures: {
                    jsx: true,
                },
            },
            globals: {
                ...globals.node,
                ...globals.commonjs,
            },
        },
    },

    // Typescript
    {
        files: ['src/**/*.{ts,tsx}'],
        plugins: {
            '@typescript-eslint': tsEslintConfig,
        },
        languageOptions: {
            parser: tsParser,
        },
        rules: {
            ...tsEslintConfig.configs.base.rules,
            ...tsEslintConfig.configs['eslint-recommended'].rules,
            ...tsEslintConfig.configs.recommended.rules,
        },
    },
    eslintConfigPrettier,
];
