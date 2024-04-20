export const updateCacheDuration = 8 * 60 * 60; // 8 Hours

export enum UpdatesCategories {
    General = 0,

    Desktop = 1,
    Mobile = 2,

    Uncompressed = 10,
    BC3 = 11,
    BC7 = 12,
    ETC2 = 13,
    ASTC = 14,

    Windows = 20,
    Linux = 21,
    MacOS = 22,
    Android = 23,
    iOS = 24,
}

export const PlatformLookup = [
    UpdatesCategories.Desktop,
    UpdatesCategories.Desktop,
    UpdatesCategories.Desktop,
    UpdatesCategories.Mobile,
    UpdatesCategories.Mobile,
    UpdatesCategories.Mobile,
];

export const OperatingSystemLookup = [
    UpdatesCategories.Windows,
    UpdatesCategories.Linux,
    UpdatesCategories.MacOS,
    UpdatesCategories.General,
    UpdatesCategories.Android,
    UpdatesCategories.iOS,
];

export const TextureLookup = [
    UpdatesCategories.Uncompressed,
    UpdatesCategories.BC3,
    UpdatesCategories.BC7,
    UpdatesCategories.ETC2,
    UpdatesCategories.ASTC,
];

export const UpdateTypeLookup = [
    UpdatesCategories.General,

    UpdatesCategories.Desktop,
    UpdatesCategories.Mobile,

    UpdatesCategories.Uncompressed,
    UpdatesCategories.BC3,
    UpdatesCategories.BC7,
    UpdatesCategories.ETC2,
    UpdatesCategories.ASTC,

    UpdatesCategories.Windows,
    UpdatesCategories.Linux,
    UpdatesCategories.MacOS,
    UpdatesCategories.Android,
    UpdatesCategories.iOS,
];

export const incomingUpdatesPath = './tmp/incoming/';
export const processedUpdatesPath = './tmp/processed/';
export const incomingFolders = [
    'general',

    'desktop',
    'mobile',

    'uncompressed',
    'bc3',
    'bc7',
    'etc2',
    'astc',

    'windows',
    'linux',
    'macos',
    'android',
    'ios',
];
