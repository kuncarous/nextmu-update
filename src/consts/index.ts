export const UsernameLength = { min: 4, max: 16 };
export const UsernameRegex = /^[a-zA-Z0-9_\-.]{4,16}$/;

export const EmailVerifyLapse = 5;

export const gameSessionExpireTime = 60; /* based on seconds */
export const sessionExpireTime = 30 * 24 * 60 * 60; /* based on seconds */
export const redisExpireTime = 10 * 60; /* based on seconds */

export const uploadDir = 'tmp/uploads/';
export const tmpAvatarDir = 'tmp/avatars/';
