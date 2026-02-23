/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: 'jest-environment-jsdom',
  testMatch: ['**/tests/unit/**/*.test.js'],
  moduleFileExtensions: ['js'],
  verbose: true,
  setupFiles: ['<rootDir>/tests/unit/setup.js'],
};
