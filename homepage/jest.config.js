/** @type {import('jest').Config} */
module.exports = {
  projects: [
    {
      displayName: 'unit',
      testEnvironment: 'jest-environment-jsdom',
      testMatch: ['**/tests/unit/**/*.test.js'],
      moduleFileExtensions: ['js'],
      verbose: true,
      setupFiles: ['<rootDir>/tests/unit/setup.js'],
    },
    {
      displayName: 'integration',
      testEnvironment: 'node',
      testMatch: ['**/tests/integration/**/*.test.js'],
      moduleFileExtensions: ['js'],
      verbose: true,
    },
  ],
};
