module.exports = {
  testEnvironment: 'jest-environment-jsdom',
  testMatch: ['**/tests/**/*.test.js'],
  verbose: true,
  setupFiles: ['<rootDir>/tests/setup.js'],
};
