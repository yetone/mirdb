module.exports = {
  testEnvironment: 'jsdom',
  testMatch: ['**/tests/integration/*.test.js'],
  setupFilesAfterEnv: ['<rootDir>/tests/setup.js'],
  verbose: true,
};
