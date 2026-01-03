module.exports = {
  testEnvironment: 'jest-environment-jsdom',
  testMatch: ['**/tests/unit/**/*.test.js'],
  verbose: true,
  setupFiles: ['./jest.setup.js'],
};
