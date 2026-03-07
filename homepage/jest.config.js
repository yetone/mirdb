/**
 * Jest Configuration for MirDB Homepage Tests
 */

module.exports = {
  testEnvironment: 'jsdom',
  testMatch: [
    '**/tests/unit/**/*.test.js',
    '**/tests/integration/**/*.test.js'
  ],
  testPathIgnorePatterns: [
    '/node_modules/',
    '/tests/e2e/'
  ],
  moduleFileExtensions: ['js'],
  verbose: true,
  collectCoverage: false,
  testTimeout: 10000
};
