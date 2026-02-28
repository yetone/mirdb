/**
 * Jest Unit Test Configuration for MirDB Homepage
 * @type {import('jest').Config}
 */
module.exports = {
  testEnvironment: 'jsdom',
  testMatch: ['**/tests/unit/**/*.test.js'],
  collectCoverageFrom: ['src/js/**/*.js'],
  coverageDirectory: 'coverage',
  verbose: true,
};
