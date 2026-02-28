/**
 * Jest Unit Test Configuration for MirDB Homepage
 */
module.exports = {
  testEnvironment: 'jsdom',
  testMatch: ['**/tests/unit/**/*.test.js'],
  collectCoverageFrom: ['src/js/**/*.js'],
  coverageDirectory: 'coverage',
  verbose: true,
};
