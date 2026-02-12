/**
 * Jest Configuration for MirDB Homepage Unit Tests
 */
module.exports = {
  testEnvironment: 'jsdom',
  testMatch: ['**/tests/unit/**/*.test.js'],
  setupFiles: ['./tests/setup.js'],
  verbose: true,
  collectCoverageFrom: [
    'js/**/*.js',
    '!js/**/*.test.js',
  ],
  coverageDirectory: 'coverage',
  moduleFileExtensions: ['js', 'json'],
};
