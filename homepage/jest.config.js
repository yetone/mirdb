/**
 * Jest Unit Test Configuration
 * Owner: First Builder
 *
 * Expected configuration:
 * - Test environment: jsdom
 * - Test match patterns for unit tests
 * - Coverage configuration
 * - Module resolution
 */

module.exports = {
  testEnvironment: 'jsdom',
  testMatch: [
    '**/tests/unit/**/*.test.js',
    '**/tests/integration/**/*.test.js'
  ],
  moduleFileExtensions: ['js', 'json'],
  collectCoverageFrom: [
    'theme/js/**/*.js'
  ],
  coverageDirectory: 'coverage',
  verbose: true
};
