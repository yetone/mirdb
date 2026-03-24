/**
 * Jest Configuration
 * Owner: First scenario builder
 *
 * Configuration:
 * - Test environment (jsdom)
 * - Module resolution
 * - Coverage settings
 */

module.exports = {
  testEnvironment: 'jsdom',
  rootDir: '../../',
  testMatch: ['<rootDir>/tests/unit/**/*.test.js'],
  moduleDirectories: ['node_modules', 'src'],
  collectCoverageFrom: [
    'src/js/**/*.js',
    '!src/js/main.js',
  ],
  coverageDirectory: '<rootDir>/coverage',
  verbose: true,
};
