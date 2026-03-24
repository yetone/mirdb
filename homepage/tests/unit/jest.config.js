/**
 * Jest Configuration
 * Owner: First scenario builder
 *
 * Configuration:
 * - Test environment (jsdom)
 * - Module resolution
 * - Coverage settings
 */

export default {
  testEnvironment: 'jsdom',
  rootDir: '../../',
  testMatch: ['<rootDir>/tests/unit/**/*.test.js'],
  moduleFileExtensions: ['js', 'json'],
  collectCoverageFrom: [
    'src/js/**/*.js',
    '!src/js/main.js',
  ],
  coverageDirectory: 'coverage',
  verbose: true,
};
