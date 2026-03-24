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
  testEnvironment: 'node',
  rootDir: '../../',
  testRegex: 'tests/unit/.*\\.test\\.cjs$',
  testPathIgnorePatterns: ['/node_modules/', '/tests/e2e/'],
  moduleDirectories: ['node_modules', 'src'],
  moduleFileExtensions: ['js', 'json', 'cjs'],
  collectCoverageFrom: [
    'src/js/**/*.js',
    '!src/js/main.js',
  ],
  coverageDirectory: '<rootDir>/coverage',
  verbose: true,
  transform: {},
};
