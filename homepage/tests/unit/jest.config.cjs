/**
 * Jest Configuration
 * Owner: Scenario 1 - Hero Section Display
 *
 * Configuration for unit tests across all scenarios.
 */

module.exports = {
  testEnvironment: 'jsdom',
  rootDir: '../../',
  testMatch: ['<rootDir>/tests/unit/**/*.test.js'],
  moduleFileExtensions: ['js', 'cjs', 'mjs'],
  transform: {
    '^.+\\.js$': 'babel-jest',
  },
  transformIgnorePatterns: [],
  verbose: true,
  collectCoverage: false,
  coverageDirectory: '<rootDir>/coverage',
};
