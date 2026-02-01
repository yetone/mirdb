/**
 * Jest configuration for MirDB Landing Page tests.
 * Owner: First Builder
 *
 * Configuration for:
 * - JSDOM environment for DOM testing
 * - Test file patterns
 * - Setup files
 */
module.exports = {
  testEnvironment: 'jsdom',
  setupFilesAfterEnv: ['<rootDir>/tests/setup.js'],
  testMatch: [
    '<rootDir>/tests/**/*.test.js'
  ],
  moduleFileExtensions: ['js', 'json'],
  collectCoverageFrom: [
    'assets/js/**/*.js'
  ],
  testPathIgnorePatterns: [
    '/node_modules/'
  ]
};
