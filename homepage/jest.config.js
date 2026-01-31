/**
 * Jest Configuration for MirDB Homepage Unit/Integration Tests
 *
 * Configures:
 * - jsdom environment for DOM testing
 * - Coverage thresholds
 * - Test file patterns
 */

module.exports = {
  testEnvironment: 'jsdom',
  testMatch: [
    '**/tests/unit/**/*.test.js',
    '**/tests/integration/**/*.test.js',
  ],
  collectCoverageFrom: [
    'js/**/*.js',
    '!js/**/*.min.js',
  ],
  coverageThreshold: {
    global: {
      branches: 80,
      functions: 80,
      lines: 80,
      statements: 80,
    },
  },
  setupFiles: ['<rootDir>/tests/setup.js'],
};
