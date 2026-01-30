/**
 * Jest Configuration
 * Owner: First builder
 */
module.exports = {
  testEnvironment: 'jsdom',
  testMatch: [
    '**/tests/unit/**/*.test.js',
    '**/tests/integration/**/*.test.js'
  ],
  moduleNameMapper: {},
  setupFilesAfterEnv: [],
  testPathIgnorePatterns: ['/node_modules/', '/tests/e2e/'],
  verbose: true,
  collectCoverageFrom: [
    'js/**/*.js',
    '!js/vendor/**'
  ]
};
