/**
 * Jest Configuration
 */
module.exports = {
  testEnvironment: 'jsdom',
  testMatch: [
    '**/tests/unit/**/*.test.js',
    '**/tests/integration/**/*.test.js'
  ],
  moduleFileExtensions: ['js', 'json'],
  collectCoverageFrom: [
    'js/**/*.js',
    '!js/main.js'
  ],
  coverageDirectory: 'coverage',
  verbose: true
};
