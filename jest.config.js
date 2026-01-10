module.exports = {
  testEnvironment: 'jest-environment-jsdom',
  testMatch: ['**/tests/**/*.test.js'],
  verbose: true,
  collectCoverageFrom: ['*.html'],
  moduleFileExtensions: ['js', 'json', 'html'],
  setupFiles: ['./tests/setup.js'],
};
