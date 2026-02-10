module.exports = {
  testEnvironment: 'jsdom',
  testMatch: ['**/tests/unit/**/*.test.js', '**/tests/integration/**/*.test.js'],
  verbose: true,
  setupFiles: ['<rootDir>/tests/setup.js'],
};
