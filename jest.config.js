module.exports = {
  testEnvironment: 'jest-environment-jsdom',
  testMatch: ['**/tests/unit/**/*.test.js'],
  verbose: true,
  globals: {
    TextEncoder: require('util').TextEncoder,
    TextDecoder: require('util').TextDecoder,
  },
};
