module.exports = {
  testEnvironment: 'jsdom',
  setupFilesAfterEnv: ['<rootDir>/tests/setup.cjs'],
  testMatch: ['<rootDir>/tests/**/*.test.cjs'],
  moduleFileExtensions: ['js', 'cjs', 'json'],
  verbose: true,
  collectCoverageFrom: ['src/**/*.js']
};
