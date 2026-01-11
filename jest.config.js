module.exports = {
  testEnvironment: 'jsdom',
  testMatch: ['<rootDir>/tests/**/*.test.js'],
  testPathIgnorePatterns: ['/node_modules/', '/homepage/'],
  modulePathIgnorePatterns: ['/homepage/'],
  verbose: true,
  setupFiles: ['<rootDir>/jest.setup.js']
};
