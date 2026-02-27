/** @type {import('jest').Config} */
module.exports = {
    testEnvironment: 'jsdom',
    setupFilesAfterEnv: ['<rootDir>/tests/setup.js'],
    testMatch: [
        '<rootDir>/tests/unit/**/*.test.js',
        '<rootDir>/tests/integration/**/*.test.js',
    ],
    moduleFileExtensions: ['js', 'json'],
    collectCoverageFrom: ['js/**/*.js'],
    coverageDirectory: 'coverage',
    verbose: true,
};
