/**
 * Jest Configuration for Unit Tests
 */

module.exports = {
    testEnvironment: 'jsdom',
    testMatch: ['**/tests/unit/**/*.test.js'],
    moduleFileExtensions: ['js'],
    verbose: true,
    testTimeout: 10000,
    setupFilesAfterEnv: [],
    collectCoverageFrom: [
        'scripts/**/*.js',
        '!**/node_modules/**'
    ],
    transformIgnorePatterns: [
        'node_modules/(?!(@exodus/bytes)/)'
    ],
    moduleNameMapper: {
        '^@exodus/bytes$': '<rootDir>/tests/setup/__mocks__/exodus-bytes.js'
    }
};
