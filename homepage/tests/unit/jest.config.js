/**
 * Jest Configuration
 * Owner: First scenario builder
 *
 * Configuration:
 * - Test environment (jsdom)
 * - Module resolution
 * - Coverage settings
 */

module.exports = {
    testEnvironment: 'jsdom',
    roots: ['<rootDir>'],
    testMatch: ['**/*.test.js'],
    moduleFileExtensions: ['js', 'json'],
    collectCoverageFrom: [
        '../../src/js/**/*.js',
        '!**/node_modules/**',
    ],
    coverageDirectory: '<rootDir>/coverage',
    verbose: true,
};
