/**
 * Test Configuration
 *
 * Shared configuration for unit and E2E tests.
 */

const path = require('path');

const config = {
    // Paths
    homepagePath: path.resolve(__dirname, '../../index.html'),
    stylesPath: path.resolve(__dirname, '../../styles/main.css'),
    scriptsPath: path.resolve(__dirname, '../../scripts/main.js'),

    // Server configuration
    port: process.env.PORT || 3000,
    baseUrl: process.env.BASE_URL || 'http://localhost:3000',

    // Viewport configurations
    viewports: {
        mobile: { width: 375, height: 667 },
        tablet: { width: 768, height: 1024 },
        desktop: { width: 1280, height: 800 }
    },

    // Timeout configurations
    timeouts: {
        short: 5000,
        medium: 10000,
        long: 30000
    }
};

module.exports = config;
