const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
    testDir: './tests',
    testMatch: '*.e2e.spec.js',
    timeout: 30000,
    use: {
        browserName: 'chromium',
        headless: true
    },
    reporter: 'list'
});
