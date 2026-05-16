import { defineConfig, devices } from "@playwright/test";

/**
 * Playwright configuration for the MirDB homepage cross-browser suite.
 *
 * Three browser projects validate the PRD's NFR-2 contract that the homepage
 * renders identically on Chrome / Edge (chromium), Safari (webkit), and
 * Firefox in their two most recent versions:
 *   - chromium  -> Chrome + Edge (Blink engine)
 *   - webkit    -> Safari        (WebKit engine)
 *   - firefox   -> Firefox       (Gecko engine)
 *
 * The webServer block boots a static HTTP server so that ES module imports
 * inside the fixture page resolve correctly relative to the homepage root.
 */
export default defineConfig({
    testDir: "./tests/cross-browser",
    testMatch: ["**/*.test.js"],
    fullyParallel: false,
    forbidOnly: !!process.env.CI,
    retries: 0,
    workers: 1,
    reporter: [["list"]],
    timeout: 60_000,
    expect: {
        toHaveScreenshot: {
            maxDiffPixelRatio: 0.02,
            animations: "disabled",
        },
    },
    use: {
        baseURL: "http://127.0.0.1:5173",
        trace: "off",
        video: "off",
        screenshot: "off",
    },
    projects: [
        {
            name: "chromium",
            use: { ...devices["Desktop Chrome"] },
        },
        {
            name: "webkit",
            use: { ...devices["Desktop Safari"] },
        },
        {
            name: "firefox",
            use: { ...devices["Desktop Firefox"] },
        },
    ],
    webServer: {
        command: "npx http-server . -p 5173 -c-1 --silent",
        url: "http://127.0.0.1:5173/tests/cross-browser/fixture.html",
        reuseExistingServer: !process.env.CI,
        timeout: 30_000,
        stdout: "ignore",
        stderr: "pipe",
    },
});
