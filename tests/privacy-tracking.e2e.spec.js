/**
 * Privacy - No External Tracking E2E Tests
 * Verifies the homepage does not make tracking network requests or set third-party cookies (NFR-5)
 *
 * Test Cases:
 * 3. No requests to known tracking domains on page load
 * 4. Page does not set third-party tracking cookies
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

// Known tracking domains to block/detect
const TRACKING_DOMAINS = [
    // Google Analytics / Tag Manager
    'google-analytics.com',
    'googletagmanager.com',
    'www.google-analytics.com',
    'ssl.google-analytics.com',
    'analytics.google.com',
    'stats.g.doubleclick.net',

    // Facebook
    'connect.facebook.net',
    'www.facebook.com/tr',
    'pixel.facebook.com',

    // Other common trackers
    'mixpanel.com',
    'api.mixpanel.com',
    'cdn.mxpnl.com',
    'hotjar.com',
    'static.hotjar.com',
    'script.hotjar.com',
    'segment.com',
    'cdn.segment.com',
    'api.segment.io',
    'analytics.twitter.com',
    't.co/i/adsct',
    'ads-twitter.com',
    'snap.licdn.com',
    'dc.ads.linkedin.com',
    'd.adroll.com',
    's.adroll.com',
    'doubleclick.net',
    'ad.doubleclick.net',
    'amplitude.com',
    'api.amplitude.com',
    'cdn.amplitude.com',
    'heapanalytics.com',
    'cdn.heapanalytics.com',
    'fullstory.com',
    'rs.fullstory.com',
    'logrocket.com',
    'cdn.logrocket.io',
    'mouseflow.com',
    'clarity.ms',
    'plausible.io',
    'stats.wp.com',
    'pixel.wp.com',
    'bat.bing.com',
    'tr.snapchat.com',
    'sc-static.net',
    'tiktokcdn.com',
    'analytics.tiktok.com'
];

// Known tracking cookie prefixes/names
const TRACKING_COOKIE_PATTERNS = [
    '_ga',         // Google Analytics
    '_gid',        // Google Analytics
    '_gat',        // Google Analytics
    '__utma',      // Google Analytics (legacy)
    '__utmb',      // Google Analytics (legacy)
    '__utmc',      // Google Analytics (legacy)
    '__utmz',      // Google Analytics (legacy)
    '_fbp',        // Facebook Pixel
    '_fbc',        // Facebook Click ID
    'fr',          // Facebook
    '_gcl_au',     // Google Ads
    '_hjid',       // Hotjar
    '_hjSession',  // Hotjar
    'mp_',         // Mixpanel
    'ajs_',        // Segment
    '_mkto_',      // Marketo
    '_dc_gtm_',    // Google Tag Manager
    '_gac_',       // Google Ads
    'IDE',         // DoubleClick
    'DSID',        // DoubleClick
    'NID',         // Google
    '_clck',       // Microsoft Clarity
    '_clsk'        // Microsoft Clarity
];

test.describe('Privacy - No External Tracking E2E Tests (NFR-5)', () => {
    const indexPath = path.resolve(__dirname, '../index.html');
    const pageUrl = `file://${indexPath}`;

    test('TC3: No requests to known tracking domains on page load', async ({ page }) => {
        // Track all network requests
        const trackingRequests = [];
        const allRequests = [];

        page.on('request', (request) => {
            const url = request.url();
            allRequests.push(url);

            // Check if request goes to a tracking domain
            for (const domain of TRACKING_DOMAINS) {
                if (url.includes(domain)) {
                    trackingRequests.push({
                        url: url,
                        domain: domain,
                        resourceType: request.resourceType()
                    });
                }
            }
        });

        // Navigate to the page and wait for network to settle
        await page.goto(pageUrl, { waitUntil: 'networkidle' });

        // Wait additional time for any delayed tracking calls
        await page.waitForTimeout(2000);

        // Log all requests for debugging
        console.log(`Total requests made: ${allRequests.length}`);
        if (trackingRequests.length > 0) {
            console.log('Tracking requests detected:');
            trackingRequests.forEach(req => {
                console.log(`  - ${req.url} (${req.resourceType})`);
            });
        }

        // Assert no tracking requests were made
        expect(trackingRequests, 'No requests should be made to known tracking domains').toEqual([]);
    });

    test('TC4: Page does not set third-party tracking cookies', async ({ page, context }) => {
        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'networkidle' });

        // Wait for any async cookie setting
        await page.waitForTimeout(1000);

        // Get all cookies set by the page
        const cookies = await context.cookies();

        // Check for tracking cookies
        const trackingCookies = [];

        for (const cookie of cookies) {
            const cookieName = cookie.name;

            for (const pattern of TRACKING_COOKIE_PATTERNS) {
                if (cookieName.startsWith(pattern) || cookieName.includes(pattern)) {
                    trackingCookies.push({
                        name: cookie.name,
                        domain: cookie.domain,
                        value: cookie.value.substring(0, 20) + '...',
                        pattern: pattern
                    });
                }
            }
        }

        // Log cookie information for debugging
        console.log(`Total cookies set: ${cookies.length}`);
        if (cookies.length > 0) {
            console.log('Cookies:');
            cookies.forEach(c => {
                console.log(`  - ${c.name} (domain: ${c.domain})`);
            });
        }

        if (trackingCookies.length > 0) {
            console.log('Tracking cookies detected:');
            trackingCookies.forEach(tc => {
                console.log(`  - ${tc.name} (matches pattern: ${tc.pattern})`);
            });
        }

        // Assert no tracking cookies were set
        expect(trackingCookies, 'No tracking cookies should be set').toEqual([]);
    });

    test('TC-Storage: No tracking data in localStorage or sessionStorage', async ({ page }) => {
        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'networkidle' });

        // Wait for any async storage operations
        await page.waitForTimeout(1000);

        // Check localStorage for tracking-related items
        const localStorageTracking = await page.evaluate(() => {
            const trackingPatterns = [
                '_ga', '_gid', 'ga:', 'analytics', 'mixpanel',
                'segment', 'fbp', 'hotjar', 'amplitude', 'heap'
            ];
            const suspicious = [];

            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                for (const pattern of trackingPatterns) {
                    if (key && key.toLowerCase().includes(pattern.toLowerCase())) {
                        suspicious.push({
                            key: key,
                            pattern: pattern,
                            storage: 'localStorage'
                        });
                    }
                }
            }

            return suspicious;
        });

        // Check sessionStorage for tracking-related items
        const sessionStorageTracking = await page.evaluate(() => {
            const trackingPatterns = [
                '_ga', '_gid', 'ga:', 'analytics', 'mixpanel',
                'segment', 'fbp', 'hotjar', 'amplitude', 'heap'
            ];
            const suspicious = [];

            for (let i = 0; i < sessionStorage.length; i++) {
                const key = sessionStorage.key(i);
                for (const pattern of trackingPatterns) {
                    if (key && key.toLowerCase().includes(pattern.toLowerCase())) {
                        suspicious.push({
                            key: key,
                            pattern: pattern,
                            storage: 'sessionStorage'
                        });
                    }
                }
            }

            return suspicious;
        });

        const allTracking = [...localStorageTracking, ...sessionStorageTracking];

        if (allTracking.length > 0) {
            console.log('Tracking data in storage detected:');
            allTracking.forEach(item => {
                console.log(`  - ${item.key} in ${item.storage} (matches: ${item.pattern})`);
            });
        }

        // Assert no tracking data in storage
        expect(allTracking, 'No tracking data should be stored in localStorage or sessionStorage').toEqual([]);
    });

    test('TC-Beacon: No tracking beacons or sendBeacon calls', async ({ page }) => {
        // Intercept sendBeacon calls
        const beaconCalls = [];

        await page.addInitScript(() => {
            const originalSendBeacon = navigator.sendBeacon;
            navigator.sendBeacon = function(url, data) {
                window.__beaconCalls = window.__beaconCalls || [];
                window.__beaconCalls.push({ url, timestamp: Date.now() });
                return originalSendBeacon.apply(navigator, arguments);
            };
        });

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'networkidle' });

        // Wait for any beacon calls
        await page.waitForTimeout(2000);

        // Get beacon calls
        const calls = await page.evaluate(() => window.__beaconCalls || []);

        // Check if any beacon calls go to tracking domains
        const trackingBeacons = calls.filter(call => {
            return TRACKING_DOMAINS.some(domain => call.url.includes(domain));
        });

        if (trackingBeacons.length > 0) {
            console.log('Tracking beacon calls detected:');
            trackingBeacons.forEach(b => {
                console.log(`  - ${b.url}`);
            });
        }

        // Assert no tracking beacons
        expect(trackingBeacons, 'No sendBeacon calls should be made to tracking domains').toEqual([]);
    });

    test('TC-CSP: Verify no inline tracking scripts execute', async ({ page }) => {
        // Track script execution
        const executedScripts = [];

        page.on('console', (msg) => {
            // Some tracking scripts log their initialization
            const text = msg.text();
            const trackingIndicators = [
                'google analytics',
                'ga.js',
                'gtag',
                'fbq',
                'facebook pixel',
                'mixpanel',
                'segment',
                'hotjar'
            ];

            for (const indicator of trackingIndicators) {
                if (text.toLowerCase().includes(indicator)) {
                    executedScripts.push({
                        message: text,
                        indicator: indicator
                    });
                }
            }
        });

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'networkidle' });

        // Wait for scripts to execute
        await page.waitForTimeout(1000);

        // Check for global tracking objects
        const trackingGlobals = await page.evaluate(() => {
            const globals = [];

            // Google Analytics globals
            if (window.ga) globals.push('window.ga');
            if (window.gtag) globals.push('window.gtag');
            if (window.GoogleAnalyticsObject) globals.push('window.GoogleAnalyticsObject');
            if (window._gaq) globals.push('window._gaq');
            if (window.dataLayer) globals.push('window.dataLayer');

            // Facebook Pixel globals
            if (window.fbq) globals.push('window.fbq');
            if (window._fbq) globals.push('window._fbq');

            // Other tracking globals
            if (window.mixpanel) globals.push('window.mixpanel');
            if (window.analytics) globals.push('window.analytics');
            if (window.hj) globals.push('window.hj');
            if (window.amplitude) globals.push('window.amplitude');
            if (window.heap) globals.push('window.heap');

            return globals;
        });

        if (executedScripts.length > 0) {
            console.log('Tracking script execution detected:');
            executedScripts.forEach(s => {
                console.log(`  - ${s.message}`);
            });
        }

        if (trackingGlobals.length > 0) {
            console.log('Tracking global variables detected:');
            trackingGlobals.forEach(g => {
                console.log(`  - ${g}`);
            });
        }

        // Assert no tracking globals
        expect(trackingGlobals, 'No tracking global variables should be defined').toEqual([]);
        expect(executedScripts, 'No tracking script initialization messages should appear').toEqual([]);
    });
});
