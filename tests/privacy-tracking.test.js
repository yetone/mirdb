/**
 * Privacy - No External Tracking Tests (Unit Tests)
 * Verifies the homepage does not include external tracking or analytics scripts (NFR-5)
 *
 * Test Cases:
 * 1. No Google Analytics or gtag scripts are included
 * 2. No Facebook Pixel or similar tracking scripts are included
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load the HTML file
const htmlPath = path.join(__dirname, '..', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf8');

// Parse with JSDOM
const dom = new JSDOM(html);
const document = dom.window.document;

// Test results tracking
let passed = 0;
let failed = 0;
const results = [];

function test(name, fn) {
    try {
        fn();
        passed++;
        results.push({ name, status: 'pass' });
        console.log(`\x1b[32m✓\x1b[0m ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`\x1b[31m✗\x1b[0m ${name}`);
        console.log(`  Error: ${error.message}`);
    }
}

function assert(condition, message) {
    if (!condition) {
        throw new Error(message);
    }
}

console.log('\n=== Privacy - No External Tracking Tests (Unit) ===\n');

// Known tracking/analytics script patterns
const GOOGLE_ANALYTICS_PATTERNS = [
    'google-analytics.com',
    'googletagmanager.com',
    'gtag(',
    'ga(',
    'GoogleAnalyticsObject',
    'www.google-analytics.com/analytics.js',
    'www.googletagmanager.com/gtag',
    '_gaq',
    'UA-',
    'G-',
    'gtag.js'
];

const FACEBOOK_TRACKING_PATTERNS = [
    'facebook.net',
    'facebook.com/tr',
    'fbq(',
    'connect.facebook.net',
    'fbevents.js',
    'Facebook Pixel',
    'fb_pixel',
    'fbq.queue'
];

const OTHER_TRACKING_PATTERNS = [
    'mixpanel.com',
    'hotjar.com',
    'segment.com',
    'analytics.twitter.com',
    'twitter.com/i/adsct',
    'linkedin.com/insight',
    'snap.licdn.com',
    'adroll.com',
    'doubleclick.net',
    'amplitude.com',
    'heap-analytics.com',
    'fullstory.com',
    'logrocket.com',
    'mouseflow.com',
    'clarity.ms',
    'plausible.io',
    'stats.wp.com'
];

// Test Case 1: No Google Analytics or gtag scripts are included
test('TC1: No Google Analytics or gtag scripts are included', () => {
    // Check all script elements
    const scripts = document.querySelectorAll('script');
    const scriptContents = [];

    scripts.forEach(script => {
        const src = script.getAttribute('src') || '';
        const content = script.textContent || '';
        scriptContents.push({ src, content });
    });

    // Check the raw HTML for any Google Analytics patterns
    for (const pattern of GOOGLE_ANALYTICS_PATTERNS) {
        const foundInHtml = html.includes(pattern);
        assert(!foundInHtml, `Google Analytics pattern "${pattern}" should not be present in HTML`);

        // Also check script src attributes and inline content
        for (const script of scriptContents) {
            const foundInSrc = script.src.includes(pattern);
            const foundInContent = script.content.includes(pattern);
            assert(!foundInSrc, `Google Analytics pattern "${pattern}" found in script src`);
            assert(!foundInContent, `Google Analytics pattern "${pattern}" found in inline script`);
        }
    }

    // Verify no script tags with Google Analytics domains
    const gaScripts = document.querySelectorAll('script[src*="google-analytics"], script[src*="googletagmanager"], script[src*="gtag"]');
    assert(gaScripts.length === 0, 'No Google Analytics script tags should exist');
});

// Test Case 2: No Facebook Pixel or similar tracking scripts are included
test('TC2: No Facebook Pixel or similar tracking scripts are included', () => {
    // Check all script elements
    const scripts = document.querySelectorAll('script');
    const scriptContents = [];

    scripts.forEach(script => {
        const src = script.getAttribute('src') || '';
        const content = script.textContent || '';
        scriptContents.push({ src, content });
    });

    // Check the raw HTML for any Facebook tracking patterns
    for (const pattern of FACEBOOK_TRACKING_PATTERNS) {
        const foundInHtml = html.includes(pattern);
        assert(!foundInHtml, `Facebook tracking pattern "${pattern}" should not be present in HTML`);

        // Also check script src attributes and inline content
        for (const script of scriptContents) {
            const foundInSrc = script.src.includes(pattern);
            const foundInContent = script.content.includes(pattern);
            assert(!foundInSrc, `Facebook tracking pattern "${pattern}" found in script src`);
            assert(!foundInContent, `Facebook tracking pattern "${pattern}" found in inline script`);
        }
    }

    // Verify no script tags with Facebook tracking domains
    const fbScripts = document.querySelectorAll('script[src*="facebook.net"], script[src*="facebook.com/tr"], script[src*="fbevents"]');
    assert(fbScripts.length === 0, 'No Facebook Pixel script tags should exist');
});

// Additional test: No other common tracking scripts
test('TC-Additional: No other common tracking/analytics scripts are included', () => {
    // Check the raw HTML for any other tracking patterns
    for (const pattern of OTHER_TRACKING_PATTERNS) {
        const foundInHtml = html.includes(pattern);
        assert(!foundInHtml, `Tracking pattern "${pattern}" should not be present in HTML`);
    }
});

// Test: No tracking-related meta tags or data attributes
test('TC-Meta: No tracking-related meta tags or data attributes', () => {
    // Check for Facebook app ID meta tag
    const fbAppId = document.querySelector('meta[property="fb:app_id"]');
    assert(!fbAppId, 'No Facebook App ID meta tag should be present');

    // Check for Google site verification (often used with analytics)
    const googleVerification = document.querySelector('meta[name="google-site-verification"]');
    assert(!googleVerification, 'No Google site verification meta tag should be present');

    // Check for tracking pixels disguised as images
    const pixelImages = document.querySelectorAll('img[src*="facebook.com/tr"], img[src*="google-analytics.com"], img[src*="doubleclick.net"]');
    assert(pixelImages.length === 0, 'No tracking pixel images should be present');
});

// Test: No noscript fallback trackers
test('TC-NoScript: No noscript fallback trackers', () => {
    const noscriptElements = document.querySelectorAll('noscript');

    noscriptElements.forEach((noscript, index) => {
        const content = noscript.innerHTML;

        // Check for common noscript tracking patterns
        for (const pattern of [...GOOGLE_ANALYTICS_PATTERNS, ...FACEBOOK_TRACKING_PATTERNS]) {
            const found = content.includes(pattern);
            assert(!found, `Tracking pattern "${pattern}" found in noscript element ${index}`);
        }
    });
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
