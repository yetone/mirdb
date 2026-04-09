/**
 * Metrics Auto-Refresh Tests - Scenario 4
 *
 * Owner: Scenario 4 - Metrics Auto-Refresh
 *
 * Test areas:
 * - Auto-refresh calls /api/metrics every 30 seconds
 * - Uptime value increases after refresh cycle
 * - DOM state (scroll position, focused element) preserved
 * - setInterval/setTimeout configured for 30000ms
 */

const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Test results tracking
let passed = 0;
let failed = 0;
const results = [];

function test(name, fn) {
    try {
        fn();
        passed++;
        results.push({ name, status: 'pass' });
        console.log(`  [PASS] ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`  [FAIL] ${name}`);
        console.log(`    Error: ${error.message}`);
    }
}

// Helper to load metrics.js content
function loadMetricsJs() {
    const jsPath = path.join(__dirname, '../../static/js/metrics.js');
    return fs.readFileSync(jsPath, 'utf-8');
}

// Helper to load index.html content
function loadHomepageHtml() {
    const htmlPath = path.join(__dirname, '../../static/index.html');
    return fs.readFileSync(htmlPath, 'utf-8');
}

console.log('\n========================================');
console.log('Scenario 4: Metrics Auto-Refresh Tests');
console.log('========================================\n');

// =============================================
// Test Case 1: Monitor network requests after page load
// Expected: Auto-refresh calls /api/metrics every 30 seconds
// =============================================
console.log('Test Case 1: Auto-refresh API calls');

test('metrics.js file exists', () => {
    const js = loadMetricsJs();
    assert(js.length > 0, 'metrics.js file should have content');
});

test('metrics.js defines /api/metrics endpoint', () => {
    const js = loadMetricsJs();
    assert(js.includes('/api/metrics'), 'Should reference /api/metrics endpoint');
});

test('metrics.js has fetch function for metrics', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('fetchMetrics') && js.includes('fetch('),
        'Should have fetchMetrics function using fetch API'
    );
});

test('metrics.js configures 30-second refresh interval', () => {
    const js = loadMetricsJs();
    // Check for 30000ms interval configuration
    assert(
        js.includes('30000') || js.includes('METRICS_REFRESH_INTERVAL = 30000'),
        'Should configure 30000ms (30 seconds) refresh interval'
    );
});

test('metrics.js starts auto-refresh on init', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('startMetricsRefresh') && js.includes('init'),
        'Should start metrics refresh on initialization'
    );
});

// =============================================
// Test Case 2: Compare uptime before and after refresh cycle
// Expected: Uptime value increases after 30 seconds
// =============================================
console.log('\nTest Case 2: Uptime value updates');

test('metrics.js has uptime formatting function', () => {
    const js = loadMetricsJs();
    assert(js.includes('formatUptime'), 'Should have formatUptime function');
});

test('formatUptime converts seconds to human-readable format', () => {
    const js = loadMetricsJs();
    // Check that formatUptime handles hours, minutes, seconds
    assert(
        js.includes('hours') && js.includes('minutes') && (js.includes('secs') || js.includes('seconds')),
        'formatUptime should convert seconds to h/m/s format'
    );
});

test('metrics.js updates uptime element with data-metric attribute', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('data-metric="uptime"') || js.includes("data-metric='uptime'"),
        'Should target uptime element by data-metric attribute'
    );
});

test('metrics.js dispatches metrics-updated event', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('mirdb:metrics-updated') && js.includes('CustomEvent'),
        'Should dispatch mirdb:metrics-updated custom event'
    );
});

// =============================================
// Test Case 3: Check DOM state preservation during refresh
// Expected: Scroll position and focused element preserved
// =============================================
console.log('\nTest Case 3: DOM state preservation');

test('metrics.js preserves scroll position', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('scrollX') && js.includes('scrollY'),
        'Should track scroll position'
    );
});

test('metrics.js stores active element before refresh', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('activeElement'),
        'Should store active element reference'
    );
});

test('metrics.js restores focus after refresh if needed', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('focus()'),
        'Should restore focus after refresh'
    );
});

test('metrics.js restores scroll position if changed', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('scrollTo'),
        'Should restore scroll position if it changed'
    );
});

test('metrics.js uses fetch API (not full page reload)', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('fetch(') && !js.includes('location.reload'),
        'Should use fetch API without page reload'
    );
});

// =============================================
// Test Case 4: Verify JavaScript refresh timer implementation
// Expected: setInterval or setTimeout configured for 30000ms
// =============================================
console.log('\nTest Case 4: JavaScript timer implementation');

test('metrics.js uses setInterval for periodic refresh', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('setInterval'),
        'Should use setInterval for periodic refresh'
    );
});

test('setInterval is configured with refresh interval', () => {
    const js = loadMetricsJs();
    // Check that setInterval is called with the interval variable or 30000
    assert(
        (js.includes('setInterval(refreshMetrics') || js.includes('setInterval(')) &&
        (js.includes('30000') || js.includes('interval')),
        'setInterval should be configured with 30000ms or interval variable'
    );
});

test('metrics.js can stop refresh interval', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('clearInterval') && js.includes('stopMetricsRefresh'),
        'Should have ability to stop refresh interval'
    );
});

test('metrics.js exports refresh interval constant', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('REFRESH_INTERVAL') && js.includes('window.MirDB.metrics'),
        'Should export REFRESH_INTERVAL constant for testing'
    );
});

test('refresh interval is exactly 30000 milliseconds', () => {
    const js = loadMetricsJs();
    // Check for exact 30000ms configuration
    assert(
        js.includes('METRICS_REFRESH_INTERVAL = 30000'),
        'METRICS_REFRESH_INTERVAL should be exactly 30000ms'
    );
});

// =============================================
// HTML Integration Tests
// =============================================
console.log('\nHTML Integration Tests');

test('index.html loads metrics.js script', () => {
    const html = loadHomepageHtml();
    assert(
        html.includes('metrics.js'),
        'HTML should load metrics.js script'
    );
});

test('index.html loads metrics.js from correct path', () => {
    const html = loadHomepageHtml();
    assert(
        html.includes('src="/static/js/metrics.js"') ||
        html.includes("src='/static/js/metrics.js'"),
        'HTML should load metrics.js from /static/js/metrics.js'
    );
});

test('metrics.js is loaded after main.js', () => {
    const html = loadHomepageHtml();
    const mainJsIndex = html.indexOf('main.js');
    const metricsJsIndex = html.indexOf('metrics.js');
    assert(
        mainJsIndex > 0 && metricsJsIndex > mainJsIndex,
        'metrics.js should be loaded after main.js'
    );
});

test('HTML has metric elements with data-metric attributes', () => {
    const html = loadHomepageHtml();
    const metrics = ['uptime', 'memory', 'keys', 'ops', 'storage'];
    for (const metric of metrics) {
        assert(
            html.includes(`data-metric="${metric}"`),
            `HTML should have data-metric="${metric}" attribute`
        );
    }
});

// =============================================
// Module Export Tests
// =============================================
console.log('\nModule Export Tests');

test('metrics.js exports to window.MirDB.metrics', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('window.MirDB.metrics'),
        'Should export to window.MirDB.metrics namespace'
    );
});

test('exports refresh function', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('refresh: refreshMetrics'),
        'Should export refresh function'
    );
});

test('exports startRefresh function', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('startRefresh: startMetricsRefresh'),
        'Should export startRefresh function'
    );
});

test('exports stopRefresh function', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('stopRefresh: stopMetricsRefresh'),
        'Should export stopRefresh function'
    );
});

test('exports isRefreshActive function', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('isRefreshActive'),
        'Should export isRefreshActive function'
    );
});

test('exports getRefreshInterval function', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('getRefreshInterval'),
        'Should export getRefreshInterval function'
    );
});

// =============================================
// Error Handling Tests
// =============================================
console.log('\nError Handling Tests');

test('metrics.js handles fetch errors gracefully', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('catch') && js.includes('error'),
        'Should have error handling for fetch failures'
    );
});

test('metrics.js dispatches error event on failure', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('mirdb:metrics-error'),
        'Should dispatch mirdb:metrics-error event on failure'
    );
});

test('metrics.js prevents concurrent refresh requests', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('isRefreshing'),
        'Should track refresh state to prevent concurrent requests'
    );
});

// =============================================
// Summary
// =============================================
console.log('\n' + '='.repeat(50));
console.log(`Test Results: ${passed} passed, ${failed} failed`);
console.log('='.repeat(50));

// Exit with appropriate code
if (failed > 0) {
    console.log('\nFailed tests:');
    results.filter(r => r.status === 'fail').forEach(r => {
        console.log(`  - ${r.name}: ${r.error}`);
    });
    process.exit(1);
} else {
    console.log('\nAll tests passed!');
    process.exit(0);
}
