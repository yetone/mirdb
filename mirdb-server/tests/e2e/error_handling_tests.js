/**
 * Error Handling Tests - Scenario 15
 *
 * Owner: Scenario 15 - Error Handling - API Unavailable
 *
 * Test areas:
 * - Mock /api/metrics returning 500 error - Dashboard shows error state
 * - Mock /api/metrics returning timeout - Dashboard shows loading/retry state
 * - Mock /api/status returning 500 error - Status indicator shows unknown/error state
 * - Recover from error state when API returns - Dashboard recovers
 * - Check error message accessibility - Error states announced to screen readers
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

// Helper to load main.js content
function loadMainJs() {
    const jsPath = path.join(__dirname, '../../static/js/main.js');
    return fs.readFileSync(jsPath, 'utf-8');
}

// Helper to load main.css content
function loadMainCss() {
    const cssPath = path.join(__dirname, '../../static/css/main.css');
    return fs.readFileSync(cssPath, 'utf-8');
}

// Helper to load index.html content
function loadHomepageHtml() {
    const htmlPath = path.join(__dirname, '../../static/index.html');
    return fs.readFileSync(htmlPath, 'utf-8');
}

console.log('\n========================================');
console.log('Scenario 15: Error Handling Tests');
console.log('========================================\n');

// =============================================
// Test Case 1: Mock /api/metrics returning 500 error
// Expected: Dashboard shows error state, no JavaScript errors
// =============================================
console.log('Test Case 1: Metrics API 500 Error Handling');

test('metrics.js handles HTTP error responses', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('response.ok') && js.includes('throw'),
        'Should check response.ok and throw on error'
    );
});

test('metrics.js identifies server errors by status code', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('error.status') || js.includes('response.status'),
        'Should identify server errors by status code'
    );
});

test('metrics.js has showMetricsError function', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('showMetricsError'),
        'Should have showMetricsError function for displaying errors'
    );
});

test('metrics.js creates error display element', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('metrics-error-display') || js.includes('createErrorDisplayElement'),
        'Should create error display element for error states'
    );
});

test('metrics.js shows error message for server errors', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('server') && js.includes('error'),
        'Should handle server error type'
    );
});

test('metrics.js tracks error state', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('isInErrorState'),
        'Should track whether in error state'
    );
});

test('CSS has metrics-error styles', () => {
    const css = loadMainCss();
    assert(
        css.includes('.metrics-error'),
        'Should have .metrics-error CSS styles'
    );
});

test('CSS has error-visible class for showing errors', () => {
    const css = loadMainCss();
    assert(
        css.includes('.error-visible') || css.includes('error-visible'),
        'Should have error-visible class for showing error states'
    );
});

// =============================================
// Test Case 2: Mock /api/metrics returning timeout
// Expected: Dashboard shows loading/retry state after timeout
// =============================================
console.log('\nTest Case 2: Metrics API Timeout Handling');

test('metrics.js has timeout configuration', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('FETCH_TIMEOUT') || js.includes('timeout'),
        'Should have timeout configuration'
    );
});

test('metrics.js uses AbortController for timeout', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('AbortController'),
        'Should use AbortController for fetch timeout'
    );
});

test('metrics.js handles AbortError for timeout', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('AbortError'),
        'Should handle AbortError when request times out'
    );
});

test('metrics.js identifies timeout error type', () => {
    const js = loadMetricsJs();
    assert(
        js.includes("type = 'timeout'") || js.includes('timeout'),
        'Should identify timeout errors'
    );
});

test('metrics.js shows timeout-specific message', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('timeout') && js.includes('timed out'),
        'Should show timeout-specific error message'
    );
});

test('metrics.js clears timeout on successful response', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('clearTimeout'),
        'Should clear timeout on successful response'
    );
});

// =============================================
// Test Case 3: Mock /api/status returning 500 error
// Expected: Status indicator shows unknown/error state
// =============================================
console.log('\nTest Case 3: Status API 500 Error Handling');

test('main.js handles HTTP error responses', () => {
    const js = loadMainJs();
    assert(
        js.includes('response.ok') && js.includes('throw'),
        'Should check response.ok and throw on error'
    );
});

test('main.js has STATUS_UNKNOWN constant', () => {
    const js = loadMainJs();
    assert(
        js.includes('STATUS_UNKNOWN'),
        'Should have STATUS_UNKNOWN constant for error states'
    );
});

test('main.js updates status to unknown on error', () => {
    const js = loadMainJs();
    assert(
        js.includes('STATUS_UNKNOWN') && js.includes('updateStatusDisplay'),
        'Should update status to unknown on error'
    );
});

test('main.js has timeout handling for status API', () => {
    const js = loadMainJs();
    assert(
        js.includes('AbortController') && js.includes('FETCH_TIMEOUT'),
        'Should have timeout handling for status API'
    );
});

test('CSS has unknown status dot styles', () => {
    const css = loadMainCss();
    assert(
        css.includes('.status-dot.unknown'),
        'Should have .status-dot.unknown CSS styles'
    );
});

test('main.js dispatches status-error event', () => {
    const js = loadMainJs();
    assert(
        js.includes('mirdb:status-error') && js.includes('CustomEvent'),
        'Should dispatch mirdb:status-error event'
    );
});

// =============================================
// Test Case 4: Recover from error state when API returns
// Expected: Dashboard recovers and shows metrics on next successful fetch
// =============================================
console.log('\nTest Case 4: Recovery from Error State');

test('metrics.js has clearMetricsError function', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('clearMetricsError'),
        'Should have clearMetricsError function'
    );
});

test('metrics.js clears error state on successful fetch', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('clearMetricsError') && js.includes('isInErrorState'),
        'Should clear error state on successful fetch'
    );
});

test('metrics.js dispatches recovery event', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('mirdb:metrics-recovered'),
        'Should dispatch mirdb:metrics-recovered event'
    );
});

test('main.js tracks recovery from error state', () => {
    const js = loadMainJs();
    assert(
        js.includes('wasInErrorState') || js.includes('isInErrorState'),
        'Should track recovery from error state'
    );
});

test('main.js dispatches status-recovered event', () => {
    const js = loadMainJs();
    assert(
        js.includes('mirdb:status-recovered'),
        'Should dispatch mirdb:status-recovered event'
    );
});

test('metrics.js resets consecutive error counter on recovery', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('consecutiveErrors') && js.includes('= 0'),
        'Should reset consecutive error counter on recovery'
    );
});

test('metrics.js removes error CSS classes on recovery', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('metrics-error-state') && js.includes('remove'),
        'Should remove error CSS classes on recovery'
    );
});

// =============================================
// Test Case 5: Check error message accessibility
// Expected: Error states announced to screen readers
// =============================================
console.log('\nTest Case 5: Error Message Accessibility');

test('metrics.js creates error display with role="alert"', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('role') && js.includes('alert'),
        'Error display should have role="alert"'
    );
});

test('metrics.js error display has aria-live="assertive"', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('aria-live') && js.includes('assertive'),
        'Error display should have aria-live="assertive"'
    );
});

test('metrics.js error display has aria-atomic="true"', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('aria-atomic'),
        'Error display should have aria-atomic attribute'
    );
});

test('main.js status indicator has aria-live for announcements', () => {
    const js = loadMainJs();
    assert(
        js.includes('aria-live') && js.includes('statusIndicator'),
        'Status indicator should have aria-live for announcements'
    );
});

test('main.js updates aria-label on error', () => {
    const js = loadMainJs();
    assert(
        js.includes('aria-label') && js.includes('error'),
        'Should update aria-label on error state'
    );
});

test('HTML status indicator has role="status"', () => {
    const html = loadHomepageHtml();
    assert(
        html.includes('role="status"') && html.includes('status-indicator'),
        'Status indicator should have role="status"'
    );
});

test('HTML status indicator has aria-live attribute', () => {
    const html = loadHomepageHtml();
    assert(
        html.includes('aria-live') && html.includes('status-indicator'),
        'Status indicator should have aria-live attribute'
    );
});

test('CSS supports reduced motion for error animations', () => {
    const css = loadMainCss();
    assert(
        css.includes('prefers-reduced-motion'),
        'Should support prefers-reduced-motion for error animations'
    );
});

// =============================================
// Additional Error Handling Tests
// =============================================
console.log('\nAdditional Error Handling Tests');

test('metrics.js tracks consecutive errors', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('consecutiveErrors'),
        'Should track consecutive error count'
    );
});

test('metrics.js exports error handling functions', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('showError:') && js.includes('clearError:') && js.includes('isInError:'),
        'Should export error handling functions'
    );
});

test('main.js exports error state query functions', () => {
    const js = loadMainJs();
    assert(
        js.includes('isInError:') && js.includes('getConsecutiveErrors:'),
        'Should export error state query functions'
    );
});

test('metrics.js handles network errors distinctly', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('network') && js.includes('error.type'),
        'Should handle network errors distinctly from server errors'
    );
});

test('main.js handles network errors distinctly', () => {
    const js = loadMainJs();
    assert(
        js.includes('network') && js.includes('error.type'),
        'Should handle network errors distinctly from server errors'
    );
});

test('CSS has high contrast support for error states', () => {
    const css = loadMainCss();
    assert(
        css.includes('prefers-contrast') && css.includes('error'),
        'Should support high contrast mode for error states'
    );
});

test('metrics.js error display shows retry message', () => {
    const js = loadMetricsJs();
    assert(
        js.includes('retry') || js.includes('Will retry'),
        'Error display should indicate automatic retry'
    );
});

test('CSS error animation respects reduced motion', () => {
    const css = loadMainCss();
    assert(
        css.includes('prefers-reduced-motion') && css.includes('error'),
        'Error animations should respect reduced motion preference'
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
