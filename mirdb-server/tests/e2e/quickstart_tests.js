/**
 * Quick Start Section E2E Tests
 *
 * Owner: Scenario 5 - Quick Start Guide Display
 *
 * Test cases:
 * 1. SET command example with key, flags, exptime, bytes format
 * 2. GET command example with key parameter
 * 3. DELETE command example with key parameter
 * 4. Endpoint information display (host and port)
 * 5. Code blocks have appropriate styling for readability
 */

const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Helper to load HTML content
function loadHomepageHtml() {
    const htmlPath = path.join(__dirname, '../../static/index.html');
    return fs.readFileSync(htmlPath, 'utf-8');
}

// Helper to load CSS content
function loadMainCss() {
    const cssPath = path.join(__dirname, '../../static/css/main.css');
    return fs.readFileSync(cssPath, 'utf-8');
}

// Helper to check if HTML contains text
function containsText(html, text) {
    return html.includes(text);
}

// Helper to check if HTML contains element with ID
function hasElementWithId(html, id) {
    return html.includes(`id="${id}"`);
}

// Test results tracking
let passed = 0;
let failed = 0;
const results = [];

function test(name, fn) {
    try {
        fn();
        passed++;
        results.push({ name, status: 'pass' });
        console.log(`  \u2713 ${name}`);
    } catch (error) {
        failed++;
        results.push({ name, status: 'fail', error: error.message });
        console.log(`  \u2717 ${name}`);
        console.log(`    Error: ${error.message}`);
    }
}

console.log('Quick Start Section E2E Tests\n');
console.log('='.repeat(50));

// =============================================
// Test Case 1: SET Command Example
// Section contains SET command with key, flags, exptime, bytes format
// =============================================
console.log('\nTest Case 1: SET Command Example');

test('quickstart section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'quickstart'), 'Should have quickstart section');
});

test('SET example section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'set-example'), 'Should have SET example section');
});

test('SET command is displayed', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'set') && containsText(html, 'mykey'),
           'Should display SET command with key');
});

test('SET command shows key parameter', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '&lt;key&gt;') || containsText(html, '<key>'),
           'Should show key parameter in SET syntax');
});

test('SET command shows flags parameter', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '&lt;flags&gt;') || containsText(html, 'flags'),
           'Should show flags parameter in SET syntax');
});

test('SET command shows exptime parameter', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '&lt;exptime&gt;') || containsText(html, 'exptime'),
           'Should show exptime parameter in SET syntax');
});

test('SET command shows bytes parameter', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '&lt;bytes&gt;') || containsText(html, 'bytes'),
           'Should show bytes parameter in SET syntax');
});

test('SET example shows STORED response', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'STORED'), 'Should show STORED response');
});

// =============================================
// Test Case 2: GET Command Example
// Section contains GET command with key parameter
// =============================================
console.log('\nTest Case 2: GET Command Example');

test('GET example section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'get-example'), 'Should have GET example section');
});

test('GET command is displayed', () => {
    const html = loadHomepageHtml();
    // Check for the GET command pattern
    const getCommandPattern = /get.*mykey/i;
    assert(getCommandPattern.test(html), 'Should display GET command with key');
});

test('GET command shows key parameter syntax', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'get &lt;key&gt;') || containsText(html, 'get <key>'),
           'Should show key parameter in GET syntax');
});

test('GET example shows VALUE response', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'VALUE'), 'Should show VALUE response');
});

test('GET example shows END response', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'END'), 'Should show END response');
});

// =============================================
// Test Case 3: DELETE Command Example
// Section contains DELETE command with key parameter
// =============================================
console.log('\nTest Case 3: DELETE Command Example');

test('DELETE example section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'delete-example'), 'Should have DELETE example section');
});

test('DELETE command is displayed', () => {
    const html = loadHomepageHtml();
    // Check for the DELETE command pattern
    const deleteCommandPattern = /delete.*mykey/i;
    assert(deleteCommandPattern.test(html), 'Should display DELETE command with key');
});

test('DELETE command shows key parameter syntax', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'delete &lt;key&gt;') || containsText(html, 'delete <key>'),
           'Should show key parameter in DELETE syntax');
});

test('DELETE example shows DELETED response', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'DELETED'), 'Should show DELETED response');
});

// =============================================
// Test Case 4: Endpoint Information Display
// Displays Memcached protocol endpoint host and port
// =============================================
console.log('\nTest Case 4: Endpoint Information Display');

test('quickstart endpoint element exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'quickstart-endpoint'), 'Should have endpoint element in quickstart');
});

test('endpoint shows host 0.0.0.0', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '0.0.0.0'), 'Should display host 0.0.0.0');
});

test('endpoint shows port 12333', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '12333'), 'Should display port 12333');
});

test('endpoint shows full address 0.0.0.0:12333', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, '0.0.0.0:12333'), 'Should display full endpoint 0.0.0.0:12333');
});

test('connection example section exists', () => {
    const html = loadHomepageHtml();
    assert(hasElementWithId(html, 'connection-example'), 'Should have connection example section');
});

test('connection example shows telnet command', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'telnet'), 'Should show telnet connection method');
});

test('connection example shows netcat command', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'nc'), 'Should show netcat connection method');
});

// =============================================
// Test Case 5: Code Examples Syntax Highlighting
// Code blocks have appropriate styling for readability
// =============================================
console.log('\nTest Case 5: Code Block Styling');

test('code-block class is used', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'class="code-block"'), 'Should use code-block class');
});

test('code-comment class is used for comments', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'class="code-comment"'), 'Should use code-comment class for syntax highlighting');
});

test('code-command class is used for commands', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'class="code-command"'), 'Should use code-command class for syntax highlighting');
});

test('code-key class is used for keys', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'class="code-key"'), 'Should use code-key class for syntax highlighting');
});

test('code-param class is used for parameters', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'class="code-param"'), 'Should use code-param class for syntax highlighting');
});

test('code-value class is used for values', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'class="code-value"'), 'Should use code-value class for syntax highlighting');
});

test('code-response class is used for responses', () => {
    const html = loadHomepageHtml();
    assert(containsText(html, 'class="code-response"'), 'Should use code-response class for syntax highlighting');
});

// CSS styling tests
test('CSS defines code-block styles', () => {
    const css = loadMainCss();
    assert(css.includes('.code-block'), 'CSS should have code-block styles');
});

test('CSS defines code-comment styles', () => {
    const css = loadMainCss();
    assert(css.includes('.code-comment'), 'CSS should have code-comment styles');
});

test('CSS defines code-command styles', () => {
    const css = loadMainCss();
    assert(css.includes('.code-command'), 'CSS should have code-command styles');
});

test('CSS defines code-key styles', () => {
    const css = loadMainCss();
    assert(css.includes('.code-key'), 'CSS should have code-key styles');
});

test('CSS defines code-param styles', () => {
    const css = loadMainCss();
    assert(css.includes('.code-param'), 'CSS should have code-param styles');
});

test('CSS defines code-value styles', () => {
    const css = loadMainCss();
    assert(css.includes('.code-value'), 'CSS should have code-value styles');
});

test('CSS defines code-response styles', () => {
    const css = loadMainCss();
    assert(css.includes('.code-response'), 'CSS should have code-response styles');
});

test('code-block has background color for contrast', () => {
    const css = loadMainCss();
    assert(css.includes('.code-block') && css.includes('background-color'),
           'code-block should have background color');
});

test('code-block uses monospace font', () => {
    const css = loadMainCss();
    assert(css.includes('font-family-mono') || css.includes('monospace'),
           'code-block should use monospace font');
});

test('code examples have appropriate padding', () => {
    const css = loadMainCss();
    assert(css.includes('.code-block') && css.includes('padding'),
           'code-block should have padding for readability');
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
