/**
 * Supported Commands Section Tests
 * Tests for verifying the supported memcached commands section displays
 * commands with examples (REQ-7)
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

// Load the HTML file
const htmlPath = path.join(__dirname, '..', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf8');

// Parse with JSDOM
const dom = new JSDOM(html, {
    resources: 'usable'
});
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

console.log('\n=== Supported Commands Section Tests ===\n');

// Test Case 1: Check for commands section presence
// Type: unit
// Expected: A section dedicated to supported commands exists on the page
test('TC1: Commands section presence - A section dedicated to supported commands exists', () => {
    // Look for the commands section
    const commandsSection = document.querySelector('.commands');
    assert(commandsSection, 'Commands section should exist');

    // Verify it has a heading that mentions "Commands"
    const heading = commandsSection.querySelector('h2');
    assert(heading, 'Commands section should have an h2 heading');

    const headingText = heading.textContent.toLowerCase();
    assert(
        headingText.includes('command') || headingText.includes('supported'),
        'Heading should mention "command" or "supported"'
    );
});

// Test Case 2: Verify 'get' command is listed
// Type: unit
// Expected: The 'get' memcached command is mentioned in the supported commands
test('TC2: GET command listed - The "get" memcached command is mentioned', () => {
    const commandsSection = document.querySelector('.commands');
    assert(commandsSection, 'Commands section should exist');

    const commandsText = commandsSection.textContent.toLowerCase();
    const hasGetCommand = commandsText.includes('get');
    assert(hasGetCommand, 'Commands section should mention the GET command');

    // Also verify it's in a command item or code block
    const commandItems = commandsSection.querySelectorAll('.command-item');
    const codeBlocks = commandsSection.querySelectorAll('code');

    let getFoundInProperFormat = false;

    commandItems.forEach(item => {
        if (item.textContent.toLowerCase().includes('get')) {
            getFoundInProperFormat = true;
        }
    });

    codeBlocks.forEach(block => {
        if (block.textContent.toLowerCase().includes('get')) {
            getFoundInProperFormat = true;
        }
    });

    assert(getFoundInProperFormat, 'GET command should be displayed in a command item or code block');
});

// Test Case 3: Verify 'set' command is listed
// Type: unit
// Expected: The 'set' memcached command is mentioned in the supported commands
test('TC3: SET command listed - The "set" memcached command is mentioned', () => {
    const commandsSection = document.querySelector('.commands');
    assert(commandsSection, 'Commands section should exist');

    const commandsText = commandsSection.textContent.toLowerCase();
    const hasSetCommand = commandsText.includes('set');
    assert(hasSetCommand, 'Commands section should mention the SET command');

    // Also verify it's in a command item or code block
    const commandItems = commandsSection.querySelectorAll('.command-item');
    const codeBlocks = commandsSection.querySelectorAll('code');

    let setFoundInProperFormat = false;

    commandItems.forEach(item => {
        if (item.textContent.toLowerCase().includes('set')) {
            setFoundInProperFormat = true;
        }
    });

    codeBlocks.forEach(block => {
        if (block.textContent.toLowerCase().includes('set')) {
            setFoundInProperFormat = true;
        }
    });

    assert(setFoundInProperFormat, 'SET command should be displayed in a command item or code block');
});

// Summary
console.log(`\n=== Test Summary ===`);
console.log(`Passed: ${passed}`);
console.log(`Failed: ${failed}`);
console.log(`Total: ${passed + failed}`);

// Export results for potential integration
module.exports = { results, passed, failed };

// Exit with appropriate code
process.exit(failed > 0 ? 1 : 0);
