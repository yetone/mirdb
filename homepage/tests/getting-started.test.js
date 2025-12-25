/**
 * E2E-style tests for the Getting Started section of MirDB homepage
 * Using JSDOM for DOM testing in Node.js environment
 *
 * Scenario: Verify installation instructions and basic usage examples are provided
 */

import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import assert from 'assert';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Simple test framework
let passCount = 0;
let failCount = 0;
const errors = [];

function describe(name, fn) {
  console.log('\n' + name);
  fn();
}

function it(name, fn) {
  try {
    fn();
    console.log('  ✓ ' + name);
    passCount++;
  } catch (error) {
    console.log('  ✗ ' + name);
    console.log('    Error: ' + error.message);
    errors.push({ name, error: error.message });
    failCount++;
  }
}

// Load the built HTML (Astro builds to dist/)
const htmlPath = path.join(__dirname, '..', 'dist', 'index.html');
if (!fs.existsSync(htmlPath)) {
  console.error('Error: dist/index.html not found. Run "npm run build" first.');
  process.exit(1);
}

const htmlContent = fs.readFileSync(htmlPath, 'utf8');
const dom = new JSDOM(htmlContent);
const document = dom.window.document;
const gettingStartedSection = document.getElementById('getting-started');

// Run tests
describe('Getting Started Section', function() {
  /**
   * Test Case 1: Check Getting Started section for installation instructions
   * Expected: Step-by-step installation instructions are displayed in a code block
   */
  it('should display step-by-step installation instructions in a code block', function() {
    // Verify Getting Started section exists
    assert.ok(gettingStartedSection, 'Getting Started section should exist');

    // Verify installation instructions exist in a code block
    const codeBlocks = gettingStartedSection.querySelectorAll('pre code');
    assert.ok(codeBlocks.length > 0, 'Should have code blocks in Getting Started section');

    // Check for the installation code block specifically
    const installationCode = gettingStartedSection.querySelector('code.installation');
    assert.ok(installationCode, 'Should have installation code block');

    // Get the section text content
    const sectionText = gettingStartedSection.textContent;

    // Verify step-by-step installation instructions are present
    assert.ok(/clone/i.test(sectionText), 'Should mention cloning repository');
    assert.ok(/cargo\s+build/i.test(sectionText), 'Should mention cargo build');
    assert.ok(/cargo\s+run/i.test(sectionText), 'Should mention cargo run');

    // Verify installation instructions are in a <pre> block
    const preBlocks = gettingStartedSection.querySelectorAll('pre');
    assert.ok(preBlocks.length > 0, 'Should have pre blocks for code');
  });

  /**
   * Test Case 2: Check for basic configuration example
   * Expected: A basic configuration example showing TOML format is provided
   */
  it('should display a basic configuration example in TOML format', function() {
    // Verify Getting Started section exists
    assert.ok(gettingStartedSection, 'Getting Started section should exist');

    // Look for configuration code block with TOML content
    const configCodeBlock = gettingStartedSection.querySelector('code.config, code.toml');
    assert.ok(configCodeBlock, 'Should have a configuration code block');

    // Get the section text content
    const sectionText = gettingStartedSection.textContent;

    // Verify TOML configuration elements are present
    assert.ok(/addr\s*=/.test(sectionText), 'Should have addr configuration');
    assert.ok(/work_dir\s*=/.test(sectionText), 'Should have work_dir configuration');
    assert.ok(/0\.0\.0\.0:12333|12333/.test(sectionText), 'Should show the default port');

    // Verify TOML format markers (key = "value" patterns)
    assert.ok(/\w+\s*=\s*["'][^"']+["']/.test(sectionText), 'Should have TOML key-value pairs');
  });

  /**
   * Test Case 3: Check for connection instructions
   * Expected: Instructions for connecting to MirDB using a memcached client are provided
   */
  it('should display instructions for connecting to MirDB using a memcached client', function() {
    // Verify Getting Started section exists
    assert.ok(gettingStartedSection, 'Getting Started section should exist');

    // Get the section text content
    const sectionText = gettingStartedSection.textContent;

    // Verify connection instructions mention memcached client
    assert.ok(/memcached/i.test(sectionText), 'Should mention memcached');
    assert.ok(/client/i.test(sectionText), 'Should mention client');
    assert.ok(/connect|telnet|nc\s|netcat/i.test(sectionText), 'Should mention connection method');

    // Verify connection examples with port
    assert.ok(/12333/.test(sectionText), 'Should show port 12333');
    assert.ok(/localhost|127\.0\.0\.1/.test(sectionText), 'Should show localhost or IP');

    // Verify code examples for basic operations are present (SET, GET, DELETE)
    assert.ok(/\bset\s+\w+/i.test(sectionText), 'Should show SET operation');
    assert.ok(/\bget\s+\w+/i.test(sectionText), 'Should show GET operation');
    assert.ok(/\bdelete\s+\w+/i.test(sectionText), 'Should show DELETE operation');

    // Verify there's at least one code block with connection/usage example
    const codeBlocks = gettingStartedSection.querySelectorAll('pre');
    assert.ok(codeBlocks.length > 0, 'Should have code blocks for examples');
  });
});

// Summary
console.log('\n----------------------------------------');
console.log(`Test Results: ${passCount} passed, ${failCount} failed`);

if (failCount > 0) {
  console.log('\nFailed tests:');
  errors.forEach(({ name, error }) => {
    console.log(`  - ${name}: ${error}`);
  });
  process.exit(1);
} else {
  console.log('\nAll tests passed!');
  process.exit(0);
}
