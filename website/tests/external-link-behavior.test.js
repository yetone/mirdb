/**
 * External Link Behavior Unit Tests
 *
 * Scenario: Verify external links open in new tabs and preserve user's place
 *
 * Test Case 3: Verify rel='noopener noreferrer' on external links
 * - External links have rel='noopener noreferrer' for security
 *
 * This uses jsdom to test the HTML structure without a browser.
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');
const assert = require('assert');

// Load the HTML file
const htmlPath = path.resolve(__dirname, '../dist/index.html');
const html = fs.readFileSync(htmlPath, 'utf8');

// Parse with JSDOM
const dom = new JSDOM(html);
const document = dom.window.document;

// Test counter
let passCount = 0;
let failCount = 0;

// Test helper function
function test(name, fn) {
  try {
    fn();
    console.log(`✓ ${name}`);
    passCount++;
  } catch (error) {
    console.log(`✗ ${name}`);
    console.log(`  Error: ${error.message}`);
    failCount++;
  }
}

// Expect helper function
function expect(value) {
  return {
    toBeTruthy() {
      if (!value) {
        throw new Error(`Expected value to be truthy, got: ${value}`);
      }
    },
    toContain(substring) {
      if (!value || !value.includes(substring)) {
        throw new Error(`Expected "${value}" to contain "${substring}"`);
      }
    },
    toBe(expected) {
      if (value !== expected) {
        throw new Error(`Expected "${value}" to be "${expected}"`);
      }
    },
    toBeGreaterThan(num) {
      if (value <= num) {
        throw new Error(`Expected ${value} to be greater than ${num}`);
      }
    }
  };
}

console.log('\n=== External Link Behavior Unit Tests ===\n');

// Test Case 3: Verify rel='noopener noreferrer' on external links
console.log('--- Test Case 3: Security Attributes ---\n');

test('All external links have target="_blank"', () => {
  const externalLinks = document.querySelectorAll('a[href^="https://github.com"]');
  expect(externalLinks.length).toBeGreaterThan(0);

  externalLinks.forEach(link => {
    const target = link.getAttribute('target');
    if (target !== '_blank') {
      throw new Error(`Link to ${link.href} should have target="_blank", got: ${target}`);
    }
  });
});

test('All external links have rel="noopener"', () => {
  const externalLinks = document.querySelectorAll('a[target="_blank"]');
  expect(externalLinks.length).toBeGreaterThan(0);

  externalLinks.forEach(link => {
    const rel = link.getAttribute('rel');
    if (!rel || !rel.includes('noopener')) {
      throw new Error(`Link to ${link.href} missing rel="noopener", got: ${rel}`);
    }
  });
});

test('All external links have rel="noreferrer"', () => {
  const externalLinks = document.querySelectorAll('a[target="_blank"]');
  expect(externalLinks.length).toBeGreaterThan(0);

  externalLinks.forEach(link => {
    const rel = link.getAttribute('rel');
    if (!rel || !rel.includes('noreferrer')) {
      throw new Error(`Link to ${link.href} missing rel="noreferrer", got: ${rel}`);
    }
  });
});

test('Hero GitHub button has correct security attributes', () => {
  const githubBtn = document.querySelector('[data-testid="cta-github"]');
  expect(githubBtn).toBeTruthy();
  expect(githubBtn.getAttribute('target')).toBe('_blank');
  expect(githubBtn.getAttribute('rel')).toContain('noopener');
  expect(githubBtn.getAttribute('rel')).toContain('noreferrer');
});

test('Footer GitHub link has correct security attributes', () => {
  const footerGithub = document.querySelector('[data-testid="footer-github-link"]');
  expect(footerGithub).toBeTruthy();
  expect(footerGithub.getAttribute('target')).toBe('_blank');
  expect(footerGithub.getAttribute('rel')).toContain('noopener');
  expect(footerGithub.getAttribute('rel')).toContain('noreferrer');
});

test('Footer documentation link has correct security attributes', () => {
  const footerDocs = document.querySelector('[data-testid="footer-docs-link"]');
  expect(footerDocs).toBeTruthy();
  expect(footerDocs.getAttribute('target')).toBe('_blank');
  expect(footerDocs.getAttribute('rel')).toContain('noopener');
  expect(footerDocs.getAttribute('rel')).toContain('noreferrer');
});

test('Architecture docs link has correct security attributes', () => {
  const architectureDocs = document.querySelector('[data-testid="architecture-docs-link"]');
  expect(architectureDocs).toBeTruthy();
  expect(architectureDocs.getAttribute('target')).toBe('_blank');
  expect(architectureDocs.getAttribute('rel')).toContain('noopener');
  expect(architectureDocs.getAttribute('rel')).toContain('noreferrer');
});

test('Configuration docs link has correct security attributes', () => {
  const configDocs = document.querySelector('[data-testid="config-docs-link"]');
  expect(configDocs).toBeTruthy();
  expect(configDocs.getAttribute('target')).toBe('_blank');
  expect(configDocs.getAttribute('rel')).toContain('noopener');
  expect(configDocs.getAttribute('rel')).toContain('noreferrer');
});

// Test Case 1 & 2: Verify external links structure (unit test portion)
console.log('\n--- Test Cases 1 & 2: Link Structure ---\n');

test('GitHub repository link exists and points to correct URL', () => {
  const githubBtn = document.querySelector('[data-testid="cta-github"]');
  expect(githubBtn).toBeTruthy();
  expect(githubBtn.getAttribute('href')).toBe('https://github.com/pjzhong/mirdb');
});

test('Documentation links exist throughout the page', () => {
  const docLinks = [
    document.querySelector('[data-testid="footer-docs-link"]'),
    document.querySelector('[data-testid="architecture-docs-link"]'),
    document.querySelector('[data-testid="config-docs-link"]')
  ];

  docLinks.forEach((link, index) => {
    if (!link) {
      throw new Error(`Documentation link ${index + 1} not found`);
    }
    const href = link.getAttribute('href');
    if (!href || !href.includes('github.com')) {
      throw new Error(`Documentation link ${index + 1} should point to github.com, got: ${href}`);
    }
  });
});

test('External links count matches expected', () => {
  const externalLinks = document.querySelectorAll('a[target="_blank"]');
  // We expect at least 5 external links:
  // - Hero GitHub button
  // - Footer GitHub link
  // - Footer docs link
  // - Architecture docs link
  // - Configuration docs link
  expect(externalLinks.length).toBeGreaterThan(4);
});

// Summary
console.log('\n=== Test Summary ===');
console.log(`Passed: ${passCount}`);
console.log(`Failed: ${failCount}`);
console.log(`Total:  ${passCount + failCount}`);

// Exit with proper code
process.exit(failCount > 0 ? 1 : 0);
