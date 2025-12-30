/**
 * Tests for Getting Started section of MirDB product homepage
 * Verifies REQ-3: Include a "Getting Started" section with installation and basic usage instructions
 */

const fs = require('fs');
const path = require('path');

describe('Getting Started Section', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Getting Started section exists
  test('TC1: Getting Started section exists', () => {
    const gettingStartedById = document.getElementById('getting-started');
    const headings = document.querySelectorAll('h1, h2, h3');
    let gettingStartedHeading = null;
    headings.forEach(h => {
      if (h.textContent.toLowerCase().includes('getting started')) {
        gettingStartedHeading = h;
      }
    });

    const sectionExists = gettingStartedById !== null || gettingStartedHeading !== null;
    expect(sectionExists).toBe(true);
  });

  // Test Case 2: Installation command is present
  test('TC2: Installation command is present', () => {
    const codeBlocks = document.querySelectorAll('pre, code');
    const installationPatterns = [
      /cargo\s+(run|build|install)/i,
      /git\s+clone/i,
      /mirdb/i,
      /\.\/target/i,
      /-c\s+.*\.toml/i
    ];

    let hasInstallationCommand = false;
    codeBlocks.forEach(block => {
      const text = block.textContent;
      if (installationPatterns.some(pattern => pattern.test(text))) {
        hasInstallationCommand = true;
      }
    });

    expect(hasInstallationCommand).toBe(true);
  });

  // Test Case 3: Basic usage example is present
  test('TC3: Basic usage example is present', () => {
    const codeBlocks = document.querySelectorAll('pre, code');
    const usagePatterns = [
      /\bset\s+\w+/i,
      /\bget\s+\w+/i,
      /telnet/i,
      /localhost.*12333/i,
      /nc\s+localhost/i
    ];

    let hasUsageExample = false;
    codeBlocks.forEach(block => {
      const text = block.textContent;
      if (usagePatterns.some(pattern => pattern.test(text))) {
        hasUsageExample = true;
      }
    });

    expect(hasUsageExample).toBe(true);
  });

  // Test Case 4: Configuration snippet is present
  test('TC4: Configuration snippet is present', () => {
    const codeBlocks = document.querySelectorAll('pre, code');
    const configPatterns = [
      /addr\s*=\s*["'].*["']/,
      /work_dir\s*=/,
      /max_level\s*=/,
      /\.toml/i,
      /sst_max_size\s*=/,
      /mem_table_max_size\s*=/
    ];

    let hasConfigSnippet = false;
    codeBlocks.forEach(block => {
      const text = block.textContent;
      if (configPatterns.some(pattern => pattern.test(text))) {
        hasConfigSnippet = true;
      }
    });

    expect(hasConfigSnippet).toBe(true);
  });

  // Test Case 5: Getting Started section is easily discoverable
  test('TC5: Getting Started section is easily discoverable', () => {
    // Check for navigation link to Getting Started section
    const navLinks = document.querySelectorAll('nav a, header a, [role="navigation"] a, .hero-actions a');
    let hasNavLink = false;
    navLinks.forEach(link => {
      const linkText = link.textContent.toLowerCase();
      const href = link.getAttribute('href') || '';
      if (linkText.includes('getting started') || linkText.includes('get started') || href.includes('getting-started')) {
        hasNavLink = true;
      }
    });

    // Also check the section exists with a clear heading
    const gettingStartedSection = document.getElementById('getting-started');

    const isDiscoverable = hasNavLink || gettingStartedSection !== null;
    expect(isDiscoverable).toBe(true);
  });
});
