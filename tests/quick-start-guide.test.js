/**
 * Quick Start Guide Tests
 *
 * These tests verify the quick-start guide provides clear installation
 * and usage instructions on the MirDB landing page.
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Quick Start Guide', () => {
  let htmlContent;

  beforeAll(() => {
    // Load the landing page HTML
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document.documentElement.innerHTML = htmlContent;
  });

  // Test Case 1: Page contains quick-start, getting-started, or installation section
  test('TC1: Page contains quick-start, getting-started, or installation section', () => {
    // Check for quick-start section by ID, class, or heading text
    const quickStartById = document.querySelector('#quick-start');
    const quickStartByClass = document.querySelector('.quick-start');
    const gettingStartedById = document.querySelector('#getting-started');
    const installationById = document.querySelector('#installation');

    // Check for section headings containing relevant text
    const allHeadings = document.querySelectorAll('h1, h2, h3, h4');
    let hasQuickStartHeading = false;

    allHeadings.forEach(heading => {
      const text = heading.textContent.toLowerCase();
      if (text.includes('quick start') ||
          text.includes('getting started') ||
          text.includes('installation') ||
          text.includes('get started')) {
        hasQuickStartHeading = true;
      }
    });

    const hasQuickStartSection =
      quickStartById !== null ||
      quickStartByClass !== null ||
      gettingStartedById !== null ||
      installationById !== null ||
      hasQuickStartHeading;

    expect(hasQuickStartSection).toBe(true);
  });

  // Test Case 2: Quick-start section contains 'cargo build' or 'cargo run' command
  test('TC2: Quick-start section contains cargo build or cargo run command', () => {
    // Find the quick-start section
    const quickStartSection = document.querySelector('#quick-start') ||
                              document.querySelector('.quick-start') ||
                              document.querySelector('#getting-started') ||
                              document.querySelector('#installation');

    expect(quickStartSection).not.toBeNull();

    const sectionText = quickStartSection.textContent;

    // Check for cargo commands
    const hasCargoBuild = sectionText.includes('cargo build');
    const hasCargoRun = sectionText.includes('cargo run');

    expect(hasCargoBuild || hasCargoRun).toBe(true);
  });

  // Test Case 3: Code examples use <pre><code> or similar markup with syntax highlighting
  test('TC3: Code examples use pre/code markup with syntax highlighting', () => {
    // Find the quick-start section
    const quickStartSection = document.querySelector('#quick-start') ||
                              document.querySelector('.quick-start') ||
                              document.querySelector('#getting-started') ||
                              document.querySelector('#installation');

    expect(quickStartSection).not.toBeNull();

    // Check for code blocks with various patterns
    const codeBlocks = quickStartSection.querySelectorAll('pre, code, .code-block, [class*="code"]');
    expect(codeBlocks.length).toBeGreaterThan(0);

    // Check that at least one code block contains actual code/commands
    let hasCodeContent = false;
    codeBlocks.forEach(block => {
      const text = block.textContent;
      // Code blocks should contain commands or code-like content
      if (text.includes('cargo') ||
          text.includes('git') ||
          text.includes('set ') ||
          text.includes('get ') ||
          text.includes('telnet') ||
          text.includes('./')) {
        hasCodeContent = true;
      }
    });

    expect(hasCodeContent).toBe(true);
  });

  // Test Case 4: Instructions reference port 12333 (default MirDB port)
  test('TC4: Instructions reference port 12333 (default MirDB port)', () => {
    // Find the quick-start section
    const quickStartSection = document.querySelector('#quick-start') ||
                              document.querySelector('.quick-start') ||
                              document.querySelector('#getting-started') ||
                              document.querySelector('#installation');

    expect(quickStartSection).not.toBeNull();

    const sectionText = quickStartSection.textContent;

    // Check for port 12333 reference
    expect(sectionText).toContain('12333');
  });

  // Additional test: Verify code blocks have styling/formatting
  test('TC5: Code blocks have proper styling for readability', () => {
    // Find the quick-start section
    const quickStartSection = document.querySelector('#quick-start') ||
                              document.querySelector('.quick-start');

    expect(quickStartSection).not.toBeNull();

    // Check for styled code blocks
    const styledCodeBlocks = quickStartSection.querySelectorAll('.code-block, pre, [class*="highlight"]');
    expect(styledCodeBlocks.length).toBeGreaterThan(0);

    // Verify the HTML contains proper code markup
    const sectionHTML = quickStartSection.innerHTML;
    const hasCodeMarkup = sectionHTML.includes('<code') ||
                          sectionHTML.includes('class="code') ||
                          sectionHTML.includes('class=\'code');

    expect(hasCodeMarkup).toBe(true);
  });
});
