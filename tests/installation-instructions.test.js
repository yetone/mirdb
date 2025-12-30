/**
 * E2E and Unit Tests for Installation Instructions (REQ-6)
 *
 * These tests verify that the MirDB homepage correctly displays
 * installation instructions and configuration options as specified in REQ-6.
 */

const fs = require('fs');
const path = require('path');

describe('Installation Instructions - REQ-6', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Git Clone Command', () => {
    /**
     * Test Case ID: 1
     * Input: Check for git clone command
     * Expected: Installation section contains 'git clone' command for MirDB repository
     */
    test('should have a quickstart or installation section', () => {
      const quickstartSection = document.getElementById('quickstart');
      expect(quickstartSection).toBeTruthy();
    });

    test('should contain git clone command in the installation section', () => {
      const quickstartSection = document.getElementById('quickstart');
      const textContent = quickstartSection.textContent.toLowerCase();

      expect(textContent).toContain('git clone');
    });

    test('should clone the MirDB repository URL', () => {
      const quickstartSection = document.getElementById('quickstart');
      const htmlText = quickstartSection.innerHTML.toLowerCase();

      // Should reference the mirdb repository
      expect(htmlText).toMatch(/git\s+clone.*mirdb/i);
    });

    test('should include cd command to enter the project directory', () => {
      const quickstartSection = document.getElementById('quickstart');
      const textContent = quickstartSection.textContent;

      expect(textContent).toMatch(/cd\s+mirdb/i);
    });
  });

  describe('Test Case 2: Cargo Build Command', () => {
    /**
     * Test Case ID: 2
     * Input: Check for cargo build command
     * Expected: Installation section contains 'cargo build --release' or similar
     */
    test('should contain cargo build command', () => {
      const quickstartSection = document.getElementById('quickstart');
      const textContent = quickstartSection.textContent.toLowerCase();

      expect(textContent).toContain('cargo build');
    });

    test('should use release build flag for production', () => {
      const quickstartSection = document.getElementById('quickstart');
      const textContent = quickstartSection.textContent;

      expect(textContent).toMatch(/cargo\s+build\s+--release/i);
    });
  });

  describe('Test Case 3: Run Command', () => {
    /**
     * Test Case ID: 3
     * Input: Check for run command
     * Expected: Installation section shows how to start MirDB with configuration file
     */
    test('should show how to run MirDB server', () => {
      const quickstartSection = document.getElementById('quickstart');
      const textContent = quickstartSection.textContent;

      // Should contain a reference to running the mirdb server binary
      expect(textContent).toMatch(/\.\/target\/release\/mirdb|mirdb-server/i);
    });

    test('should show configuration file usage', () => {
      const quickstartSection = document.getElementById('quickstart');
      const textContent = quickstartSection.textContent;

      // Should show -c flag with config file path
      expect(textContent).toMatch(/-c\s+.*\.toml/i);
    });

    test('should reference the etc/mirdb.toml config file', () => {
      const quickstartSection = document.getElementById('quickstart');
      const textContent = quickstartSection.textContent;

      expect(textContent).toContain('mirdb.toml');
    });
  });

  describe('Test Case 4: Code Block Formatting', () => {
    /**
     * Test Case ID: 4
     * Input: Verify commands are in code blocks
     * Expected: All shell commands are properly formatted in code blocks with bash highlighting
     */
    test('should have code blocks for installation commands', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeBlocks = quickstartSection.querySelectorAll('.code-block, pre, code');

      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('code blocks should contain the git clone command', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeBlocks = quickstartSection.querySelectorAll('.code-block');

      let foundGitClone = false;
      codeBlocks.forEach((block) => {
        if (block.textContent.includes('git clone')) {
          foundGitClone = true;
        }
      });

      expect(foundGitClone).toBe(true);
    });

    test('code blocks should contain cargo build command', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeBlocks = quickstartSection.querySelectorAll('.code-block');

      let foundCargoBuild = false;
      codeBlocks.forEach((block) => {
        if (block.textContent.includes('cargo build')) {
          foundCargoBuild = true;
        }
      });

      expect(foundCargoBuild).toBe(true);
    });

    test('code blocks should contain run command', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeBlocks = quickstartSection.querySelectorAll('.code-block');

      let foundRunCommand = false;
      codeBlocks.forEach((block) => {
        if (block.textContent.match(/\.\/target\/release\/mirdb|mirdb-server/i)) {
          foundRunCommand = true;
        }
      });

      expect(foundRunCommand).toBe(true);
    });

    test('code blocks should use monospace font styling', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeBlocks = quickstartSection.querySelectorAll('.code-block, pre');

      expect(codeBlocks.length).toBeGreaterThan(0);

      // Verify there's a CSS class for code styling
      const hasCodeBlockClass = quickstartSection.querySelector('.code-block') !== null;
      const hasPreElement = quickstartSection.querySelector('pre') !== null;

      expect(hasCodeBlockClass || hasPreElement).toBe(true);
    });

    test('code comments should be properly styled', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeComments = quickstartSection.querySelectorAll('.code-comment');

      // Should have comments explaining each step
      expect(codeComments.length).toBeGreaterThan(0);
    });

    test('comments should explain clone, build, and run steps', () => {
      const quickstartSection = document.getElementById('quickstart');
      const codeComments = quickstartSection.querySelectorAll('.code-comment');

      const commentTexts = Array.from(codeComments).map(c => c.textContent.toLowerCase());
      const combinedComments = commentTexts.join(' ');

      // Check for explanatory comments
      const hasCloneComment = combinedComments.includes('clone');
      const hasBuildComment = combinedComments.includes('build');
      const hasRunComment = combinedComments.includes('run') || combinedComments.includes('configuration');

      expect(hasCloneComment).toBe(true);
      expect(hasBuildComment).toBe(true);
      expect(hasRunComment).toBe(true);
    });
  });

  describe('Installation Section Structure', () => {
    test('should have an Installation heading or subheading', () => {
      const quickstartSection = document.getElementById('quickstart');
      const textContent = quickstartSection.textContent.toLowerCase();

      expect(textContent).toContain('installation');
    });

    test('should have Quick Start section title', () => {
      const quickstartSection = document.getElementById('quickstart');
      const sectionTitle = quickstartSection.querySelector('.section-title, h2');

      expect(sectionTitle).toBeTruthy();
      expect(sectionTitle.textContent.toLowerCase()).toContain('quick start');
    });

    test('should have section subtitle explaining the purpose', () => {
      const quickstartSection = document.getElementById('quickstart');
      const subtitle = quickstartSection.querySelector('.section-subtitle, p');

      expect(subtitle).toBeTruthy();
      expect(subtitle.textContent.length).toBeGreaterThan(10);
    });
  });

  describe('Navigation Links', () => {
    test('navigation should have a link to quickstart section', () => {
      const navLinks = document.querySelectorAll('.nav-links a');

      let hasQuickstartLink = false;
      navLinks.forEach((link) => {
        const href = link.getAttribute('href');
        if (href === '#quickstart') {
          hasQuickstartLink = true;
        }
      });

      expect(hasQuickstartLink).toBe(true);
    });
  });
});
