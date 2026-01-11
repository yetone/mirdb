/**
 * Tests for Quick Start Installation Section
 *
 * This test suite verifies that the Quick Start section provides clear installation commands:
 * - Git clone command for the repository
 * - Cargo build command for building the project
 * - Server run command for starting mirdb-server
 * - All commands displayed in proper code blocks
 */

describe('Quick Start Installation Section', () => {
  let document;
  let quickstartSection;

  beforeEach(() => {
    // Load the actual HTML from the homepage
    const html = global.getIndexHTML();
    global.loadHTML(global.extractBodyContent(html));

    // Get the quick start section
    quickstartSection = window.document.getElementById('quick-start') ||
                        window.document.getElementById('quickstart') ||
                        window.document.querySelector('.quickstart-section') ||
                        window.document.querySelector('.quickstart') ||
                        window.document.querySelector('[data-testid="quickstart-section"]');
  });

  // Test Case 1: Check for git clone command
  describe('Test Case 1: Git Clone Command', () => {
    test('should contain git clone https://github.com/yetone/mirdb.git command', () => {
      expect(quickstartSection).toBeTruthy();

      // Get all text content from the quick start section
      const sectionContent = quickstartSection.textContent;

      // Check for the git clone command
      expect(sectionContent).toContain('git clone');
      expect(sectionContent).toContain('https://github.com/yetone/mirdb.git');

      // Verify the full command is present
      const hasFullCloneCommand = sectionContent.includes('git clone https://github.com/yetone/mirdb.git');
      expect(hasFullCloneCommand).toBe(true);
    });

    test('git clone command should reference the correct GitHub repository', () => {
      const sectionContent = quickstartSection.textContent;

      // Verify repository URL is correct
      const repoPattern = /git clone\s+https:\/\/github\.com\/yetone\/mirdb\.git/;
      expect(repoPattern.test(sectionContent)).toBe(true);
    });
  });

  // Test Case 2: Check for cargo build command
  describe('Test Case 2: Cargo Build Command', () => {
    test('should contain cargo build --release or cargo build command', () => {
      expect(quickstartSection).toBeTruthy();

      const sectionContent = quickstartSection.textContent;

      // Check for cargo build command
      expect(sectionContent).toContain('cargo build');

      // The command should ideally include --release flag for production build
      const hasReleaseFlag = sectionContent.includes('cargo build --release');
      const hasBasicBuild = sectionContent.includes('cargo build');

      // Either cargo build --release OR cargo build should be present
      expect(hasReleaseFlag || hasBasicBuild).toBe(true);
    });

    test('cargo build command should include --release flag for production build', () => {
      const sectionContent = quickstartSection.textContent;

      // Verify --release flag is present for optimized build
      expect(sectionContent).toContain('cargo build --release');
    });
  });

  // Test Case 3: Check for server run command
  describe('Test Case 3: Server Run Command', () => {
    test('should contain server execution command showing ./target/release/mirdb-server or similar', () => {
      expect(quickstartSection).toBeTruthy();

      const sectionContent = quickstartSection.textContent;

      // Check for server run command
      const hasReleaseServerPath = sectionContent.includes('./target/release/mirdb-server');
      const hasDebugServerPath = sectionContent.includes('./target/debug/mirdb-server');
      const hasCargoRun = sectionContent.includes('cargo run');
      const hasMirdbServer = sectionContent.includes('mirdb-server');

      // Should have some form of server execution command
      expect(hasReleaseServerPath || hasDebugServerPath || hasCargoRun || hasMirdbServer).toBe(true);
    });

    test('server run command should show the release binary path', () => {
      const sectionContent = quickstartSection.textContent;

      // Verify the release binary path is shown
      expect(sectionContent).toContain('./target/release/mirdb-server');
    });
  });

  // Test Case 4: Verify commands are in code blocks
  describe('Test Case 4: Commands in Code Blocks', () => {
    test('all commands should be displayed in pre/code elements with proper formatting', () => {
      expect(quickstartSection).toBeTruthy();

      // Find all code blocks in the quick start section
      const codeBlocks = quickstartSection.querySelectorAll('pre, code, .code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Check that pre elements exist
      const preElements = quickstartSection.querySelectorAll('pre');
      expect(preElements.length).toBeGreaterThan(0);

      // Check that code elements exist
      const codeElements = quickstartSection.querySelectorAll('code');
      expect(codeElements.length).toBeGreaterThan(0);
    });

    test('git clone command should be inside a code element', () => {
      // Find code elements containing the git clone command
      const codeElements = quickstartSection.querySelectorAll('code');
      const preElements = quickstartSection.querySelectorAll('pre');

      let foundInCodeBlock = false;

      // Check code elements
      codeElements.forEach(el => {
        if (el.textContent.includes('git clone')) {
          foundInCodeBlock = true;
        }
      });

      // Also check pre elements that might contain the command directly
      preElements.forEach(el => {
        if (el.textContent.includes('git clone')) {
          foundInCodeBlock = true;
        }
      });

      expect(foundInCodeBlock).toBe(true);
    });

    test('cargo build command should be inside a code element', () => {
      const codeElements = quickstartSection.querySelectorAll('code');
      const preElements = quickstartSection.querySelectorAll('pre');

      let foundInCodeBlock = false;

      codeElements.forEach(el => {
        if (el.textContent.includes('cargo build')) {
          foundInCodeBlock = true;
        }
      });

      preElements.forEach(el => {
        if (el.textContent.includes('cargo build')) {
          foundInCodeBlock = true;
        }
      });

      expect(foundInCodeBlock).toBe(true);
    });

    test('server run command should be inside a code element', () => {
      const codeElements = quickstartSection.querySelectorAll('code');
      const preElements = quickstartSection.querySelectorAll('pre');

      let foundInCodeBlock = false;

      codeElements.forEach(el => {
        if (el.textContent.includes('mirdb-server')) {
          foundInCodeBlock = true;
        }
      });

      preElements.forEach(el => {
        if (el.textContent.includes('mirdb-server')) {
          foundInCodeBlock = true;
        }
      });

      expect(foundInCodeBlock).toBe(true);
    });
  });

  // Additional tests for Quick Start section structure
  describe('Quick Start Section Structure', () => {
    test('should have a quick start section element', () => {
      expect(quickstartSection).toBeTruthy();
      expect(quickstartSection.tagName).toBe('SECTION');
    });

    test('should have a section title', () => {
      const title = quickstartSection.querySelector('h2, .section-title');
      expect(title).toBeTruthy();
      expect(title.textContent.toLowerCase()).toContain('quick start');
    });

    test('commands should be in logical order: clone -> build -> run', () => {
      const sectionContent = quickstartSection.textContent;

      const cloneIndex = sectionContent.indexOf('git clone');
      const buildIndex = sectionContent.indexOf('cargo build');
      const runIndex = sectionContent.indexOf('mirdb-server');

      // All commands should be present
      expect(cloneIndex).toBeGreaterThanOrEqual(0);
      expect(buildIndex).toBeGreaterThan(cloneIndex);
      expect(runIndex).toBeGreaterThan(buildIndex);
    });
  });
});
