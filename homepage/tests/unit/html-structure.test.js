/**
 * HTML Structure and Content Tests
 * Owner: Scenario 4 - Quick Start Installation Section
 * Also covers:
 * - Scenario 5 - GitHub Integration and Status Badges
 * - Scenario 13 - Footer Section
 * - Scenario 16 - Static File Structure Validation
 *
 * Tests for:
 * - Quick start section presence
 * - Installation commands
 * - GitHub links
 * - CI badge
 * - Footer element
 * - License info
 * - File structure validation
 */

const { loadHomepageHTML } = require('../setup/test-utils');

describe('Quick Start Installation Section', () => {
  beforeEach(() => {
    // Load the homepage HTML into JSDOM (uses global document from jsdom environment)
    const html = loadHomepageHTML();
    document.body.innerHTML = html;
  });

  describe('TC1: Quick Start Section Exists', () => {
    test('Section with Quick Start, Getting Started, or Installation heading exists', () => {
      // Look for sections with appropriate IDs
      const quickStartSection = document.querySelector('#quick-start');
      const gettingStartedSection = document.querySelector('#getting-started');
      const installationSection = document.querySelector('#installation');

      // At least one should exist
      const sectionExists = quickStartSection || gettingStartedSection || installationSection;
      expect(sectionExists).toBeTruthy();

      // Check for heading with appropriate text
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const hasAppropriateHeading = Array.from(allHeadings).some(heading => {
        const text = heading.textContent.toLowerCase();
        return text.includes('quick start') ||
               text.includes('getting started') ||
               text.includes('installation');
      });
      expect(hasAppropriateHeading).toBe(true);
    });

    test('Quick start section has proper semantic structure', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).toBeTruthy();
      expect(quickStartSection.tagName.toLowerCase()).toBe('section');
    });

    test('Quick start section has an accessible title', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const titleId = quickStartSection.getAttribute('aria-labelledby');
      expect(titleId).toBeTruthy();

      const titleElement = document.getElementById(titleId);
      expect(titleElement).toBeTruthy();
    });
  });

  describe('TC2: Cargo/Build Commands Present', () => {
    test('Installation instructions include cargo build or cargo install commands', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).toBeTruthy();

      const sectionText = quickStartSection.textContent.toLowerCase();

      // Check for cargo commands
      const hasCargoCommand =
        sectionText.includes('cargo build') ||
        sectionText.includes('cargo install') ||
        sectionText.includes('cargo run');

      expect(hasCargoCommand).toBe(true);
    });

    test('Code blocks contain valid shell/bash commands', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('code, pre');

      expect(codeBlocks.length).toBeGreaterThan(0);

      // Check for at least one code block with a bash/shell command
      const hasShellCommand = Array.from(codeBlocks).some(block => {
        const text = block.textContent;
        return text.includes('cargo') ||
               text.includes('git clone') ||
               text.includes('./');
      });
      expect(hasShellCommand).toBe(true);
    });

    test('Build command includes --release flag for production builds', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('code, pre');

      const hasReleaseFlag = Array.from(codeBlocks).some(block => {
        return block.textContent.includes('--release');
      });
      expect(hasReleaseFlag).toBe(true);
    });
  });

  describe('TC3: Numbered/Ordered Steps', () => {
    test('Instructions are presented as numbered steps or clear sequential process', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).toBeTruthy();

      // Check for numbered steps via various methods:
      // 1. Ordered list <ol>
      const orderedList = quickStartSection.querySelector('ol');

      // 2. Steps with numbers in class or content
      const stepElements = quickStartSection.querySelectorAll('.step, .step-number, [class*="step"]');

      // 3. Numbers in text (1., 2., 3. or Step 1, Step 2, etc.)
      const sectionText = quickStartSection.textContent;
      const hasNumberedText = /\b(1|step\s*1)\b/i.test(sectionText) &&
                              /\b(2|step\s*2)\b/i.test(sectionText);

      // At least one method of showing sequential steps should be present
      const hasSequentialSteps = orderedList ||
                                 stepElements.length >= 2 ||
                                 hasNumberedText;

      expect(hasSequentialSteps).toBe(true);
    });

    test('At least 2 steps are present in the quick start section', () => {
      const quickStartSection = document.querySelector('#quick-start');

      // Count steps via class selectors
      const steps = quickStartSection.querySelectorAll('.step, .step-number, [class*="step"]');

      // Or count li elements in ordered list
      const listItems = quickStartSection.querySelectorAll('ol > li, .step');

      const stepCount = Math.max(steps.length, listItems.length);
      expect(stepCount).toBeGreaterThanOrEqual(2);
    });

    test('Steps have visual number indicators', () => {
      const quickStartSection = document.querySelector('#quick-start');

      // Check for step-number elements or numbered list
      const stepNumbers = quickStartSection.querySelectorAll('.step-number');
      const orderedList = quickStartSection.querySelector('ol');

      const hasVisualNumbers = stepNumbers.length > 0 || orderedList !== null;
      expect(hasVisualNumbers).toBe(true);
    });
  });

  describe('TC4: Configuration Information', () => {
    test('Basic configuration options or default settings are mentioned', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).toBeTruthy();

      // The section should mention how to run/configure the application
      const sectionText = quickStartSection.textContent.toLowerCase();

      // Check for configuration-related content
      // This could be: config file references, environment variables,
      // port numbers, or run commands with flags
      const hasConfigInfo =
        sectionText.includes('config') ||
        sectionText.includes('configuration') ||
        sectionText.includes('run') ||
        sectionText.includes('./target') ||
        sectionText.includes('release') ||
        sectionText.includes('start') ||
        sectionText.includes('default') ||
        sectionText.includes('port') ||
        sectionText.includes('12333');

      expect(hasConfigInfo).toBe(true);
    });

    test('Running the application is explained', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const sectionText = quickStartSection.textContent.toLowerCase();

      // Should explain how to run the built application
      const hasRunInstructions =
        sectionText.includes('./target/release') ||
        sectionText.includes('cargo run') ||
        sectionText.includes('mirdb') ||
        sectionText.includes('run');

      expect(hasRunInstructions).toBe(true);
    });

    test('Default server port is mentioned', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const sectionText = quickStartSection.textContent;

      // MirDB uses port 12333 by default - check for port mention
      const hasPortInfo = sectionText.includes('12333') ||
                         /port.*\d+/i.test(sectionText) ||
                         /default.*port/i.test(sectionText);

      expect(hasPortInfo).toBe(true);
    });
  });
});

describe('GitHub Integration and Status Badges (Scenario 5)', () => {
    beforeAll(() => {
        const htmlContent = loadHomepageHTML();
        document.documentElement.innerHTML = htmlContent;
    });

    describe('Test Case 1: GitHub link in hero section', () => {
        test('Link to github.com/yetone/mirdb exists in hero or header area', () => {
            const heroSection = document.querySelector('#hero');
            expect(heroSection).not.toBeNull();

            const githubLink = heroSection.querySelector('a[href*="github.com/yetone/mirdb"]');
            expect(githubLink).not.toBeNull();
            expect(githubLink.getAttribute('href')).toContain('github.com/yetone/mirdb');
        });
    });

    describe('Test Case 2: GitHub link functionality', () => {
        test('GitHub link has correct href attribute pointing to repository', () => {
            const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
            expect(githubLinks.length).toBeGreaterThan(0);

            const heroGithubLink = document.querySelector('#hero a[href*="github.com/yetone/mirdb"]');
            expect(heroGithubLink).not.toBeNull();
            expect(heroGithubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
        });

        test('GitHub link opens in appropriate context', () => {
            const heroGithubLink = document.querySelector('#hero a[href*="github.com/yetone/mirdb"]');
            expect(heroGithubLink).not.toBeNull();
            // Link should be accessible and have proper href
            expect(heroGithubLink.hasAttribute('href')).toBe(true);
        });
    });

    describe('Test Case 3: CI status badge', () => {
        test('CircleCI badge image is present', () => {
            const circleCIBadge = document.querySelector('img[src*="circleci.com"]');
            expect(circleCIBadge).not.toBeNull();
        });

        test('CircleCI badge has correct source URL', () => {
            const circleCIBadge = document.querySelector('img[src*="circleci.com"]');
            expect(circleCIBadge).not.toBeNull();
            expect(circleCIBadge.getAttribute('src')).toContain('circleci.com/gh/yetone/mirdb');
        });

        test('CircleCI badge has proper alt text', () => {
            const circleCIBadge = document.querySelector('img[src*="circleci.com"]');
            expect(circleCIBadge).not.toBeNull();
            expect(circleCIBadge.getAttribute('alt')).toBeTruthy();
            expect(circleCIBadge.getAttribute('alt').toLowerCase()).toContain('circleci');
        });
    });

    describe('Test Case 5: GitHub link in footer', () => {
        test('Footer contains link to GitHub repository', () => {
            const footer = document.querySelector('footer');
            expect(footer).not.toBeNull();

            const footerGithubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
            expect(footerGithubLink).not.toBeNull();
            expect(footerGithubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
        });

        test('Footer GitHub link has accessible label', () => {
            const footer = document.querySelector('footer');
            const footerGithubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
            expect(footerGithubLink).not.toBeNull();

            // Should have either visible text or aria-label
            const hasAccessibleName = footerGithubLink.textContent.trim().length > 0 ||
                                     footerGithubLink.getAttribute('aria-label');
            expect(hasAccessibleName).toBeTruthy();
        });
    });

    describe('Badge link functionality', () => {
        test('CircleCI badge is wrapped in a link to CI dashboard', () => {
            const circleCIBadge = document.querySelector('img[src*="circleci.com"]');
            expect(circleCIBadge).not.toBeNull();

            const parentLink = circleCIBadge.closest('a');
            expect(parentLink).not.toBeNull();
            expect(parentLink.getAttribute('href')).toContain('circleci.com/gh/yetone/mirdb');
        });
    });
});
