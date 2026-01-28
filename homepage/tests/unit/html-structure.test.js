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

/**
 * Scenario 4: Quick Start Installation Section Tests
 */
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

/**
 * Scenario 5: GitHub Integration and Status Badges Tests
 */
describe('GitHub Integration and Status Badges', () => {
  beforeEach(() => {
    const html = loadHomepageHTML();
    document.body.innerHTML = html;
  });

  describe('TC1: GitHub link in hero/header area', () => {
    test('Link to github.com/yetone/mirdb exists in hero or header area', () => {
      // Check for GitHub link in hero section
      const heroSection = document.querySelector('#hero');
      expect(heroSection).toBeTruthy();

      const githubLinks = heroSection.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
      expect(githubLinks.length).toBeGreaterThan(0);
    });

    test('GitHub link is prominent (in hero CTAs)', () => {
      const heroCtaContainer = document.querySelector('.hero-ctas');
      expect(heroCtaContainer).toBeTruthy();

      const githubLink = heroCtaContainer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).toBeTruthy();
    });
  });

  describe('TC2: GitHub link has correct href attribute', () => {
    test('GitHub link points to correct repository URL', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      // Check that at least one link has the exact URL
      const hasCorrectUrl = Array.from(githubLinks).some(link => {
        const href = link.getAttribute('href');
        return href === 'https://github.com/yetone/mirdb';
      });
      expect(hasCorrectUrl).toBe(true);
    });

    test('Hero GitHub link is functional (has valid href)', () => {
      const heroGithubLink = document.querySelector('#hero a[href*="github.com"]');
      expect(heroGithubLink).toBeTruthy();

      const href = heroGithubLink.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https?:\/\/github\.com\/yetone\/mirdb$/);
    });

    test('GitHub link opens in appropriate context', () => {
      const heroGithubLink = document.querySelector('#hero a[href*="github.com/yetone/mirdb"]');
      expect(heroGithubLink).not.toBeNull();
      // Link should be accessible and have proper href
      expect(heroGithubLink.hasAttribute('href')).toBe(true);
    });
  });

  describe('TC3: CircleCI status badge', () => {
    test('CircleCI badge image is present', () => {
      const badgeImage = document.querySelector('img[src*="circleci"]');
      expect(badgeImage).toBeTruthy();
    });

    test('CircleCI badge has correct src URL', () => {
      const badgeImage = document.querySelector('img[src*="circleci"]');
      expect(badgeImage).toBeTruthy();

      const src = badgeImage.getAttribute('src');
      expect(src).toContain('circleci.com/gh/yetone/mirdb');
    });

    test('CircleCI badge has alt text for accessibility', () => {
      const badgeImage = document.querySelector('img[src*="circleci"]');
      expect(badgeImage).toBeTruthy();

      const altText = badgeImage.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.toLowerCase()).toMatch(/circleci|build|status/);
    });

    test('CircleCI badge uses shield style', () => {
      const badgeImage = document.querySelector('img[src*="circleci"]');
      expect(badgeImage).toBeTruthy();

      const src = badgeImage.getAttribute('src');
      expect(src).toContain('style=shield');
    });
  });

  describe('TC5: GitHub link in footer', () => {
    test('Footer contains link to GitHub repository', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).toBeTruthy();
    });

    test('Footer GitHub link has correct href', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector('a[href*="github.com"]');
      expect(githubLink).toBeTruthy();

      const href = githubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');
    });

    test('Footer GitHub link is accessible (has aria-label)', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).toBeTruthy();

      const ariaLabel = githubLink.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toMatch(/github/);
    });

    test('Footer GitHub link has accessible name', () => {
      const footer = document.querySelector('footer');
      const footerGithubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(footerGithubLink).not.toBeNull();

      // Should have either visible text or aria-label
      const hasAccessibleName = footerGithubLink.textContent.trim().length > 0 ||
                               footerGithubLink.getAttribute('aria-label');
      expect(hasAccessibleName).toBeTruthy();
    });
  });

  describe('Badge container and placement', () => {
    test('CI badge is in footer section', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const badgeImage = footer.querySelector('img[src*="circleci"]');
      expect(badgeImage).toBeTruthy();
    });

    test('CI badge is wrapped in a link to CircleCI', () => {
      const badgeLink = document.querySelector('a[href*="circleci.com"]');
      expect(badgeLink).toBeTruthy();

      const badgeImage = badgeLink.querySelector('img[src*="circleci"]');
      expect(badgeImage).toBeTruthy();
    });

    test('CircleCI badge link points to CI dashboard', () => {
      const circleCIBadge = document.querySelector('img[src*="circleci.com"]');
      expect(circleCIBadge).not.toBeNull();

      const parentLink = circleCIBadge.closest('a');
      expect(parentLink).not.toBeNull();
      expect(parentLink.getAttribute('href')).toContain('circleci.com/gh/yetone/mirdb');
    });
  });
});

/**
 * Scenario 13: Footer Section Tests
 */
describe('Footer Section', () => {
  beforeEach(() => {
    const html = loadHomepageHTML();
    document.body.innerHTML = html;
  });

  describe('TC1: Semantic footer element exists at bottom of page', () => {
    test('Page contains a semantic footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();
    });

    test('Footer uses semantic HTML5 footer tag', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    test('Footer has appropriate role attribute for accessibility', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      // Footer should have contentinfo role (explicit or implicit)
      const role = footer.getAttribute('role');
      // HTML5 footer elements have implicit contentinfo role when used at top level
      // Explicit role is optional but can be 'contentinfo'
      if (role) {
        expect(role).toBe('contentinfo');
      }
      // If no explicit role, the implicit role is contentinfo (valid)
    });

    test('Footer has a class for styling', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      // Footer should have a class (e.g., 'footer' for CSS styling)
      expect(footer.classList.length).toBeGreaterThan(0);
    });

    test('Footer is the last major content element in body', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      // Footer should not be followed by any other main content sections
      const nextElement = footer.nextElementSibling;
      // Footer should either be last element or only followed by scripts
      if (nextElement) {
        expect(nextElement.tagName.toLowerCase()).toBe('script');
      }
    });
  });

  describe('TC2: Footer contains link to GitHub repository', () => {
    test('Footer contains a GitHub link', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const githubLink = footer.querySelector('a[href*="github.com"]');
      expect(githubLink).toBeTruthy();
    });

    test('GitHub link in footer points to the correct repository', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector('a[href*="github.com"]');
      expect(githubLink).toBeTruthy();

      const href = githubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link has accessible name or text', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).toBeTruthy();

      // Link should have text content or aria-label for accessibility
      const hasText = githubLink.textContent.trim().length > 0;
      const hasAriaLabel = githubLink.hasAttribute('aria-label');
      expect(hasText || hasAriaLabel).toBe(true);
    });

    test('GitHub link text mentions GitHub', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).toBeTruthy();

      const textContent = githubLink.textContent.toLowerCase();
      const ariaLabel = (githubLink.getAttribute('aria-label') || '').toLowerCase();

      expect(textContent.includes('github') || ariaLabel.includes('github')).toBe(true);
    });
  });

  describe('TC3: Footer displays license type or links to license', () => {
    test('Footer contains license information', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const footerText = footer.textContent.toLowerCase();

      // Check for common license mentions
      const hasLicenseInfo =
        footerText.includes('license') ||
        footerText.includes('mit') ||
        footerText.includes('apache') ||
        footerText.includes('bsd') ||
        footerText.includes('gpl');

      expect(hasLicenseInfo).toBe(true);
    });

    test('License is MIT license', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const footerText = footer.textContent.toLowerCase();
      expect(footerText).toContain('mit');
    });

    test('License information is visible (not hidden)', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      // Find the element containing the license text
      const footerContent = footer.querySelector('.footer-links, .footer-content');
      expect(footerContent).toBeTruthy();

      const licenseText = footerContent.textContent;
      expect(licenseText.toLowerCase()).toContain('mit');
    });
  });

  describe('Footer structure and content organization', () => {
    test('Footer has a content container for layout', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const contentContainer = footer.querySelector('.footer-content');
      expect(contentContainer).toBeTruthy();
    });

    test('Footer links are grouped together', () => {
      const footer = document.querySelector('footer');
      const linksContainer = footer.querySelector('.footer-links');
      expect(linksContainer).toBeTruthy();
    });

    test('Footer contains CI badge section', () => {
      const footer = document.querySelector('footer');
      const badgeSection = footer.querySelector('.footer-badge');
      expect(badgeSection).toBeTruthy();
    });

    test('CI badge image is present in footer', () => {
      const footer = document.querySelector('footer');
      const badgeImage = footer.querySelector('img[src*="circleci"]');
      expect(badgeImage).toBeTruthy();
    });

    test('CI badge is wrapped in a link', () => {
      const footer = document.querySelector('footer');
      const badgeLink = footer.querySelector('a[href*="circleci"]');
      expect(badgeLink).toBeTruthy();

      const badgeImage = badgeLink.querySelector('img');
      expect(badgeImage).toBeTruthy();
    });
  });
});
