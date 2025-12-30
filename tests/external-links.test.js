/**
 * E2E, Unit, and Integration Tests for External Links Functionality (REQ-7)
 *
 * These tests verify that links to GitHub repository, documentation, and
 * community resources work correctly as specified in REQ-7.
 */

const fs = require('fs');
const path = require('path');

describe('External Links Functionality - REQ-7', () => {
  let document;
  let htmlContent;
  const MIRDB_GITHUB_URL = 'https://github.com/yetone/mirdb';

  beforeAll(() => {
    // Load the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Parse HTML using jsdom
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: GitHub Link in Navigation', () => {
    /**
     * Test Case ID: 1
     * Input: Click GitHub link in navigation
     * Expected: Link opens MirDB GitHub repository in new tab
     * Type: e2e
     */
    test('should have GitHub link in navigation bar', () => {
      const nav = document.querySelector('nav');
      expect(nav).toBeTruthy();

      const navLinks = nav.querySelectorAll('.nav-links a');
      const githubLink = Array.from(navLinks).find(
        (link) => link.textContent.toLowerCase().includes('github') || link.href.includes('github')
      );

      expect(githubLink).toBeTruthy();
    });

    test('navigation GitHub link should point to MirDB repository', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');
      const githubLink = Array.from(navLinks).find(
        (link) => link.href.includes('github')
      );

      expect(githubLink).toBeTruthy();
      expect(githubLink.href).toContain(MIRDB_GITHUB_URL);
    });

    test('navigation GitHub link should open in new tab', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');
      const githubLink = Array.from(navLinks).find(
        (link) => link.href.includes('github')
      );

      expect(githubLink).toBeTruthy();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('navigation GitHub link should have security attributes', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a');
      const githubLink = Array.from(navLinks).find(
        (link) => link.href.includes('github')
      );

      expect(githubLink).toBeTruthy();
      const rel = githubLink.getAttribute('rel');
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  describe('Test Case 2: GitHub Link in Hero Section', () => {
    /**
     * Test Case ID: 2
     * Input: Click GitHub link in hero section
     * Expected: Link opens MirDB GitHub repository in new tab
     * Type: e2e
     */
    test('should have GitHub link in hero section', () => {
      const hero = document.querySelector('.hero');
      expect(hero).toBeTruthy();

      const heroButtons = hero.querySelectorAll('.hero-buttons a');
      const githubLink = Array.from(heroButtons).find(
        (link) => link.textContent.toLowerCase().includes('github') || link.href.includes('github')
      );

      expect(githubLink).toBeTruthy();
    });

    test('hero GitHub link should point to MirDB repository', () => {
      const hero = document.querySelector('.hero');
      const heroButtons = hero.querySelectorAll('.hero-buttons a');
      const githubLink = Array.from(heroButtons).find(
        (link) => link.href.includes('github')
      );

      expect(githubLink).toBeTruthy();
      expect(githubLink.href).toContain(MIRDB_GITHUB_URL);
    });

    test('hero GitHub link should open in new tab', () => {
      const hero = document.querySelector('.hero');
      const heroButtons = hero.querySelectorAll('.hero-buttons a');
      const githubLink = Array.from(heroButtons).find(
        (link) => link.href.includes('github')
      );

      expect(githubLink).toBeTruthy();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('hero GitHub link should have security attributes', () => {
      const hero = document.querySelector('.hero');
      const heroButtons = hero.querySelectorAll('.hero-buttons a');
      const githubLink = Array.from(heroButtons).find(
        (link) => link.href.includes('github')
      );

      expect(githubLink).toBeTruthy();
      const rel = githubLink.getAttribute('rel');
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  describe('Test Case 3: External Links Security Attributes', () => {
    /**
     * Test Case ID: 3
     * Input: Check all external links have target='_blank'
     * Expected: External links open in new tabs and have rel='noopener noreferrer' for security
     * Type: unit
     */
    test('all external links should have target="_blank"', () => {
      const allLinks = document.querySelectorAll('a[href^="http"]');

      allLinks.forEach((link) => {
        const href = link.getAttribute('href');
        // Only test external links (not same-origin)
        if (href && href.startsWith('http')) {
          expect(link.getAttribute('target')).toBe('_blank');
        }
      });
    });

    test('all external links with target="_blank" should have rel="noopener noreferrer"', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"][target="_blank"]');

      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    test('should have at least 5 external links on the page', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');
      expect(externalLinks.length).toBeGreaterThanOrEqual(5);
    });

    test('all external GitHub links should point to valid MirDB URLs', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');

      expect(githubLinks.length).toBeGreaterThan(0);

      githubLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https:\/\/github\.com\/yetone\/mirdb/);
      });
    });
  });

  describe('Test Case 4: Link Validity', () => {
    /**
     * Test Case ID: 4
     * Input: Verify no broken links on page
     * Expected: All links return valid HTTP responses (no 404s)
     * Type: integration
     *
     * Note: This test validates link structure and format. Actual HTTP validation
     * would require network requests which are not performed in unit tests.
     */
    test('all links should have valid href attributes (not empty)', () => {
      const allLinks = document.querySelectorAll('a[href]');

      allLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href.trim()).not.toBe('');
      });
    });

    test('all external links should use HTTPS protocol', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      externalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https:\/\//);
      });
    });

    test('internal anchor links should reference valid sections', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        // Skip empty anchor links
        if (href === '#') return;

        const targetId = href.substring(1);
        const targetElement = document.getElementById(targetId);
        expect(targetElement).toBeTruthy();
      });
    });

    test('footer should contain resource links', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const footerLinks = footer.querySelectorAll('a[href]');
      expect(footerLinks.length).toBeGreaterThanOrEqual(3);
    });

    test('footer should have GitHub repository link', () => {
      const footer = document.querySelector('footer');
      const repoLink = footer.querySelector(`a[href="${MIRDB_GITHUB_URL}"]`);

      expect(repoLink).toBeTruthy();
    });

    test('footer should have issue tracker link', () => {
      const footer = document.querySelector('footer');
      const issueLink = footer.querySelector('a[href*="github.com/yetone/mirdb/issues"]');

      expect(issueLink).toBeTruthy();
    });

    test('footer should have documentation link', () => {
      const footer = document.querySelector('footer');
      const docsLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');

      expect(docsLink).toBeTruthy();
    });
  });

  describe('Navigation Internal Links', () => {
    test('navigation should have internal links to page sections', () => {
      const nav = document.querySelector('nav');
      const internalLinks = nav.querySelectorAll('a[href^="#"]');

      expect(internalLinks.length).toBeGreaterThan(0);
    });

    test('Features link should navigate to features section', () => {
      const nav = document.querySelector('nav');
      const featuresLink = nav.querySelector('a[href="#features"]');

      expect(featuresLink).toBeTruthy();

      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeTruthy();
    });

    test('Architecture link should navigate to architecture section', () => {
      const nav = document.querySelector('nav');
      const archLink = nav.querySelector('a[href="#architecture"]');

      expect(archLink).toBeTruthy();

      const archSection = document.getElementById('architecture');
      expect(archSection).toBeTruthy();
    });

    test('Commands link should navigate to commands section', () => {
      const nav = document.querySelector('nav');
      const commandsLink = nav.querySelector('a[href="#commands"]');

      expect(commandsLink).toBeTruthy();

      const commandsSection = document.getElementById('commands');
      expect(commandsSection).toBeTruthy();
    });

    test('Quick Start link should navigate to quickstart section', () => {
      const nav = document.querySelector('nav');
      const quickstartLink = nav.querySelector('a[href="#quickstart"]');

      expect(quickstartLink).toBeTruthy();

      const quickstartSection = document.getElementById('quickstart');
      expect(quickstartSection).toBeTruthy();
    });
  });

  describe('Footer External Links', () => {
    test('footer external links should open in new tab', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer.querySelectorAll('a[href^="http"]');

      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    test('footer external links should have security attributes', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer.querySelectorAll('a[href^="http"]');

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    test('footer should have contributors link', () => {
      const footer = document.querySelector('footer');
      const contributorsLink = footer.querySelector('a[href*="contributors"]');

      expect(contributorsLink).toBeTruthy();
    });

    test('footer should have license link', () => {
      const footer = document.querySelector('footer');
      const licenseLink = footer.querySelector('a[href*="LICENSE"]');

      expect(licenseLink).toBeTruthy();
    });
  });

  describe('Link Accessibility', () => {
    test('all links should have meaningful text content', () => {
      const allLinks = document.querySelectorAll('a[href]');

      allLinks.forEach((link) => {
        const text = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');

        // Link should have either text content or aria-label
        expect(text || ariaLabel).toBeTruthy();
      });
    });

    test('external links should be distinguishable', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      // At least some external links should have GitHub in their text
      const githubLinks = Array.from(externalLinks).filter(
        (link) => link.textContent.toLowerCase().includes('github')
      );

      expect(githubLinks.length).toBeGreaterThan(0);
    });
  });
});
