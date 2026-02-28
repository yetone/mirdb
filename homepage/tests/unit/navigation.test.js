/**
 * Navigation & External Links Tests
 * Owner: Scenario 5 - Navigation & External Links
 *
 * Tests for:
 * - GitHub link in header/hero section
 * - GitHub icon/button visibility
 * - Footer element with links section
 * - Repository, Documentation, and License links in footer
 * - External link attributes (target="_blank", rel="noopener")
 *
 * Requirements: REQ-5
 * @jest-environment jsdom
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

describe('Navigation & External Links', () => {
  beforeEach(() => {
    // Load the homepage HTML
    const htmlPath = resolve(__dirname, '../../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    document.documentElement.innerHTML = html;
  });

  /**
   * Test Case 1: GitHub link in hero/header section
   * Input: Query for GitHub link in hero/header section
   * Expected: Link element with href containing 'github.com/yetone/mirdb' exists in hero or header
   */
  describe('Test Case 1: GitHub Link in Hero/Header Section', () => {
    test('GitHub link exists in header navigation', () => {
      const header = document.querySelector('header');
      const githubLink = header.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink).toBeInTheDocument();
    });

    test('GitHub link exists in hero section', () => {
      const hero = document.getElementById('hero');
      const githubLink = hero.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink).toBeInTheDocument();
    });

    test('GitHub link href contains correct repository path', () => {
      const header = document.querySelector('header');
      const githubLink = header.querySelector('a[href*="github.com"]');
      expect(githubLink.getAttribute('href')).toContain('github.com/yetone/mirdb');
    });
  });

  /**
   * Test Case 2: GitHub icon/button visibility
   * Input: Query for GitHub icon/button visibility
   * Expected: GitHub icon or 'GitHub' text is visible and clickable
   */
  describe('Test Case 2: GitHub Icon/Button Visibility', () => {
    test('GitHub button in header has GitHub icon', () => {
      const header = document.querySelector('header');
      const githubLink = header.querySelector('a[href*="github.com"]');
      const icon = githubLink.querySelector('svg');
      expect(icon).not.toBeNull();
      expect(icon.classList.contains('nav__github-icon')).toBe(true);
    });

    test('GitHub button in header contains "GitHub" text', () => {
      const header = document.querySelector('header');
      const githubLink = header.querySelector('a[href*="github.com"]');
      expect(githubLink.textContent).toContain('GitHub');
    });

    test('GitHub button in hero has GitHub icon', () => {
      const hero = document.getElementById('hero');
      const githubLink = hero.querySelector('a[href*="github.com"]');
      const icon = githubLink.querySelector('svg');
      expect(icon).not.toBeNull();
    });

    test('GitHub icon is decorative (aria-hidden)', () => {
      const header = document.querySelector('header');
      const githubLink = header.querySelector('a[href*="github.com"]');
      const icon = githubLink.querySelector('svg');
      expect(icon.getAttribute('aria-hidden')).toBe('true');
    });
  });

  /**
   * Test Case 3: Footer element exists with links section
   * Input: Query for footer element
   * Expected: Footer element exists with links section
   */
  describe('Test Case 3: Footer Element Existence', () => {
    test('footer element exists', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer).toBeInTheDocument();
    });

    test('footer has footer class', () => {
      const footer = document.querySelector('footer');
      expect(footer.classList.contains('footer')).toBe(true);
    });

    test('footer contains links section', () => {
      const footer = document.querySelector('footer');
      const linksSection = footer.querySelector('.footer__links');
      expect(linksSection).not.toBeNull();
    });

    test('footer has proper structure with content area', () => {
      const footer = document.querySelector('footer');
      const content = footer.querySelector('.footer__content');
      expect(content).not.toBeNull();
    });
  });

  /**
   * Test Case 4: Repository link in footer
   * Input: Query for Repository link in footer
   * Expected: Link with text 'Repository' or 'GitHub' exists in footer pointing to github.com
   */
  describe('Test Case 4: Repository Link in Footer', () => {
    test('Repository link exists in footer', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const repoLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('repository') ||
        link.textContent.toLowerCase().includes('github')
      );
      expect(repoLink).not.toBeNull();
    });

    test('Repository link points to github.com', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const repoLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('repository')
      );
      expect(repoLink).not.toBeNull();
      expect(repoLink.getAttribute('href')).toContain('github.com');
    });

    test('Repository link points to correct MirDB repository', () => {
      const footer = document.querySelector('footer');
      const repoLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(repoLink).not.toBeNull();
    });
  });

  /**
   * Test Case 5: Documentation link in footer
   * Input: Query for Documentation link in footer
   * Expected: Link with text containing 'Doc' or 'Documentation' exists in footer
   */
  describe('Test Case 5: Documentation Link in Footer', () => {
    test('Documentation link exists in footer', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const docLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('doc')
      );
      expect(docLink).not.toBeNull();
    });

    test('Documentation link has valid href', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const docLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('documentation')
      );
      expect(docLink).not.toBeNull();
      expect(docLink.getAttribute('href')).toBeTruthy();
    });
  });

  /**
   * Test Case 6: License link in footer
   * Input: Query for License link in footer
   * Expected: Link with text 'License' exists in footer
   */
  describe('Test Case 6: License Link in Footer', () => {
    test('License link exists in footer', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const licenseLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('license')
      );
      expect(licenseLink).not.toBeNull();
    });

    test('License link points to LICENSE file on GitHub', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const licenseLink = Array.from(links).find(link =>
        link.textContent.toLowerCase().includes('license')
      );
      expect(licenseLink).not.toBeNull();
      expect(licenseLink.getAttribute('href')).toContain('LICENSE');
    });
  });

  /**
   * Test Case 7: GitHub link href value verification
   * Input: Verify GitHub link href value
   * Expected: Link href equals 'https://github.com/yetone/mirdb' or includes this URL
   */
  describe('Test Case 7: GitHub Link Href Verification', () => {
    test('GitHub link in header has exact repository URL', () => {
      const header = document.querySelector('header');
      const githubLink = header.querySelector('a[href*="github.com"]');
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link in hero has exact repository URL', () => {
      const hero = document.getElementById('hero');
      const githubLink = hero.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('Footer repository link has exact URL', () => {
      const footer = document.querySelector('footer');
      const repoLink = footer.querySelector('a[href="https://github.com/yetone/mirdb"]');
      expect(repoLink).not.toBeNull();
    });
  });

  /**
   * Test Case 8: External links security attributes
   * Input: Check external links have target='_blank' or rel='noopener'
   * Expected: External links include target='_blank' and rel='noopener' for security
   */
  describe('Test Case 8: External Link Security Attributes', () => {
    test('external links in header have target="_blank"', () => {
      const header = document.querySelector('header');
      const externalLinks = header.querySelectorAll('a[href^="http"]');
      externalLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    test('external links in header have rel="noopener"', () => {
      const header = document.querySelector('header');
      const externalLinks = header.querySelectorAll('a[href^="http"]');
      externalLinks.forEach(link => {
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });

    test('external links in footer have target="_blank"', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer.querySelectorAll('a[href^="http"]');
      externalLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    test('external links in footer have rel="noopener"', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer.querySelectorAll('a[href^="http"]');
      externalLinks.forEach(link => {
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });

    test('external links have rel="noreferrer" for additional security', () => {
      const allExternalLinks = document.querySelectorAll('a[href^="http"]');
      allExternalLinks.forEach(link => {
        expect(link.getAttribute('rel')).toContain('noreferrer');
      });
    });
  });

  /**
   * Test Case 9: GitHub link navigation (E2E simulation)
   * Input: Test clicking GitHub link
   * Expected: Link navigates to correct GitHub repository page
   * Note: In unit tests, we verify the link attributes rather than actual navigation
   */
  describe('Test Case 9: GitHub Link Navigation (E2E)', () => {
    test('GitHub link is clickable (has valid href)', () => {
      const header = document.querySelector('header');
      const githubLink = header.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link in hero is clickable', () => {
      const hero = document.getElementById('hero');
      const githubLink = hero.querySelector('.hero__btn--github');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('all GitHub links point to the same repository', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
      expect(githubLinks.length).toBeGreaterThanOrEqual(2); // At least header and hero
      githubLinks.forEach(link => {
        expect(link.getAttribute('href')).toContain('github.com/yetone/mirdb');
      });
    });

    test('footer social GitHub link is correct', () => {
      const footer = document.querySelector('footer');
      const socialLink = footer.querySelector('.footer__social-link[href*="github.com"]');
      expect(socialLink).not.toBeNull();
      expect(socialLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });
  });

  /**
   * Additional Navigation Tests
   */
  describe('Navigation Structure', () => {
    test('header contains nav element', () => {
      const header = document.querySelector('header');
      const nav = header.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    test('nav has proper aria-label', () => {
      const nav = document.querySelector('nav');
      expect(nav.getAttribute('aria-label')).toBe('Main navigation');
    });

    test('nav contains brand/logo link', () => {
      const nav = document.querySelector('nav');
      const brandLink = nav.querySelector('.nav__brand');
      expect(brandLink).not.toBeNull();
    });

    test('nav contains internal section links', () => {
      const nav = document.querySelector('nav');
      const featuresLink = nav.querySelector('a[href="#features"]');
      const quickstartLink = nav.querySelector('a[href="#quickstart"]');
      const statusLink = nav.querySelector('a[href="#status"]');
      expect(featuresLink).not.toBeNull();
      expect(quickstartLink).not.toBeNull();
      expect(statusLink).not.toBeNull();
    });

    test('mobile menu toggle button exists', () => {
      const toggleBtn = document.querySelector('.nav__mobile-toggle');
      expect(toggleBtn).not.toBeNull();
      expect(toggleBtn.getAttribute('type')).toBe('button');
    });

    test('mobile menu toggle has accessibility attributes', () => {
      const toggleBtn = document.querySelector('.nav__mobile-toggle');
      expect(toggleBtn.getAttribute('aria-label')).toBeTruthy();
      expect(toggleBtn.getAttribute('aria-expanded')).toBe('false');
      expect(toggleBtn.getAttribute('aria-controls')).toBe('nav-menu');
    });
  });

  /**
   * Footer Structure Tests
   */
  describe('Footer Structure', () => {
    test('footer has brand section', () => {
      const footer = document.querySelector('footer');
      const brand = footer.querySelector('.footer__brand');
      expect(brand).not.toBeNull();
    });

    test('footer has section headings', () => {
      const footer = document.querySelector('footer');
      const headings = footer.querySelectorAll('.footer__heading');
      expect(headings.length).toBeGreaterThanOrEqual(2);
    });

    test('footer has copyright text', () => {
      const footer = document.querySelector('footer');
      const copyright = footer.querySelector('.footer__copyright');
      expect(copyright).not.toBeNull();
      expect(copyright.textContent).toContain('MirDB');
    });

    test('footer has social links section', () => {
      const footer = document.querySelector('footer');
      const social = footer.querySelector('.footer__social');
      expect(social).not.toBeNull();
    });

    test('external links in footer have visual indicator', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer.querySelectorAll('.footer__links a[href^="http"]');
      externalLinks.forEach(link => {
        const icon = link.querySelector('.footer__external-icon');
        expect(icon).not.toBeNull();
      });
    });
  });
});
