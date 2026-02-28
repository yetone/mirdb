/**
 * Navigation & External Links Tests
 * Owner: Scenario 5 - Navigation & External Links
 *
 * Tests for:
 * - GitHub link in header/hero section
 * - Footer links (Repository, Documentation, License)
 * - External link security attributes (target="_blank", rel="noopener")
 * - GitHub icon/button visibility
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
  describe('Test Case 1: GitHub Link in Hero/Header', () => {
    test('GitHub link exists in hero or header section', () => {
      const hero = document.getElementById('hero');
      const header = document.querySelector('header');

      const heroGithubLink = hero?.querySelector('a[href*="github.com/yetone/mirdb"]');
      const headerGithubLink = header?.querySelector('a[href*="github.com/yetone/mirdb"]');

      const hasGithubLink = heroGithubLink !== null || headerGithubLink !== null;
      expect(hasGithubLink).toBe(true);
    });

    test('GitHub link href contains github.com/yetone/mirdb', () => {
      const allLinks = document.querySelectorAll('a[href*="github.com"]');
      const mirdbLink = Array.from(allLinks).find(link =>
        link.getAttribute('href').includes('yetone/mirdb')
      );
      expect(mirdbLink).not.toBeNull();
      expect(mirdbLink.getAttribute('href')).toContain('github.com/yetone/mirdb');
    });
  });

  /**
   * Test Case 2: GitHub icon/button visibility
   * Input: Query for GitHub icon/button visibility
   * Expected: GitHub icon or 'GitHub' text is visible and clickable
   */
  describe('Test Case 2: GitHub Icon/Button Visibility', () => {
    test('GitHub button is visible and has GitHub text or icon', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      const hasGithubContent = Array.from(githubLinks).some(link => {
        const hasGithubText = link.textContent.toLowerCase().includes('github');
        const hasGithubIcon = link.querySelector('svg') !== null;
        return hasGithubText || hasGithubIcon;
      });
      expect(hasGithubContent).toBe(true);
    });

    test('GitHub button/link is clickable (is an anchor element)', () => {
      const githubLink = document.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.tagName.toLowerCase()).toBe('a');
    });
  });

  /**
   * Test Case 3: Footer element existence
   * Input: Query for footer element
   * Expected: Footer element exists with links section
   */
  describe('Test Case 3: Footer Element', () => {
    test('footer element exists', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer).toBeInTheDocument();
    });

    test('footer has class "footer"', () => {
      const footer = document.querySelector('footer');
      expect(footer.classList.contains('footer')).toBe(true);
    });

    test('footer contains a links section', () => {
      const footer = document.querySelector('footer');
      const hasLinks = footer.querySelector('a') !== null ||
                       footer.querySelector('.footer__links') !== null ||
                       footer.querySelector('[class*="link"]') !== null;
      expect(hasLinks).toBe(true);
    });
  });

  /**
   * Test Case 4: Repository link in footer
   * Input: Query for Repository link in footer
   * Expected: Link with text 'Repository' or 'GitHub' exists in footer pointing to github.com
   */
  describe('Test Case 4: Repository Link in Footer', () => {
    test('footer contains Repository or GitHub link', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');

      const hasRepoLink = Array.from(links).some(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('repository') || text.includes('github');
      });
      expect(hasRepoLink).toBe(true);
    });

    test('Repository link points to github.com', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');

      const repoLink = Array.from(links).find(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('repository') || text.includes('github');
      });

      expect(repoLink).not.toBeNull();
      expect(repoLink.getAttribute('href')).toContain('github.com');
    });
  });

  /**
   * Test Case 5: Documentation link in footer
   * Input: Query for Documentation link in footer
   * Expected: Link with text containing 'Doc' or 'Documentation' exists in footer
   */
  describe('Test Case 5: Documentation Link in Footer', () => {
    test('footer contains Documentation link', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');

      const hasDocsLink = Array.from(links).some(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('doc') || text.includes('documentation');
      });
      expect(hasDocsLink).toBe(true);
    });
  });

  /**
   * Test Case 6: License link in footer
   * Input: Query for License link in footer
   * Expected: Link with text 'License' exists in footer
   */
  describe('Test Case 6: License Link in Footer', () => {
    test('footer contains License link', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');

      const hasLicenseLink = Array.from(links).some(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('license');
      });
      expect(hasLicenseLink).toBe(true);
    });
  });

  /**
   * Test Case 7: GitHub link href value
   * Input: Verify GitHub link href value
   * Expected: Link href equals 'https://github.com/yetone/mirdb' or includes this URL
   */
  describe('Test Case 7: GitHub Link URL Verification', () => {
    test('GitHub link href equals or includes https://github.com/yetone/mirdb', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      const correctUrl = Array.from(githubLinks).some(link => {
        const href = link.getAttribute('href');
        return href === 'https://github.com/yetone/mirdb' ||
               href.includes('github.com/yetone/mirdb');
      });
      expect(correctUrl).toBe(true);
    });

    test('primary GitHub link has exact URL', () => {
      const heroGithubBtn = document.querySelector('.hero__btn--github');
      if (heroGithubBtn) {
        expect(heroGithubBtn.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
      } else {
        const anyGithubLink = document.querySelector('a[href*="github.com/yetone/mirdb"]');
        expect(anyGithubLink).not.toBeNull();
      }
    });
  });

  /**
   * Test Case 8: External links have security attributes
   * Input: Check external links have target='_blank' or rel='noopener'
   * Expected: External links include target='_blank' and rel='noopener' for security
   */
  describe('Test Case 8: External Link Security', () => {
    test('external links have target="_blank"', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      externalLinks.forEach(link => {
        // External links should open in new tab
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    test('external links have rel="noopener" or rel="noopener noreferrer"', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
      });
    });

    test('GitHub link specifically has proper security attributes', () => {
      const githubLink = document.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });
  });

  /**
   * Additional tests for navigation functionality
   */
  describe('Navigation Structure & Accessibility', () => {
    test('header element exists', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      expect(header).toBeInTheDocument();
    });

    test('header contains nav element', () => {
      const header = document.querySelector('header');
      const nav = header.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    test('nav has aria-label for accessibility', () => {
      const nav = document.querySelector('nav');
      expect(nav.getAttribute('aria-label')).toBeTruthy();
    });

    test('footer has role or semantic meaning', () => {
      const footer = document.querySelector('footer');
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });
  });

  /**
   * External link indicator tests
   */
  describe('External Link Indicators', () => {
    test('external links have visual indicator (icon or text)', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer?.querySelectorAll('a[href^="http"]') || [];

      // Check that external links are identifiable
      externalLinks.forEach(link => {
        const hasIcon = link.querySelector('svg') !== null;
        const hasExternalIndicator = link.classList.contains('external') ||
                                     link.querySelector('[class*="external"]') !== null ||
                                     hasIcon ||
                                     link.getAttribute('target') === '_blank';
        expect(hasExternalIndicator).toBe(true);
      });
    });
  });
});
