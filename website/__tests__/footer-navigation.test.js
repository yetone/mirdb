/**
 * Footer Navigation Tests
 * Tests for REQ-9 of MirDB Landing Page
 * Verifies footer contains links to documentation, GitHub, and community resources
 */

const fs = require('fs');
const path = require('path');

describe('Footer Navigation', () => {
  let document;
  let footer;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
    footer = document.querySelector('footer.footer');
  });

  // Test Case 1: Footer element is present at the bottom of the page
  describe('TC-1: Footer Element Presence', () => {
    test('should have footer element present at the bottom of the page', () => {
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
      expect(footer.classList.contains('footer')).toBe(true);
    });
  });

  // Test Case 2: Documentation link is present and visible
  describe('TC-2: Documentation Link Presence', () => {
    test('should display Documentation link that is present and visible', () => {
      const docLink = footer.querySelector('a[href*="doc"], a[href*="documentation"]');
      expect(docLink).not.toBeNull();
      const linkText = docLink.textContent.toLowerCase();
      expect(linkText).toContain('documentation');
    });
  });

  // Test Case 3: GitHub link is present and visible
  describe('TC-3: GitHub Link Presence', () => {
    test('should display GitHub link that is present and visible', () => {
      const githubLink = footer.querySelector('a[href*="github"]');
      expect(githubLink).not.toBeNull();
      const linkText = githubLink.textContent.toLowerCase();
      expect(linkText).toContain('github');
      expect(githubLink.getAttribute('href')).toMatch(/github\.com.*mirdb/i);
    });
  });

  // Test Case 4: License link is present and visible
  describe('TC-4: License Link Presence', () => {
    test('should display License link that is present and visible', () => {
      const licenseLink = footer.querySelector('a[href*="license"], a[href*="LICENSE"]');
      expect(licenseLink).not.toBeNull();
      const linkText = licenseLink.textContent.toLowerCase();
      expect(linkText).toContain('license');
    });
  });

  // Test Case 5: Copyright notice is displayed
  describe('TC-5: Copyright Notice Display', () => {
    test('should display copyright notice', () => {
      const footerText = footer.textContent;
      expect(footerText).toMatch(/©|&copy;|copyright/i);
      expect(footerText).toMatch(/mirdb/i);
      expect(footerText).toMatch(/mit license/i);
    });
  });

  // Test Case 6: Documentation link navigation (integration)
  describe('TC-6: Documentation Link Navigation', () => {
    test('should have Documentation link that navigates to documentation site', () => {
      const docLink = footer.querySelector('a[href*="doc"], a[href*="documentation"]');
      expect(docLink).not.toBeNull();
      const href = docLink.getAttribute('href');
      expect(href).toBeTruthy();
      // Link should point to documentation (internal anchor or external URL)
      expect(
        href.includes('doc') ||
        href.includes('#documentation') ||
        href.includes('readme')
      ).toBe(true);
    });
  });

  // Test Case 7: GitHub link navigation (integration)
  describe('TC-7: GitHub Link Navigation', () => {
    test('should have GitHub link that navigates to MirDB GitHub repository', () => {
      const githubLink = footer.querySelector('a[href*="github"]');
      expect(githubLink).not.toBeNull();
      const href = githubLink.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/github\.com.*mirdb/i);
      // Should open in new tab for external link
      expect(githubLink.getAttribute('target')).toBe('_blank');
      // Should have rel="noopener" for security
      const rel = githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });
  });

  // Test Case 8: License link navigation (integration)
  describe('TC-8: License Link Navigation', () => {
    test('should have License link that navigates to license information page', () => {
      const licenseLink = footer.querySelector('a[href*="license"], a[href*="LICENSE"]');
      expect(licenseLink).not.toBeNull();
      const href = licenseLink.getAttribute('href');
      expect(href).toBeTruthy();
      // License link should point to LICENSE file or license section
      expect(href.toLowerCase()).toMatch(/license/i);
      // Should open in new tab for external link
      expect(licenseLink.getAttribute('target')).toBe('_blank');
      // Should have rel="noopener" for security
      const rel = licenseLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });
  });
});

describe('Footer Accessibility', () => {
  let document;
  let footer;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
    footer = document.querySelector('footer.footer');
  });

  test('footer should be a semantic footer element', () => {
    expect(footer).not.toBeNull();
    expect(footer.tagName.toLowerCase()).toBe('footer');
  });

  test('footer links should have accessible text', () => {
    const links = footer.querySelectorAll('a');
    links.forEach(link => {
      const hasText = link.textContent.trim().length > 0;
      const hasAriaLabel = link.getAttribute('aria-label');
      expect(hasText || hasAriaLabel).toBe(true);
    });
  });

  test('external footer links should have proper attributes', () => {
    const externalLinks = footer.querySelectorAll('a[target="_blank"]');
    externalLinks.forEach(link => {
      const rel = link.getAttribute('rel');
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
    });
  });
});
