/**
 * Tests for Security - External Link Attributes (Scenario 22)
 * Verify external links have proper security attributes to prevent tabnabbing attacks
 */

const fs = require('fs');
const path = require('path');

describe('Security - External Link Attributes', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  /**
   * Helper function to get all external links
   * External links are those with http:// or https:// protocols
   */
  function getExternalLinks() {
    const allLinks = document.querySelectorAll('a');
    return Array.from(allLinks).filter(link => {
      const href = link.getAttribute('href') || '';
      return href.startsWith('http://') || href.startsWith('https://');
    });
  }

  // Test Case 1: Query external links for target attribute
  describe('Test Case 1: External links have target="_blank"', () => {
    test('should have at least one external link on the page', () => {
      const externalLinks = getExternalLinks();
      expect(externalLinks.length).toBeGreaterThanOrEqual(1);
    });

    test('all external links should have target="_blank" attribute', () => {
      const externalLinks = getExternalLinks();
      expect(externalLinks.length).toBeGreaterThanOrEqual(1);

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const target = link.getAttribute('target');
        expect(target).toBe('_blank');
      });
    });

    test('GitHub link in hero section should have target="_blank"', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      expect(heroSection).not.toBeNull();

      const heroLinks = heroSection.querySelectorAll('a');
      const githubLink = Array.from(heroLinks).find(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('external links in footer should have target="_blank"', () => {
      const footer = document.querySelector('footer, .footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelectorAll('a');
      const externalFooterLinks = Array.from(footerLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      expect(externalFooterLinks.length).toBeGreaterThanOrEqual(1);

      externalFooterLinks.forEach(link => {
        const target = link.getAttribute('target');
        expect(target).toBe('_blank');
      });
    });
  });

  // Test Case 2: Query external links for rel attribute
  describe('Test Case 2: External links have rel="noopener" or rel="noopener noreferrer"', () => {
    test('all external links should have rel attribute containing "noopener"', () => {
      const externalLinks = getExternalLinks();
      expect(externalLinks.length).toBeGreaterThanOrEqual(1);

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });

    test('rel attribute should be either "noopener" or "noopener noreferrer"', () => {
      const externalLinks = getExternalLinks();
      expect(externalLinks.length).toBeGreaterThanOrEqual(1);

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        // Accept either 'noopener' alone or 'noopener noreferrer' (with any order)
        const validPatterns = ['noopener', 'noopener noreferrer', 'noreferrer noopener'];
        const isValid = validPatterns.some(pattern => rel === pattern) || rel.includes('noopener');
        expect(isValid).toBe(true);
      });
    });

    test('GitHub link in hero section should have rel="noopener"', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      expect(heroSection).not.toBeNull();

      const heroLinks = heroSection.querySelectorAll('a');
      const githubLink = Array.from(heroLinks).find(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLink).not.toBeNull();
      const rel = githubLink.getAttribute('rel');
      expect(rel).not.toBeNull();
      expect(rel).toContain('noopener');
    });

    test('external links in footer should have rel containing "noopener"', () => {
      const footer = document.querySelector('footer, .footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelectorAll('a');
      const externalFooterLinks = Array.from(footerLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      expect(externalFooterLinks.length).toBeGreaterThanOrEqual(1);

      externalFooterLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });

    test('documentation link in footer should have rel="noopener"', () => {
      const footer = document.querySelector('footer, .footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelectorAll('a');
      const docLink = Array.from(footerLinks).find(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('doc') || text.includes('documentation');
      });

      expect(docLink).not.toBeNull();

      const href = docLink.getAttribute('href');
      if (href && (href.startsWith('http://') || href.startsWith('https://'))) {
        const rel = docLink.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      }
    });
  });

  // Additional security validation tests
  describe('Comprehensive Security Validation', () => {
    test('external links should have both target="_blank" and rel="noopener" together', () => {
      const externalLinks = getExternalLinks();
      expect(externalLinks.length).toBeGreaterThanOrEqual(1);

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        // Both attributes must be present for security
        expect(target).toBe('_blank');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });

    test('no external link should be missing security attributes', () => {
      const externalLinks = getExternalLinks();
      const insecureLinks = externalLinks.filter(link => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');
        return target !== '_blank' || !rel || !rel.includes('noopener');
      });

      expect(insecureLinks).toHaveLength(0);
    });
  });
});
