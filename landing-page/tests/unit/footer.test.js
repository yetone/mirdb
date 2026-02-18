/**
 * Footer Section Unit Tests
 * Owner: Scenario 8 - Footer Section
 *
 * Test cases:
 * - Footer element exists using semantic <footer> tag
 * - Link to documentation or docs page exists
 * - Link to support or contact page/email exists
 * - Link to privacy policy exists
 * - Link to terms of service exists
 * - Copyright text present with year
 * - All links have valid href values (not empty or '#')
 */

const fs = require('fs');
const path = require('path');

describe('Footer Section Unit Tests', () => {
  let document;
  let footer;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');

    // Create a DOM from the HTML using jsdom
    document = new DOMParser().parseFromString(html, 'text/html');
    // Select the main page footer with role="contentinfo" or class="footer"
    // to avoid matching testimonial attribution footers
    footer = document.querySelector('footer.footer, footer[role="contentinfo"]');
  });

  describe('Test Case 1: Footer element exists using semantic <footer> tag', () => {
    test('semantic footer element exists', () => {
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    test('footer has role="contentinfo" for accessibility', () => {
      expect(footer.getAttribute('role')).toBe('contentinfo');
    });

    test('footer has appropriate class for styling', () => {
      expect(footer.classList.contains('footer')).toBe(true);
    });
  });

  describe('Test Case 2: Link to documentation or docs page exists', () => {
    test('documentation link exists in footer', () => {
      const docLinks = footer.querySelectorAll('a[href*="doc"], a[href*="guide"], a[href*="help"]');
      const linkTexts = Array.from(footer.querySelectorAll('a')).filter(a => {
        const text = a.textContent.toLowerCase();
        return text.includes('doc') || text.includes('guide') || text.includes('help');
      });

      const hasDocLink = docLinks.length > 0 || linkTexts.length > 0;
      expect(hasDocLink).toBe(true);
    });

    test('documentation link has meaningful text', () => {
      const links = Array.from(footer.querySelectorAll('a'));
      const docLink = links.find(a => {
        const text = a.textContent.toLowerCase();
        return text.includes('doc') || text.includes('guide') || text.includes('help');
      });

      expect(docLink).toBeDefined();
      expect(docLink.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: Link to support or contact page/email exists', () => {
    test('support or contact link exists in footer', () => {
      const supportLinks = footer.querySelectorAll('a[href*="support"], a[href*="contact"], a[href^="mailto:"]');
      const linkTexts = Array.from(footer.querySelectorAll('a')).filter(a => {
        const text = a.textContent.toLowerCase();
        return text.includes('support') || text.includes('contact') || text.includes('help');
      });

      const hasSupportLink = supportLinks.length > 0 || linkTexts.length > 0;
      expect(hasSupportLink).toBe(true);
    });

    test('support/contact link has accessible text', () => {
      const links = Array.from(footer.querySelectorAll('a'));
      const supportLink = links.find(a => {
        const text = a.textContent.toLowerCase();
        const href = (a.getAttribute('href') || '').toLowerCase();
        return text.includes('support') || text.includes('contact') ||
               href.includes('support') || href.includes('contact') || href.startsWith('mailto:');
      });

      expect(supportLink).toBeDefined();
    });
  });

  describe('Test Case 4: Link to privacy policy exists', () => {
    test('privacy policy link exists in footer', () => {
      const privacyLinks = footer.querySelectorAll('a[href*="privacy"]');
      const linkTexts = Array.from(footer.querySelectorAll('a')).filter(a => {
        const text = a.textContent.toLowerCase();
        return text.includes('privacy');
      });

      const hasPrivacyLink = privacyLinks.length > 0 || linkTexts.length > 0;
      expect(hasPrivacyLink).toBe(true);
    });

    test('privacy policy link has appropriate text', () => {
      const links = Array.from(footer.querySelectorAll('a'));
      const privacyLink = links.find(a => {
        const text = a.textContent.toLowerCase();
        return text.includes('privacy');
      });

      expect(privacyLink).toBeDefined();
      expect(privacyLink.textContent.toLowerCase()).toContain('privacy');
    });
  });

  describe('Test Case 5: Link to terms of service exists', () => {
    test('terms of service link exists in footer', () => {
      const termsLinks = footer.querySelectorAll('a[href*="terms"]');
      const linkTexts = Array.from(footer.querySelectorAll('a')).filter(a => {
        const text = a.textContent.toLowerCase();
        return text.includes('terms') || text.includes('tos');
      });

      const hasTermsLink = termsLinks.length > 0 || linkTexts.length > 0;
      expect(hasTermsLink).toBe(true);
    });

    test('terms of service link has appropriate text', () => {
      const links = Array.from(footer.querySelectorAll('a'));
      const termsLink = links.find(a => {
        const text = a.textContent.toLowerCase();
        return text.includes('terms');
      });

      expect(termsLink).toBeDefined();
      expect(termsLink.textContent.toLowerCase()).toContain('terms');
    });
  });

  describe('Test Case 6: Copyright text present with year', () => {
    test('copyright text exists in footer', () => {
      const footerText = footer.textContent.toLowerCase();
      const hasCopyright = footerText.includes('©') || footerText.includes('copyright');

      expect(hasCopyright).toBe(true);
    });

    test('copyright includes a year', () => {
      const footerText = footer.textContent;
      // Match a 4-digit year (2020-2030 range or similar)
      const yearPattern = /\b20\d{2}\b/;

      expect(yearPattern.test(footerText)).toBe(true);
    });

    test('copyright includes company/product name', () => {
      const footerText = footer.textContent.toLowerCase();
      // Should include "product" or company name
      const hasProductName = footerText.includes('product') ||
                            footerText.includes('company') ||
                            footerText.includes('all rights reserved');

      expect(hasProductName).toBe(true);
    });
  });

  describe('Test Case 7: All footer links have valid href values', () => {
    test('no links have empty href attributes', () => {
      const links = footer.querySelectorAll('a');

      expect(links.length).toBeGreaterThan(0);

      links.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).not.toBeNull();
        expect(href).not.toBe('');
      });
    });

    test('no links have only "#" as href', () => {
      const links = footer.querySelectorAll('a');

      links.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).not.toBe('#');
      });
    });

    test('all links have valid href format', () => {
      const links = footer.querySelectorAll('a');

      links.forEach(link => {
        const href = link.getAttribute('href');
        // Valid formats: starts with /, http, https, mailto:, tel:, or is a relative path
        const isValidHref = href.startsWith('/') ||
                          href.startsWith('http') ||
                          href.startsWith('mailto:') ||
                          href.startsWith('tel:') ||
                          href.startsWith('#') === false && href.length > 0;

        expect(isValidHref).toBe(true);
      });
    });
  });

  describe('Footer structure and accessibility', () => {
    test('footer contains a container for proper layout', () => {
      const container = footer.querySelector('.container');
      expect(container).not.toBeNull();
    });

    test('footer navigation groups are properly structured', () => {
      // Check for organized link groups (nav or div with links)
      const hasStructuredLinks = footer.querySelector('nav, .footer-links, .footer-nav, [class*="footer"]');
      expect(hasStructuredLinks).not.toBeNull();
    });

    test('footer links are keyboard accessible', () => {
      const links = footer.querySelectorAll('a');
      links.forEach(link => {
        // Links are inherently focusable, but should not have negative tabindex
        const tabindex = link.getAttribute('tabindex');
        if (tabindex !== null) {
          expect(parseInt(tabindex)).toBeGreaterThanOrEqual(0);
        }
      });
    });
  });
});
