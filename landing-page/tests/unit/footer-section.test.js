/**
 * Footer Section Unit Tests
 * Owner: Scenario 7 - Footer Section
 * Tests for validating footer HTML structure, links, and attribution
 */
const fs = require('fs');
const path = require('path');

describe('Footer Section Unit Tests', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('TC1: Footer HTML Structure', () => {
    test('Footer element exists with correct class and role', () => {
      expect(htmlContent).toMatch(/<footer[^>]*class="footer"[^>]*role="contentinfo"[^>]*>/);
    });

    test('Footer contains container div', () => {
      expect(htmlContent).toMatch(/<footer[^>]*>[\s\S]*<div class="container">/);
    });

    test('Footer has content section', () => {
      expect(htmlContent).toMatch(/<div class="footer__content">/);
    });

    test('Footer has bottom section with license, attribution, and copyright', () => {
      expect(htmlContent).toMatch(/<div class="footer__bottom">/);
    });
  });

  describe('TC1: Footer Contains GitHub Link', () => {
    test('Contains GitHub repository link', () => {
      expect(htmlContent).toMatch(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*class="footer__github-link"/);
    });

    test('GitHub link opens in new tab', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/target="_blank"/);
      expect(footerContent).toMatch(/rel="noopener noreferrer"/);
    });

    test('GitHub link has accessible label', () => {
      expect(htmlContent).toMatch(/aria-label="View MirDB on GitHub"/);
    });

    test('GitHub section has SVG icon', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/<svg[^>]*class="footer__github-icon"/);
    });
  });

  describe('TC3: License Information Displayed', () => {
    test('Footer contains license section', () => {
      expect(htmlContent).toMatch(/<p class="footer__license">/);
    });

    test('License label is present', () => {
      expect(htmlContent).toMatch(/<span class="footer__license-label">License:<\/span>/);
    });

    test('License links to repository for details', () => {
      expect(htmlContent).toMatch(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*class="footer__license-link"/);
    });
  });

  describe('TC4: Author Attribution', () => {
    test('Author yetone is credited in footer', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/yetone/);
    });

    test('Attribution section exists', () => {
      expect(htmlContent).toMatch(/<p class="footer__attribution">/);
    });

    test('Author link points to GitHub profile', () => {
      expect(htmlContent).toMatch(/<a[^>]*href="https:\/\/github\.com\/yetone"[^>]*class="footer__author-link"/);
    });

    test('Author link has data attribute for identification', () => {
      expect(htmlContent).toMatch(/data-author="yetone"/);
    });
  });

  describe('TC1: Copyright Notice', () => {
    test('Copyright notice exists', () => {
      expect(htmlContent).toMatch(/<p class="footer__copyright">/);
    });

    test('Copyright contains year and MirDB', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      // Check for either © character or &copy; HTML entity
      expect(footerContent).toMatch(/(&copy;|©).*MirDB/);
    });
  });

  describe('Footer Navigation Links', () => {
    test('Footer contains navigation links section', () => {
      expect(htmlContent).toMatch(/<nav[^>]*class="footer__nav"[^>]*aria-label="Footer navigation"/);
    });

    test('Footer has link to Features section', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/<a[^>]*href="#features"[^>]*class="footer__link"/);
    });

    test('Footer has link to Usage section', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/<a[^>]*href="#usage"[^>]*class="footer__link"/);
    });

    test('Footer has link to Architecture section', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/<a[^>]*href="#architecture"[^>]*class="footer__link"/);
    });

    test('Footer has link to Getting Started section', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/<a[^>]*href="#getting-started"[^>]*class="footer__link"/);
    });
  });

  describe('Footer Branding', () => {
    test('Footer contains logo', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/<img[^>]*src="assets\/images\/logo\.gif"/);
    });

    test('Footer has branding section', () => {
      expect(htmlContent).toMatch(/<div class="footer__branding">/);
    });

    test('Footer has tagline', () => {
      expect(htmlContent).toMatch(/<p class="footer__tagline">/);
    });
  });

  describe('Accessibility', () => {
    test('Footer has proper role attribute', () => {
      expect(htmlContent).toMatch(/<footer[^>]*role="contentinfo"/);
    });

    test('Footer navigation has aria-label', () => {
      expect(htmlContent).toMatch(/aria-label="Footer navigation"/);
    });

    test('GitHub icon is hidden from screen readers', () => {
      const footerMatch = htmlContent.match(/<footer[\s\S]*?<\/footer>/);
      expect(footerMatch).toBeTruthy();
      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/aria-hidden="true"/);
    });
  });
});
