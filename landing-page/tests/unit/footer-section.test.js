/**
 * Footer Section Unit Tests
 * Owner: Scenario 7 - Footer Section
 *
 * Test Cases:
 * - TC1: Check footer HTML structure (GitHub link, license info, author attribution, copyright notice)
 * - TC3: Verify license information displayed
 * - TC4: Verify author attribution (yetone credited)
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
    test('should contain footer element with correct class', () => {
      expect(htmlContent).toMatch(/<footer[^>]*class="footer"[^>]*>/);
    });

    test('should contain GitHub repository link', () => {
      expect(htmlContent).toMatch(/<a[^>]*href="https:\/\/github\.com\/yetone\/mirdb"[^>]*>/);
    });

    test('should have GitHub link with target="_blank"', () => {
      // Extract the footer section
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      // Check for GitHub link with target="_blank" in footer
      expect(footerContent).toMatch(/href="https:\/\/github\.com\/yetone\/mirdb"[^>]*target="_blank"/);
    });

    test('should have GitHub link with rel="noopener noreferrer" for security', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/rel="noopener noreferrer"/);
    });

    test('should contain license information', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      // Check for license text (MIT or other common license)
      expect(footerContent).toMatch(/license|License|MIT|Apache|GPL/i);
    });

    test('should contain author attribution for yetone', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/yetone/i);
    });

    test('should contain copyright notice', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      // Check for copyright symbol or text
      expect(footerContent).toMatch(/(&copy;|©|Copyright)/i);
    });
  });

  describe('TC3: License Information Display', () => {
    test('should display license type text', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      // License text should be present and visible (MIT is the common Rust project license)
      expect(footerContent).toMatch(/MIT License|Licensed under MIT|MIT/i);
    });

    test('should have license in an element that can be styled', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      // License should be in a span, p, or div element with class for styling
      expect(footerContent).toMatch(/<(span|p|div|a)[^>]*>[^<]*MIT[^<]*<\/(span|p|div|a)>/i);
    });
  });

  describe('TC4: Author Attribution', () => {
    test('should credit author yetone in footer', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/yetone/);
    });

    test('should have author link to GitHub profile', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/href="https:\/\/github\.com\/yetone"/);
    });

    test('should include created by or author attribution text', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      // Check for "Created by", "by", "Author:", or similar attribution text
      expect(footerContent).toMatch(/(Created by|by|Author|Made by)/i);
    });
  });

  describe('Footer Accessibility', () => {
    test('should use semantic footer element', () => {
      expect(htmlContent).toMatch(/<footer/);
    });

    test('should have container for proper layout', () => {
      const footerMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerMatch).not.toBeNull();

      const footerContent = footerMatch[0];
      expect(footerContent).toMatch(/class="container"/);
    });
  });
});
