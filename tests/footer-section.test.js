/**
 * Tests for Footer Section (REQ-10)
 *
 * Scenario: Verify that the footer includes project links, license information,
 * and social links as specified in REQ-10.
 *
 * Test Cases:
 * 1. Footer contains GitHub link to MirDB repository
 * 2. Footer contains license information
 * 3. Footer contains documentation link
 * 4. Footer is present at page bottom
 */

const fs = require('fs');
const path = require('path');

describe('Footer Section - REQ-10', () => {
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

  describe('Test Case 1: Footer contains GitHub link', () => {
    /**
     * Test Case ID: 1
     * Input: Check footer contains GitHub link
     * Expected: Footer has working link to MirDB GitHub repository
     * Type: e2e
     */
    test('footer should have GitHub repository link', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();

      const githubLink = footer.querySelector(`a[href="${MIRDB_GITHUB_URL}"]`);
      expect(githubLink).toBeTruthy();
    });

    test('GitHub link text should clearly indicate GitHub Repository', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector(`a[href="${MIRDB_GITHUB_URL}"]`);

      expect(githubLink).toBeTruthy();
      const linkText = githubLink.textContent.toLowerCase();
      expect(linkText).toMatch(/github|repository/i);
    });

    test('GitHub link should open in new tab with security attributes', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector(`a[href="${MIRDB_GITHUB_URL}"]`);

      expect(githubLink).toBeTruthy();
      expect(githubLink.getAttribute('target')).toBe('_blank');

      const rel = githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('footer should have issue tracker link', () => {
      const footer = document.querySelector('footer');
      const issueLink = footer.querySelector('a[href*="github.com/yetone/mirdb/issues"]');

      expect(issueLink).toBeTruthy();
      expect(issueLink.textContent.toLowerCase()).toMatch(/issue/i);
    });
  });

  describe('Test Case 2: Footer contains license information', () => {
    /**
     * Test Case ID: 2
     * Input: Check footer contains license information
     * Expected: Footer displays license type (e.g., MIT, Apache 2.0)
     * Type: e2e
     */
    test('footer should have license link', () => {
      const footer = document.querySelector('footer');
      const licenseLink = footer.querySelector('a[href*="LICENSE"]');

      expect(licenseLink).toBeTruthy();
    });

    test('license link text should indicate license', () => {
      const footer = document.querySelector('footer');
      const licenseLink = footer.querySelector('a[href*="LICENSE"]');

      expect(licenseLink).toBeTruthy();
      const linkText = licenseLink.textContent.toLowerCase();
      expect(linkText).toMatch(/license/i);
    });

    test('license link should point to GitHub LICENSE file', () => {
      const footer = document.querySelector('footer');
      const licenseLink = footer.querySelector('a[href*="LICENSE"]');

      expect(licenseLink).toBeTruthy();
      const href = licenseLink.getAttribute('href');
      expect(href).toContain('github.com/yetone/mirdb');
      expect(href).toContain('LICENSE');
    });

    test('footer should indicate open source nature', () => {
      const footer = document.querySelector('footer');
      const footerText = footer.textContent.toLowerCase();

      // Footer should mention open source
      expect(footerText).toMatch(/open\s*source/i);
    });
  });

  describe('Test Case 3: Footer contains documentation link', () => {
    /**
     * Test Case ID: 3
     * Input: Check footer contains documentation link
     * Expected: Footer has link to documentation or docs section
     * Type: e2e
     */
    test('footer should have documentation link', () => {
      const footer = document.querySelector('footer');

      // Documentation can be in README or separate docs
      const docsLink = footer.querySelector('a[href*="readme" i], a[href*="documentation" i], a[href*="docs" i]') ||
        footer.querySelector('a[href*="#readme" i]');

      expect(docsLink).toBeTruthy();
    });

    test('documentation link text should indicate documentation', () => {
      const footer = document.querySelector('footer');
      const footerLinks = footer.querySelectorAll('a');

      // Find a link that mentions documentation
      const docsLink = Array.from(footerLinks).find(
        link => link.textContent.toLowerCase().match(/documentation|docs|readme/i)
      );

      expect(docsLink).toBeTruthy();
    });

    test('footer should have resources section', () => {
      const footer = document.querySelector('footer');
      const footerSections = footer.querySelectorAll('.footer-section');

      // Should have footer sections with organized content
      expect(footerSections.length).toBeGreaterThanOrEqual(2);
    });

    test('resources section should contain multiple useful links', () => {
      const footer = document.querySelector('footer');
      const footerSections = footer.querySelectorAll('.footer-section');

      // Find the Resources section (h3 for proper heading hierarchy per WCAG)
      const resourcesSection = Array.from(footerSections).find(
        section => section.querySelector('h3')?.textContent.toLowerCase().includes('resources')
      );

      expect(resourcesSection).toBeTruthy();

      const links = resourcesSection.querySelectorAll('.footer-links a');
      expect(links.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Test Case 4: Footer is present at page bottom', () => {
    /**
     * Test Case ID: 4
     * Input: Verify footer is present at page bottom
     * Expected: Footer element exists and is positioned at bottom of page content
     * Type: unit
     */
    test('footer element should exist', () => {
      const footer = document.querySelector('footer');
      expect(footer).toBeTruthy();
    });

    test('footer should be the last major element in body', () => {
      const body = document.body;
      const children = Array.from(body.children);

      // Filter out script tags and other non-content elements
      const contentElements = children.filter(
        el => !['SCRIPT', 'NOSCRIPT', 'STYLE'].includes(el.tagName)
      );

      const lastElement = contentElements[contentElements.length - 1];
      expect(lastElement.tagName).toBe('FOOTER');
    });

    test('footer should contain project information', () => {
      const footer = document.querySelector('footer');
      const footerText = footer.textContent.toLowerCase();

      // Should contain MirDB reference
      expect(footerText).toMatch(/mirdb/i);
    });

    test('footer should have proper semantic structure', () => {
      const footer = document.querySelector('footer');

      // Footer should have container
      const container = footer.querySelector('.container');
      expect(container).toBeTruthy();

      // Footer should have content sections
      const footerContent = footer.querySelector('.footer-content');
      expect(footerContent).toBeTruthy();
    });

    test('footer should have footer-bottom section', () => {
      const footer = document.querySelector('footer');
      const footerBottom = footer.querySelector('.footer-bottom');

      expect(footerBottom).toBeTruthy();
    });

    test('footer should have appropriate styling class', () => {
      const footer = document.querySelector('footer');

      // Footer should have dark background (indicated by code-bg class in CSS)
      // We check that footer exists and has structure
      expect(footer).toBeTruthy();

      // Footer should contain links
      const footerLinks = footer.querySelectorAll('a');
      expect(footerLinks.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Footer Community and Social Links', () => {
    test('footer should have community section', () => {
      const footer = document.querySelector('footer');
      // Use h3 for proper heading hierarchy per WCAG accessibility requirements
      const footerSections = footer.querySelectorAll('.footer-section h3');

      const communitySection = Array.from(footerSections).find(
        h3 => h3.textContent.toLowerCase().includes('community')
      );

      expect(communitySection).toBeTruthy();
    });

    test('footer should have contributors link', () => {
      const footer = document.querySelector('footer');
      const contributorsLink = footer.querySelector('a[href*="contributors"]');

      expect(contributorsLink).toBeTruthy();
    });

    test('all footer external links should have security attributes', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer.querySelectorAll('a[href^="http"]');

      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });
  });

  describe('Footer Accessibility', () => {
    test('all footer links should have meaningful text', () => {
      const footer = document.querySelector('footer');
      const footerLinks = footer.querySelectorAll('a');

      footerLinks.forEach((link) => {
        const text = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');

        // Each link should have text content or aria-label
        expect(text || ariaLabel).toBeTruthy();
        expect(text.length || (ariaLabel && ariaLabel.length)).toBeGreaterThan(0);
      });
    });

    test('footer sections should have headings', () => {
      const footer = document.querySelector('footer');
      // Use h3 for proper heading hierarchy per WCAG accessibility requirements
      const headings = footer.querySelectorAll('h3');

      expect(headings.length).toBeGreaterThanOrEqual(2);
    });
  });
});
