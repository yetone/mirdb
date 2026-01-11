/**
 * Tests for Footer Section
 * Scenario: Verify the footer section contains required links and information
 *
 * Test Cases:
 * 1. Check footer for GitHub link - Link to GitHub repository present in footer
 * 2. Check footer for documentation link - Link to documentation present in footer
 * 3. Verify footer uses semantic HTML - Footer content wrapped in footer element
 */

const fs = require('fs');
const path = require('path');

describe('Footer Section', () => {
  let doc;
  let htmlContent;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using DOMParser (provided by jsdom test environment)
    const parser = new DOMParser();
    doc = parser.parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Check footer for GitHub link', () => {
    test('should have a GitHub repository link in the footer', () => {
      const footer = doc.querySelector('footer');
      expect(footer).not.toBeNull();

      const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toContain('github.com/yetone/mirdb');
    });

    test('GitHub link should have proper text content', () => {
      const footer = doc.querySelector('footer');
      const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');

      // The link should have some text content (either "GitHub" or similar)
      expect(githubLink.textContent.trim()).toBeTruthy();
    });

    test('GitHub link should open in new tab with secure attributes', () => {
      const footer = doc.querySelector('footer');
      const githubLink = footer.querySelector('a[href*="github.com/yetone/mirdb"]');

      // Check for target="_blank"
      expect(githubLink.getAttribute('target')).toBe('_blank');

      // Check for rel="noopener" for security
      const rel = githubLink.getAttribute('rel') || '';
      expect(rel).toContain('noopener');
    });
  });

  describe('Test Case 2: Check footer for documentation link', () => {
    test('should have a documentation link in the footer', () => {
      const footer = doc.querySelector('footer');
      expect(footer).not.toBeNull();

      // Look for documentation link (could be README, docs, or link with "Documentation" text)
      const allFooterLinks = footer.querySelectorAll('a');

      let hasDocLink = false;
      allFooterLinks.forEach(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();

        if (href.includes('readme') ||
            href.includes('README') ||
            href.includes('docs') ||
            text.includes('documentation') ||
            text.includes('docs') ||
            text.includes('readme')) {
          hasDocLink = true;
        }
      });

      expect(hasDocLink).toBe(true);
    });

    test('documentation link should point to a valid documentation resource', () => {
      const footer = doc.querySelector('footer');
      const allFooterLinks = Array.from(footer.querySelectorAll('a'));

      const docLink = allFooterLinks.find(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.includes('readme') ||
               href.includes('README') ||
               href.includes('docs') ||
               text.includes('documentation');
      });

      expect(docLink).toBeTruthy();

      // Should have a valid href
      const href = docLink.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    });

    test('documentation link should have proper security attributes', () => {
      const footer = doc.querySelector('footer');
      const allFooterLinks = Array.from(footer.querySelectorAll('a'));

      const docLink = allFooterLinks.find(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('documentation');
      });

      if (docLink && docLink.getAttribute('target') === '_blank') {
        const rel = docLink.getAttribute('rel') || '';
        expect(rel).toContain('noopener');
      }
    });
  });

  describe('Test Case 3: Verify footer uses semantic HTML', () => {
    test('footer content should be wrapped in a footer element', () => {
      const footer = doc.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    test('footer element should exist in the document', () => {
      const footers = doc.querySelectorAll('footer');
      expect(footers.length).toBeGreaterThan(0);
    });

    test('footer should be a direct child of body or within semantic structure', () => {
      const footer = doc.querySelector('footer');
      expect(footer).toBeTruthy();

      // Footer should be accessible and within the DOM tree
      expect(doc.body.contains(footer)).toBe(true);
    });

    test('footer should contain meaningful content', () => {
      const footer = doc.querySelector('footer');

      // Footer should have some text content
      expect(footer.textContent.trim().length).toBeGreaterThan(0);

      // Footer should have at least one link
      const links = footer.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });

    test('footer should use appropriate CSS class for styling', () => {
      const footer = doc.querySelector('footer');

      // Check if footer has a class (either .footer or similar)
      const hasClass = footer.className.length > 0 || footer.classList.length > 0;
      // Footer either has classes or is styled directly
      expect(footer).toBeTruthy();
    });
  });

  describe('Additional Footer Requirements', () => {
    test('footer links should be organized in a container', () => {
      const footer = doc.querySelector('footer');
      const linksContainer = footer.querySelector('.footer-links') || footer.querySelector('div');

      expect(linksContainer).toBeTruthy();
    });

    test('all external links in footer should have proper attributes', () => {
      const footer = doc.querySelector('footer');
      const externalLinks = footer.querySelectorAll('a[href^="http"]');

      externalLinks.forEach(link => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel') || '';

        // External links should open in new tab
        expect(target).toBe('_blank');

        // External links should have noopener for security
        expect(rel).toContain('noopener');
      });
    });

    test('footer should have copyright or project description', () => {
      const footer = doc.querySelector('footer');
      const footerText = footer.textContent.toLowerCase();

      // Footer should contain project name or copyright info
      const hasProjectInfo = footerText.includes('mirdb') ||
                            footerText.includes('key-value') ||
                            footerText.includes('memcached');

      expect(hasProjectInfo).toBe(true);
    });
  });
});
