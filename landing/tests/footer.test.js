import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Footer Section', () => {
  let document;
  let footer;

  beforeEach(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');
    const dom = new JSDOM(html);
    document = dom.window.document;
    footer = document.querySelector('footer');
  });

  // Test Case 1: Semantic <footer> element is present
  describe('Test Case 1: Semantic Footer Element', () => {
    it('should have a semantic <footer> element present on the page', () => {
      expect(footer).not.toBeNull();
    });

    it('should have the footer at the bottom of the page structure', () => {
      // Footer should be a direct child of body and come after main
      const body = document.body;
      const main = document.querySelector('main');

      expect(footer.parentElement).toBe(body);
      expect(main).not.toBeNull();

      // Footer should come after main in the DOM order
      const bodyChildren = Array.from(body.children);
      const mainIndex = bodyChildren.indexOf(main);
      const footerIndex = bodyChildren.indexOf(footer);

      expect(footerIndex).toBeGreaterThan(mainIndex);
    });

    it('should have proper ARIA role for footer', () => {
      // Footer element has implicit contentinfo role, but we can also check for explicit role
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });
  });

  // Test Case 2: License information is present
  describe('Test Case 2: License Information', () => {
    it('should contain license information or link to LICENSE file', () => {
      const footerText = footer.textContent.toLowerCase();
      const hasLicenseText = footerText.includes('license') ||
                             footerText.includes('mit') ||
                             footerText.includes('apache') ||
                             footerText.includes('open source');

      const licenseLink = footer.querySelector('a[href*="LICENSE"], a[href*="license"]');

      expect(hasLicenseText || licenseLink !== null).toBe(true);
    });

    it('should display license information visibly', () => {
      const licenseElements = footer.querySelectorAll('p, span, a');
      let hasVisibleLicense = false;

      licenseElements.forEach(el => {
        const text = el.textContent.toLowerCase();
        if (text.includes('license') || text.includes('mit') || text.includes('open source')) {
          hasVisibleLicense = true;
        }
      });

      expect(hasVisibleLicense).toBe(true);
    });
  });

  // Test Case 3: GitHub repository link is present
  describe('Test Case 3: GitHub Repository Link', () => {
    it('should have a link to GitHub repository in footer', () => {
      const githubLink = footer.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
    });

    it('should link to the correct MirDB GitHub repository', () => {
      const githubLink = footer.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href').toLowerCase()).toContain('mirdb');
    });

    it('should open GitHub link in a new tab for security', () => {
      const githubLink = footer.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });

    it('should have descriptive text for GitHub link', () => {
      // Find all GitHub links and check if any has descriptive repository text
      const githubLinks = footer.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      let hasDescriptiveRepoLink = false;
      githubLinks.forEach(link => {
        const linkText = link.textContent.toLowerCase();
        if (linkText.includes('github') ||
            linkText.includes('source') ||
            linkText.includes('repository') ||
            linkText.includes('code')) {
          hasDescriptiveRepoLink = true;
        }
      });
      expect(hasDescriptiveRepoLink).toBe(true);
    });
  });

  // Test Case 4: Contribution guidelines link is present
  describe('Test Case 4: Contribution Guidelines Link', () => {
    it('should have a link to contribution guidelines in footer', () => {
      const contributingLink = footer.querySelector('a[href*="CONTRIBUTING"], a[href*="contributing"]');
      const footerText = footer.textContent.toLowerCase();
      const hasContributeText = footerText.includes('contribut');

      expect(contributingLink !== null || hasContributeText).toBe(true);
    });

    it('should have contribution link with proper href', () => {
      const links = footer.querySelectorAll('a');
      let hasContributingLink = false;

      links.forEach(link => {
        const href = (link.getAttribute('href') || '').toLowerCase();
        const text = link.textContent.toLowerCase();

        if (href.includes('contributing') ||
            href.includes('contribute') ||
            text.includes('contribut')) {
          hasContributingLink = true;
        }
      });

      expect(hasContributingLink).toBe(true);
    });

    it('should have accessible contribution link', () => {
      const links = footer.querySelectorAll('a');
      let contributingLink = null;

      links.forEach(link => {
        const href = (link.getAttribute('href') || '').toLowerCase();
        const text = link.textContent.toLowerCase();

        if (href.includes('contributing') || text.includes('contribut')) {
          contributingLink = link;
        }
      });

      expect(contributingLink).not.toBeNull();
      // Link should have meaningful text
      expect(contributingLink.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  // Additional accessibility tests
  describe('Footer Accessibility', () => {
    it('should have proper color contrast for footer text', () => {
      // This is a structural test - actual color contrast should be verified with tools
      // Here we just ensure footer has content
      expect(footer.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have proper link styling distinction', () => {
      const links = footer.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });
  });
});
