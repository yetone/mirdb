/**
 * Tests for External Navigation Links (REQ-6)
 * Verify navigation to external resources like GitHub and documentation
 */

const fs = require('fs');
const path = require('path');

describe('External Navigation Links', () => {
  let document;

  beforeEach(() => {
    const html = fs.readFileSync(path.resolve(__dirname, '../index.html'), 'utf8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  // Test Case 1: Check for GitHub repository link
  describe('Test Case 1: GitHub Repository Link', () => {
    test('should have at least one link to GitHub repository', () => {
      const allLinks = document.querySelectorAll('a');
      const githubLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLinks.length).toBeGreaterThanOrEqual(1);
    });

    test('should have GitHub link in hero section', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      expect(heroSection).not.toBeNull();

      const heroLinks = heroSection.querySelectorAll('a');
      const githubLink = Array.from(heroLinks).find(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLink).not.toBeNull();
    });

    test('should have GitHub link in footer', () => {
      const footer = document.querySelector('footer, .footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelectorAll('a');
      const githubLink = Array.from(footerLinks).find(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.includes('github.com') || text.includes('github');
      });

      expect(githubLink).not.toBeNull();
    });

    test('GitHub link should point to MirDB repository', () => {
      const allLinks = document.querySelectorAll('a');
      const githubLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLinks.length).toBeGreaterThanOrEqual(1);

      // At least one GitHub link should contain mirdb
      const mirdbGithubLink = githubLinks.find(link => {
        const href = link.getAttribute('href') || '';
        return href.toLowerCase().includes('mirdb');
      });

      expect(mirdbGithubLink).not.toBeNull();
    });
  });

  // Test Case 2: Check for documentation link
  describe('Test Case 2: Documentation Link', () => {
    test('should have a documentation link (may be placeholder)', () => {
      const allLinks = document.querySelectorAll('a');
      const docLink = Array.from(allLinks).find(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return (
          text.includes('doc') ||
          text.includes('documentation') ||
          href.includes('doc') ||
          href.includes('#documentation')
        );
      });

      expect(docLink).not.toBeNull();
    });

    test('documentation link should be in footer navigation', () => {
      const footer = document.querySelector('footer, .footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelectorAll('a');
      const docLink = Array.from(footerLinks).find(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('doc') || text.includes('documentation');
      });

      expect(docLink).not.toBeNull();
    });
  });

  // Test Case 3: Verify external link security attributes
  describe('Test Case 3: External Link Security Attributes', () => {
    test('all external links should have target="_blank"', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      expect(externalLinks.length).toBeGreaterThanOrEqual(1);

      externalLinks.forEach(link => {
        const target = link.getAttribute('target');
        expect(target).toBe('_blank');
      });
    });

    test('all external links should have rel="noopener"', () => {
      const allLinks = document.querySelectorAll('a');
      const externalLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      expect(externalLinks.length).toBeGreaterThanOrEqual(1);

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });

    test('GitHub link in hero should have proper security attributes', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      expect(heroSection).not.toBeNull();

      const heroLinks = heroSection.querySelectorAll('a');
      const githubLink = Array.from(heroLinks).find(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });

    test('GitHub link in footer should have proper security attributes', () => {
      const footer = document.querySelector('footer, .footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelectorAll('a');
      const githubLink = Array.from(footerLinks).find(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
    });
  });

  // Test Case 4: Verify GitHub link navigation behavior
  describe('Test Case 4: GitHub Link Navigation', () => {
    test('GitHub link should have valid URL format', () => {
      const allLinks = document.querySelectorAll('a');
      const githubLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLinks.length).toBeGreaterThanOrEqual(1);

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https:\/\/github\.com\//);
      });
    });

    test('GitHub link should be accessible (has href attribute)', () => {
      const allLinks = document.querySelectorAll('a');
      const githubLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLinks.length).toBeGreaterThanOrEqual(1);

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).not.toBeNull();
        expect(href).not.toBe('');
        expect(href).not.toBe('#');
      });
    });

    test('View on GitHub button in hero should contain "GitHub" text', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      expect(heroSection).not.toBeNull();

      const heroLinks = heroSection.querySelectorAll('a');
      const githubBtn = Array.from(heroLinks).find(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('github');
      });

      expect(githubBtn).not.toBeNull();
      expect(githubBtn.textContent.toLowerCase()).toContain('github');
    });

    test('clicking GitHub link should open in new tab (via target="_blank")', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      expect(heroSection).not.toBeNull();

      const heroLinks = heroSection.querySelectorAll('a');
      const githubLink = Array.from(heroLinks).find(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLink).not.toBeNull();
      // Verify the link has target="_blank" which causes it to open in new tab
      expect(githubLink.getAttribute('target')).toBe('_blank');
      // Verify the URL is a valid GitHub repository URL
      expect(githubLink.getAttribute('href')).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+/);
    });
  });

  // Additional tests for comprehensive coverage
  describe('Additional External Link Tests', () => {
    test('external links should not have javascript: protocol', () => {
      const allLinks = document.querySelectorAll('a');
      const externalishLinks = Array.from(allLinks).filter(link => {
        const text = link.textContent.toLowerCase();
        return text.includes('github') || text.includes('doc');
      });

      externalishLinks.forEach(link => {
        const href = link.getAttribute('href') || '';
        expect(href).not.toMatch(/^javascript:/i);
      });
    });

    test('page should have both hero and footer with GitHub links', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"], header.hero');
      const footer = document.querySelector('footer, .footer');

      expect(heroSection).not.toBeNull();
      expect(footer).not.toBeNull();

      const heroGithubLinks = heroSection.querySelectorAll('a[href*="github.com"]');
      const footerGithubLinks = footer.querySelectorAll('a[href*="github.com"]');

      expect(heroGithubLinks.length).toBeGreaterThanOrEqual(1);
      expect(footerGithubLinks.length).toBeGreaterThanOrEqual(1);
    });
  });
});
