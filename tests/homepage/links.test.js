/**
 * Navigation and Links Tests
 * Owner: Scenario 5 - Navigation and Links Validation
 *
 * Tests for:
 * - GitHub repository link
 * - README/Documentation link
 * - CTA button links
 * - Footer links
 * - Link validity (no broken links)
 * - Installation/setup link
 * - External link security attributes
 */

const { loadHTML, getByTestId, getAllLinks } = require('./test-utils');

describe('Navigation and Links Validation', () => {
  let doc;

  beforeAll(async () => {
    doc = await loadHTML();
  });

  describe('Test Case 1: GitHub repository link', () => {
    test('A link with href containing "github.com" and "mirdb" exists', () => {
      const links = getAllLinks(doc);
      const githubLinks = Array.from(links).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com') && href.toLowerCase().includes('mirdb');
      });

      expect(githubLinks.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 2: README/Documentation link', () => {
    test('A link to README.md or documentation section exists', () => {
      const links = getAllLinks(doc);
      const docLinks = Array.from(links).filter(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.includes('readme') ||
               href.includes('README') ||
               href.includes('#readme') ||
               text.includes('documentation') ||
               text.includes('docs');
      });

      expect(docLinks.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: Get Started CTA link', () => {
    test('A link with text "Get Started" has valid href', () => {
      const links = getAllLinks(doc);
      const getStartedLink = Array.from(links).find(link => {
        const text = link.textContent.trim();
        return text === 'Get Started' || text.includes('Get Started');
      });

      expect(getStartedLink).toBeDefined();
      expect(getStartedLink.getAttribute('href')).toBeTruthy();
      expect(getStartedLink.getAttribute('href')).not.toBe('#');
      expect(getStartedLink.getAttribute('href')).not.toBe('');
    });
  });

  describe('Test Case 4: Footer repository link', () => {
    test('Footer contains link to GitHub repository', () => {
      const footer = getByTestId(doc, 'footer') || doc.querySelector('footer');
      expect(footer).toBeTruthy();

      const footerLinks = footer.querySelectorAll('a');
      const githubLink = Array.from(footerLinks).find(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('github.com');
      });

      expect(githubLink).toBeDefined();
    });
  });

  describe('Test Case 5: Validate all anchor href attributes', () => {
    test('No anchor elements have empty or "#" only href', () => {
      const links = getAllLinks(doc);
      const invalidLinks = Array.from(links).filter(link => {
        const href = link.getAttribute('href');
        // Allow internal anchor links like #features, #quick-start
        if (href && href.startsWith('#') && href.length > 1) {
          return false;
        }
        // Flag empty, null, or "#" only hrefs
        return !href || href === '' || href === '#';
      });

      expect(invalidLinks.length).toBe(0);
    });
  });

  describe('Test Case 6: Installation/setup instructions link', () => {
    test('A link to setup or installation documentation exists', () => {
      const links = getAllLinks(doc);
      const installLinks = Array.from(links).filter(link => {
        const href = link.getAttribute('href') || '';
        const text = link.textContent.toLowerCase();
        return href.toLowerCase().includes('install') ||
               href.toLowerCase().includes('setup') ||
               href.includes('#installation') ||
               text.includes('install') ||
               text.includes('setup') ||
               text.includes('quick start') ||
               text.includes('get started');
      });

      expect(installLinks.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 7: External links have target="_blank"', () => {
    test('External links open in new tab for security', () => {
      const links = getAllLinks(doc);
      const externalLinks = Array.from(links).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });
  });

  describe('Test Case 8: External links have rel="noopener"', () => {
    test('External links have rel="noopener" or "noreferrer" for security', () => {
      const links = getAllLinks(doc);
      const externalLinks = Array.from(links).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http://') || href.startsWith('https://');
      });

      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel') || '';
        const hasSecurityRel = rel.includes('noopener') || rel.includes('noreferrer');
        expect(hasSecurityRel).toBe(true);
      });
    });
  });

  describe('Navigation structure', () => {
    test('Navigation element exists with proper structure', () => {
      const nav = doc.querySelector('nav');
      expect(nav).toBeTruthy();

      const navLinks = nav.querySelectorAll('a');
      expect(navLinks.length).toBeGreaterThan(0);
    });

    test('Navigation has aria-label for accessibility', () => {
      const nav = doc.querySelector('nav');
      expect(nav).toBeTruthy();
      expect(nav.getAttribute('aria-label')).toBeTruthy();
    });
  });

  describe('Internal navigation links', () => {
    test('Internal anchor links point to existing sections', () => {
      const links = getAllLinks(doc);
      const internalLinks = Array.from(links).filter(link => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('#') && href.length > 1;
      });

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const targetId = href.substring(1); // Remove the # prefix
        const targetElement = doc.getElementById(targetId);
        expect(targetElement).toBeTruthy();
      });
    });
  });
});
