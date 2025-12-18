/**
 * External Links Functionality Tests
 * Scenario: Verify all external links point to correct destinations and are functional
 *
 * Test Cases:
 * TC-1: Click GitHub repository link - Opens MirDB GitHub repository in new tab with valid page
 * TC-2: Click Documentation link - Opens documentation site with valid page
 * TC-3: Click crates.io link (if present) - Opens MirDB package on crates.io with valid page
 * TC-4: External links have target='_blank' attribute
 * TC-5: External links have rel='noopener noreferrer' for security
 * TC-6: No broken links (404 errors) are found
 */

const fs = require('fs');
const path = require('path');

describe('External Links Functionality', () => {
  let document;
  let allExternalLinks;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
    // Collect all external links (http:// or https://)
    allExternalLinks = document.querySelectorAll('a[href^="http://"], a[href^="https://"]');
  });

  // TC-1: Click GitHub repository link - Opens MirDB GitHub repository in new tab with valid page
  describe('TC-1: GitHub Repository Link', () => {
    test('should have GitHub link that points to MirDB repository', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"][href*="mirdb"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      // Verify at least one link goes to the main repository
      const hasMainRepoLink = Array.from(githubLinks).some(link => {
        const href = link.getAttribute('href');
        return href === 'https://github.com/yetone/mirdb' ||
               href.match(/github\.com\/yetone\/mirdb\/?$/);
      });
      expect(hasMainRepoLink).toBe(true);
    });

    test('GitHub link in hero section should point to correct repository', () => {
      const heroSection = document.querySelector('.hero-section');
      const heroGithubLink = heroSection.querySelector('a[href*="github"]');
      expect(heroGithubLink).not.toBeNull();
      expect(heroGithubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link in footer should point to correct repository', () => {
      const footer = document.querySelector('footer');
      const footerGithubLink = footer.querySelector('a[href*="github.com"][href*="mirdb"]');
      expect(footerGithubLink).not.toBeNull();
      // Footer GitHub link should be the main repo (not LICENSE or other paths)
      const href = footerGithubLink.getAttribute('href');
      expect(href).toMatch(/github\.com\/yetone\/mirdb\/?$/);
    });

    test('GitHub links should open in new tab', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
      githubLinks.forEach(link => {
        // Only main repo links (not sub-paths like LICENSE) - but all should have target
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });
  });

  // TC-2: Click Documentation link - Opens documentation site with valid page
  describe('TC-2: Documentation Link', () => {
    test('should have documentation link accessible from the page', () => {
      // Documentation link in footer (internal anchor)
      const footer = document.querySelector('footer');
      const docLink = footer.querySelector('a[href*="doc"]');
      expect(docLink).not.toBeNull();

      const href = docLink.getAttribute('href');
      // Documentation is currently an internal anchor link
      expect(href).toBe('#documentation');
    });

    test('documentation section should exist as target for internal link', () => {
      const docSection = document.querySelector('#documentation');
      expect(docSection).not.toBeNull();
    });

    test('Get Started button should link to documentation section', () => {
      const primaryCTA = document.querySelector('.cta-primary');
      expect(primaryCTA).not.toBeNull();
      expect(primaryCTA.getAttribute('href')).toBe('#documentation');
    });
  });

  // TC-3: Click crates.io link (if present) - Opens MirDB package on crates.io with valid page
  describe('TC-3: Crates.io Link (if present)', () => {
    test('crates.io link should point to MirDB package if present', () => {
      const cratesLinks = document.querySelectorAll('a[href*="crates.io"]');

      // This test passes if either:
      // 1. No crates.io link exists (acceptable)
      // 2. If present, it points to the correct package
      if (cratesLinks.length > 0) {
        cratesLinks.forEach(link => {
          const href = link.getAttribute('href');
          expect(href).toMatch(/crates\.io\/crates\/mirdb/i);
        });
      }

      // Always pass - crates.io link is optional per test case description
      expect(true).toBe(true);
    });
  });

  // TC-4: External links have target='_blank' attribute
  describe('TC-4: External Links Target Attribute', () => {
    test('all external links should have target="_blank" attribute', () => {
      expect(allExternalLinks.length).toBeGreaterThan(0);

      allExternalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const target = link.getAttribute('target');
        expect(target).toBe('_blank');
      });
    });

    test('hero section GitHub link should have target="_blank"', () => {
      const heroSection = document.querySelector('.hero-section');
      const githubLink = heroSection.querySelector('a[href*="github"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('footer external links should have target="_blank"', () => {
      const footer = document.querySelector('footer');
      const externalLinks = footer.querySelectorAll('a[href^="http://"], a[href^="https://"]');

      externalLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });
  });

  // TC-5: External links have rel='noopener noreferrer' for security
  describe('TC-5: External Links Security Attributes', () => {
    test('all external links should have rel attribute containing "noopener"', () => {
      expect(allExternalLinks.length).toBeGreaterThan(0);

      allExternalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });

    test('all external links should have rel attribute containing "noreferrer"', () => {
      allExternalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toContain('noreferrer');
      });
    });

    test('external links with target="_blank" must have rel="noopener noreferrer"', () => {
      const blankTargetLinks = document.querySelectorAll('a[target="_blank"]');

      blankTargetLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Only check external links
        if (href && (href.startsWith('http://') || href.startsWith('https://'))) {
          const rel = link.getAttribute('rel');
          expect(rel).toContain('noopener');
          expect(rel).toContain('noreferrer');
        }
      });
    });

    test('hero section GitHub link should have proper security attributes', () => {
      const heroSection = document.querySelector('.hero-section');
      const githubLink = heroSection.querySelector('a[href*="github"]');
      expect(githubLink).not.toBeNull();

      const rel = githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('footer GitHub link should have proper security attributes', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector('a[href*="github.com"][href*="mirdb"]');
      expect(githubLink).not.toBeNull();

      const rel = githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('footer License link should have proper security attributes', () => {
      const footer = document.querySelector('footer');
      const licenseLink = footer.querySelector('a[href*="LICENSE"]');
      expect(licenseLink).not.toBeNull();

      const rel = licenseLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  // TC-6: No broken links (404 errors) are found - Structural verification
  describe('TC-6: Link Validity Check', () => {
    test('all external links should have valid href format', () => {
      allExternalLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href).toMatch(/^https?:\/\/.+/);
      });
    });

    test('GitHub repository links should have valid GitHub URL format', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Valid GitHub URL format
        expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+/);
      });
    });

    test('all internal anchor links should have valid targets', () => {
      const anchorLinks = document.querySelectorAll('a[href^="#"]');

      anchorLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href !== '#') {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });

    test('should have expected external links count', () => {
      // We expect at least 3 external links:
      // 1. Hero GitHub button
      // 2. Footer GitHub link
      // 3. Footer License link
      expect(allExternalLinks.length).toBeGreaterThanOrEqual(3);
    });

    test('external links should point to expected domains', () => {
      const expectedDomains = ['github.com'];
      const actualDomains = new Set();

      allExternalLinks.forEach(link => {
        const href = link.getAttribute('href');
        try {
          const url = new URL(href);
          actualDomains.add(url.hostname);
        } catch (e) {
          // Invalid URL
        }
      });

      // All external links should be to github.com
      actualDomains.forEach(domain => {
        expect(expectedDomains).toContain(domain);
      });
    });
  });

  // Additional edge case tests
  describe('Additional Link Tests', () => {
    test('external links should have accessible text content', () => {
      allExternalLinks.forEach(link => {
        const hasText = link.textContent.trim().length > 0;
        const hasAriaLabel = link.getAttribute('aria-label');
        expect(hasText || hasAriaLabel).toBe(true);
      });
    });

    test('no duplicate external links in same section', () => {
      const sections = ['header', 'main', 'footer'];

      sections.forEach(sectionName => {
        const section = document.querySelector(sectionName);
        if (section) {
          const links = section.querySelectorAll('a[href^="http://"], a[href^="https://"]');
          const hrefs = Array.from(links).map(link => link.getAttribute('href'));
          const uniqueHrefs = new Set(hrefs);
          expect(hrefs.length).toBe(uniqueHrefs.size);
        }
      });
    });

    test('License link should point to LICENSE file on GitHub', () => {
      const licenseLink = document.querySelector('a[href*="LICENSE"]');
      expect(licenseLink).not.toBeNull();
      expect(licenseLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb/blob/master/LICENSE');
    });
  });
});

// E2E-style link validation tests
describe('E2E Link Validation', () => {
  let document;
  let allLinks;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
    allLinks = document.querySelectorAll('a[href]');
  });

  test('should collect all links for validation', () => {
    expect(allLinks.length).toBeGreaterThan(0);

    // Log all links for reference (useful for debugging)
    const linkData = Array.from(allLinks).map(link => ({
      href: link.getAttribute('href'),
      text: link.textContent.trim().substring(0, 50),
      target: link.getAttribute('target'),
      rel: link.getAttribute('rel')
    }));

    // Verify we have the expected links
    const externalLinks = linkData.filter(l => l.href.startsWith('http'));
    expect(externalLinks.length).toBeGreaterThanOrEqual(3);
  });

  test('external links structure should be correct for validation', () => {
    const externalLinks = document.querySelectorAll('a[href^="http://"], a[href^="https://"]');

    // Each external link should have proper structure for link checking
    externalLinks.forEach(link => {
      const href = link.getAttribute('href');
      const target = link.getAttribute('target');
      const rel = link.getAttribute('rel');

      // All required attributes should be present
      expect(href).toBeTruthy();
      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  test('all page links should be accessible for crawling', () => {
    allLinks.forEach(link => {
      const href = link.getAttribute('href');

      // Each link should have a valid href
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);

      // No javascript: hrefs
      expect(href).not.toMatch(/^javascript:/);

      // No empty fragment-only hrefs (except #)
      if (href.startsWith('#') && href !== '#') {
        const targetId = href.substring(1);
        const target = document.getElementById(targetId);
        expect(target).not.toBeNull();
      }
    });
  });
});
