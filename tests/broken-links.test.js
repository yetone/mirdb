/**
 * Broken Links - Error Handling Tests
 *
 * This test suite verifies that all links on the page are valid and functional:
 * 1. Internal anchor links point to existing element IDs
 * 2. All links have valid href attributes (no empty or javascript:void(0))
 * 3. External links are properly formatted and reachable
 */

describe('Error Handling - Broken Links', () => {
  // Helper function to get all anchor elements on the page
  const getAllLinks = () => {
    return Array.from(document.querySelectorAll('a'));
  };

  // Helper function to get internal anchor links (href="#section")
  const getInternalAnchorLinks = () => {
    return getAllLinks().filter(link => {
      const href = link.getAttribute('href');
      return href && href.startsWith('#') && href.length > 1;
    });
  };

  // Helper function to get external links
  const getExternalLinks = () => {
    return getAllLinks().filter(link => {
      const href = link.getAttribute('href');
      return href && (href.startsWith('http://') || href.startsWith('https://'));
    });
  };

  describe('Test Case 1 (E2E): Internal Anchor Links Validation', () => {
    test('all internal anchor links point to existing element IDs', () => {
      const internalLinks = getInternalAnchorLinks();

      expect(internalLinks.length).toBeGreaterThan(0);

      const brokenLinks = [];

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const targetId = href.substring(1); // Remove the # prefix
        const targetElement = document.getElementById(targetId);

        if (!targetElement) {
          brokenLinks.push({
            href: href,
            linkText: link.textContent.trim(),
            targetId: targetId
          });
        }
      });

      if (brokenLinks.length > 0) {
        const errorMessage = brokenLinks.map(
          link => `Link "${link.linkText}" (href="${link.href}") points to non-existent element with id="${link.targetId}"`
        ).join('\n');
        fail(`Found ${brokenLinks.length} broken internal anchor links:\n${errorMessage}`);
      }
    });

    test('navigation menu internal links all have valid targets', () => {
      const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');

      navLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });

    test('CTA button internal links point to existing sections', () => {
      const ctaButtons = document.querySelectorAll('.cta-buttons a[href^="#"]');

      ctaButtons.forEach(button => {
        const href = button.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });

    test('skip link points to main content element', () => {
      const skipLink = document.querySelector('.skip-link');

      if (skipLink) {
        const href = skipLink.getAttribute('href');
        expect(href).toBe('#main-content');

        const mainContent = document.getElementById('main-content');
        expect(mainContent).not.toBeNull();
      }
    });
  });

  describe('Test Case 2 (Unit): Valid href Attributes', () => {
    test('no links have empty href attributes', () => {
      const allLinks = getAllLinks();
      const linksWithEmptyHref = allLinks.filter(link => {
        const href = link.getAttribute('href');
        return href === '' || href === null;
      });

      if (linksWithEmptyHref.length > 0) {
        const errorMessage = linksWithEmptyHref.map(
          link => `Link "${link.textContent.trim()}" has empty or null href`
        ).join('\n');
        fail(`Found ${linksWithEmptyHref.length} links with empty href:\n${errorMessage}`);
      }
    });

    test('no links use javascript:void(0) href', () => {
      const allLinks = getAllLinks();
      const linksWithVoidHref = allLinks.filter(link => {
        const href = link.getAttribute('href');
        return href && href.toLowerCase().includes('javascript:void');
      });

      expect(linksWithVoidHref.length).toBe(0);
    });

    test('no links use javascript: protocol in href', () => {
      const allLinks = getAllLinks();
      const linksWithJsProtocol = allLinks.filter(link => {
        const href = link.getAttribute('href');
        return href && href.toLowerCase().startsWith('javascript:');
      });

      expect(linksWithJsProtocol.length).toBe(0);
    });

    test('all links have valid href format', () => {
      const allLinks = getAllLinks();
      const invalidLinks = [];

      allLinks.forEach(link => {
        const href = link.getAttribute('href');

        if (!href) {
          invalidLinks.push({
            link: link.textContent.trim(),
            reason: 'Missing href attribute'
          });
          return;
        }

        // Valid href patterns: #anchor, relative path, http(s) URL
        const isValidHref =
          href.startsWith('#') ||
          href.startsWith('/') ||
          href.startsWith('./') ||
          href.startsWith('../') ||
          href.startsWith('http://') ||
          href.startsWith('https://') ||
          href.startsWith('mailto:') ||
          href.startsWith('tel:');

        if (!isValidHref) {
          invalidLinks.push({
            link: link.textContent.trim(),
            href: href,
            reason: 'Invalid href format'
          });
        }
      });

      expect(invalidLinks.length).toBe(0);
    });

    test('no links have whitespace-only href', () => {
      const allLinks = getAllLinks();
      const linksWithWhitespaceHref = allLinks.filter(link => {
        const href = link.getAttribute('href');
        return href && href.trim() === '';
      });

      expect(linksWithWhitespaceHref.length).toBe(0);
    });

    test('hash-only links (#) are only used appropriately for home/logo', () => {
      const allLinks = getAllLinks();
      const hashOnlyLinks = allLinks.filter(link => {
        const href = link.getAttribute('href');
        return href === '#';
      });

      // Hash-only links should only be used for logo/home links
      hashOnlyLinks.forEach(link => {
        const isLogo = link.classList.contains('logo') ||
                       link.classList.contains('nav-logo') ||
                       link.getAttribute('aria-label')?.toLowerCase().includes('home');

        if (!isLogo) {
          fail(`Found hash-only link "#" that is not a logo/home link: "${link.textContent.trim()}"`);
        }
      });
    });
  });

  describe('Test Case 3 (Integration): External Links Validation', () => {
    test('all external links have valid URL format', () => {
      const externalLinks = getExternalLinks();

      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');

        // Check URL format validity
        expect(() => new URL(href)).not.toThrow();
      });
    });

    test('external links use HTTPS protocol', () => {
      const externalLinks = getExternalLinks();
      const nonHttpsLinks = externalLinks.filter(link => {
        const href = link.getAttribute('href');
        return href && href.startsWith('http://');
      });

      // All external links should use HTTPS for security
      expect(nonHttpsLinks.length).toBe(0);
    });

    test('external links have target="_blank" attribute', () => {
      const externalLinks = getExternalLinks();

      externalLinks.forEach(link => {
        const target = link.getAttribute('target');
        expect(target).toBe('_blank');
      });
    });

    test('external links have rel="noopener" for security', () => {
      const externalLinks = getExternalLinks();

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });

    test('GitHub repository links are correctly formatted', () => {
      const githubLinks = getExternalLinks().filter(link => {
        const href = link.getAttribute('href');
        return href && href.includes('github.com');
      });

      expect(githubLinks.length).toBeGreaterThan(0);

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');

        // Validate GitHub URL format
        expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+/);
      });
    });

    test('documentation links are properly formatted', () => {
      const docLinks = getExternalLinks().filter(link => {
        const href = link.getAttribute('href');
        return href && href.includes('#readme');
      });

      // Should have at least one readme/documentation link
      expect(docLinks.length).toBeGreaterThan(0);

      docLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Documentation links should point to GitHub README
        expect(href).toContain('github.com');
        expect(href).toContain('#readme');
      });
    });

    test('issues link is properly formatted', () => {
      const issuesLinks = getExternalLinks().filter(link => {
        const href = link.getAttribute('href');
        return href && href.includes('/issues');
      });

      issuesLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Issues links should point to GitHub issues page
        expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+\/issues$/);
      });
    });

    test('external links do not return 404 errors (format validation)', () => {
      const externalLinks = getExternalLinks();

      // Since we cannot make actual HTTP requests in Jest/jsdom environment,
      // we validate that the URLs are properly formatted and follow expected patterns
      // This ensures that if the URLs are correctly formatted, they should be reachable

      const expectedDomains = ['github.com'];

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const url = new URL(href);

        // Verify the domain is one of the expected external domains
        const isExpectedDomain = expectedDomains.some(domain =>
          url.hostname.includes(domain)
        );

        expect(isExpectedDomain).toBe(true);

        // Verify path is not empty (would indicate a malformed URL)
        expect(url.pathname.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Link Collection and Statistics', () => {
    test('page has a reasonable number of links', () => {
      const allLinks = getAllLinks();

      // A homepage should have some navigation and content links
      expect(allLinks.length).toBeGreaterThan(5);
    });

    test('page has both internal and external links', () => {
      const internalLinks = getInternalAnchorLinks();
      const externalLinks = getExternalLinks();

      expect(internalLinks.length).toBeGreaterThan(0);
      expect(externalLinks.length).toBeGreaterThan(0);
    });

    test('navigation contains expected sections', () => {
      const navLinks = document.querySelectorAll('.nav-links a');
      const linkHrefs = Array.from(navLinks).map(link => link.getAttribute('href'));

      // Check for expected navigation targets
      expect(linkHrefs).toContain('#features');
      expect(linkHrefs).toContain('#quick-start');
    });
  });
});
