/**
 * Security - External Link Safety Tests
 *
 * Verifies external links follow security best practices:
 * 1. All target="_blank" links have rel="noopener" or rel="noopener noreferrer"
 * 2. External links use HTTPS protocol
 * 3. No inline JavaScript in links (href="javascript:")
 *
 * These security measures protect against:
 * - Window.opener attacks (noopener)
 * - Man-in-the-middle attacks (HTTPS)
 * - XSS vulnerabilities (no javascript: protocol)
 */

describe('Security - External Link Safety', () => {
  // Helper to get all anchor elements
  const getAllLinks = () => Array.from(document.querySelectorAll('a'));

  // Helper to get external links (http/https)
  const getExternalLinks = () => getAllLinks().filter(link => {
    const href = link.getAttribute('href');
    return href && (href.startsWith('http://') || href.startsWith('https://'));
  });

  // Helper to get links with target="_blank"
  const getBlankTargetLinks = () => getAllLinks().filter(link => {
    return link.getAttribute('target') === '_blank';
  });

  describe('Test Case 1: target="_blank" links have rel="noopener"', () => {
    test('all target="_blank" links have rel="noopener" or rel="noopener noreferrer"', () => {
      const blankTargetLinks = getBlankTargetLinks();
      const vulnerableLinks = [];

      blankTargetLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        const hasNoopener = rel && rel.includes('noopener');

        if (!hasNoopener) {
          vulnerableLinks.push({
            href: link.getAttribute('href'),
            text: link.textContent.trim(),
            rel: rel || '(none)'
          });
        }
      });

      if (vulnerableLinks.length > 0) {
        const errorDetails = vulnerableLinks.map(
          l => `Link "${l.text}" (href="${l.href}") has rel="${l.rel}" - missing noopener`
        ).join('\n');
        fail(`Found ${vulnerableLinks.length} vulnerable target="_blank" links:\n${errorDetails}`);
      }

      expect(vulnerableLinks.length).toBe(0);
    });

    test('page has at least one external link with target="_blank"', () => {
      const blankTargetLinks = getBlankTargetLinks();
      expect(blankTargetLinks.length).toBeGreaterThan(0);
    });

    test('all external GitHub links have proper security attributes', () => {
      const githubLinks = getExternalLinks().filter(link =>
        link.getAttribute('href').includes('github.com')
      );

      expect(githubLinks.length).toBeGreaterThan(0);

      githubLinks.forEach(link => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        expect(target).toBe('_blank');
        expect(rel).toContain('noopener');
      });
    });

    test('noopener attribute prevents window.opener attacks', () => {
      const blankTargetLinks = getBlankTargetLinks();

      blankTargetLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        // rel must include 'noopener' to prevent window.opener access
        expect(rel).not.toBeNull();
        expect(rel).toContain('noopener');
      });
    });
  });

  describe('Test Case 2: External links use HTTPS protocol', () => {
    test('all external links use https:// protocol', () => {
      const externalLinks = getExternalLinks();
      const insecureLinks = [];

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href.startsWith('http://')) {
          insecureLinks.push({
            href: href,
            text: link.textContent.trim()
          });
        }
      });

      if (insecureLinks.length > 0) {
        const errorDetails = insecureLinks.map(
          l => `Link "${l.text}" uses insecure HTTP: ${l.href}`
        ).join('\n');
        fail(`Found ${insecureLinks.length} links using insecure HTTP:\n${errorDetails}`);
      }

      expect(insecureLinks.length).toBe(0);
    });

    test('page has multiple external HTTPS links', () => {
      const httpsLinks = getExternalLinks().filter(link =>
        link.getAttribute('href').startsWith('https://')
      );

      expect(httpsLinks.length).toBeGreaterThan(0);
    });

    test('all GitHub repository links use HTTPS', () => {
      const githubLinks = getExternalLinks().filter(link =>
        link.getAttribute('href').includes('github.com')
      );

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https:\/\//);
      });
    });

    test('documentation links use secure connections', () => {
      const docLinks = getExternalLinks().filter(link => {
        const href = link.getAttribute('href');
        const text = link.textContent.toLowerCase();
        return text.includes('doc') || href.includes('#readme');
      });

      docLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https:\/\//);
      });
    });
  });

  describe('Test Case 3: No inline JavaScript in links', () => {
    test('no href="javascript:" patterns in anchor elements', () => {
      const allLinks = getAllLinks();
      const jsLinks = [];

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.toLowerCase().startsWith('javascript:')) {
          jsLinks.push({
            href: href,
            text: link.textContent.trim()
          });
        }
      });

      if (jsLinks.length > 0) {
        const errorDetails = jsLinks.map(
          l => `Link "${l.text}" uses javascript: protocol: ${l.href}`
        ).join('\n');
        fail(`Found ${jsLinks.length} links with javascript: protocol (XSS risk):\n${errorDetails}`);
      }

      expect(jsLinks.length).toBe(0);
    });

    test('no javascript:void(0) links', () => {
      const allLinks = getAllLinks();
      const voidLinks = allLinks.filter(link => {
        const href = link.getAttribute('href');
        return href && href.toLowerCase().includes('javascript:void');
      });

      expect(voidLinks.length).toBe(0);
    });

    test('no onclick-only links without proper href', () => {
      const allLinks = getAllLinks();

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        const hasOnclick = link.hasAttribute('onclick');

        // If link has onclick but href is javascript:, that's a security risk
        if (hasOnclick && href) {
          expect(href.toLowerCase()).not.toMatch(/^javascript:/);
        }
      });
    });

    test('all links have valid non-JavaScript href values', () => {
      const allLinks = getAllLinks();

      allLinks.forEach(link => {
        const href = link.getAttribute('href');

        if (href) {
          // Valid patterns: #anchor, relative path, http(s), mailto, tel
          const isValidHref =
            href.startsWith('#') ||
            href.startsWith('/') ||
            href.startsWith('./') ||
            href.startsWith('../') ||
            href.startsWith('http://') ||
            href.startsWith('https://') ||
            href.startsWith('mailto:') ||
            href.startsWith('tel:');

          // Explicitly check it's not JavaScript
          const isJavaScript = href.toLowerCase().startsWith('javascript:');

          expect(isJavaScript).toBe(false);
          expect(isValidHref).toBe(true);
        }
      });
    });
  });

  describe('Comprehensive Security Audit', () => {
    test('security audit summary - all external links are properly secured', () => {
      const externalLinks = getExternalLinks();
      const securityIssues = [];

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');
        const text = link.textContent.trim();

        // Check HTTPS
        if (href.startsWith('http://')) {
          securityIssues.push(`[HTTP] "${text}" uses insecure HTTP`);
        }

        // Check noopener for target="_blank"
        if (target === '_blank') {
          if (!rel || !rel.includes('noopener')) {
            securityIssues.push(`[NOOPENER] "${text}" missing rel="noopener"`);
          }
        }
      });

      // Check for javascript: links
      getAllLinks().forEach(link => {
        const href = link.getAttribute('href');
        const text = link.textContent.trim();

        if (href && href.toLowerCase().startsWith('javascript:')) {
          securityIssues.push(`[XSS] "${text}" uses javascript: protocol`);
        }
      });

      if (securityIssues.length > 0) {
        fail(`Found ${securityIssues.length} security issues:\n${securityIssues.join('\n')}`);
      }

      expect(securityIssues.length).toBe(0);
    });

    test('all external links follow OWASP best practices', () => {
      const externalLinks = getExternalLinks();

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        // OWASP recommends HTTPS for all external links
        expect(href).toMatch(/^https:\/\//);

        // OWASP recommends noopener for target="_blank"
        if (target === '_blank') {
          expect(rel).toContain('noopener');
        }
      });
    });
  });
});
