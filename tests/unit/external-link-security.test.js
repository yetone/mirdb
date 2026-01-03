/**
 * Unit tests for External Link Security
 * Scenario: Verify external links have proper security attributes to prevent tabnapping
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('External Link Security', () => {
  let document;
  let html;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  // Test Case 1: Check external links for rel attribute
  describe('TC1: External links have proper rel attributes', () => {
    test('all external links with target="_blank" have rel="noopener" or rel="noreferrer"', () => {
      const externalLinksWithBlank = document.querySelectorAll('a[target="_blank"]');
      const insecureLinks = [];

      externalLinksWithBlank.forEach((link) => {
        const rel = link.getAttribute('rel');
        const href = link.getAttribute('href');

        // Check if rel contains noopener or noreferrer
        const hasNoopener = rel && rel.includes('noopener');
        const hasNoreferrer = rel && rel.includes('noreferrer');

        if (!hasNoopener && !hasNoreferrer) {
          insecureLinks.push({
            href: href,
            rel: rel || 'missing',
            element: link.outerHTML,
          });
        }
      });

      expect(insecureLinks).toEqual([]);
    });

    test('all HTTP/HTTPS links have target="_blank" and rel="noopener"', () => {
      const httpLinks = document.querySelectorAll('a[href^="http"]');
      const violations = [];

      httpLinks.forEach((link) => {
        const href = link.getAttribute('href');
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        // External links should open in new tab with proper security
        if (target !== '_blank') {
          violations.push({
            issue: 'missing target="_blank"',
            href: href,
          });
        }

        const hasNoopener = rel && rel.includes('noopener');
        const hasNoreferrer = rel && rel.includes('noreferrer');

        if (!hasNoopener && !hasNoreferrer) {
          violations.push({
            issue: 'missing rel="noopener" or rel="noreferrer"',
            href: href,
            rel: rel || 'missing',
          });
        }
      });

      expect(violations).toEqual([]);
    });

    test('GitHub links in hero section have security attributes', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      const githubLinks = heroSection.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      githubLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });

    test('documentation links have security attributes', () => {
      const docLinks = document.querySelectorAll('.docs-link');

      docLinks.forEach((link) => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        expect(target).toBe('_blank');
        expect(rel).toContain('noopener');
      });
    });

    test('footer external links have security attributes', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const externalLinks = footer.querySelectorAll('a[href^="http"]');
      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        expect(target).toBe('_blank');
        expect(rel).toContain('noopener');
      });
    });
  });

  // Test Case 2: Check for mixed content (covered by checking HTTP vs HTTPS)
  describe('TC2: No mixed content issues', () => {
    test('no external links use HTTP protocol (should use HTTPS)', () => {
      const httpOnlyLinks = [];
      const allLinks = document.querySelectorAll('a[href^="http://"]');

      allLinks.forEach((link) => {
        httpOnlyLinks.push(link.getAttribute('href'));
      });

      expect(httpOnlyLinks).toEqual([]);
    });

    test('no scripts loaded via HTTP', () => {
      const scripts = document.querySelectorAll('script[src^="http://"]');
      expect(scripts.length).toBe(0);
    });

    test('no stylesheets loaded via HTTP', () => {
      const stylesheets = document.querySelectorAll('link[rel="stylesheet"][href^="http://"]');
      expect(stylesheets.length).toBe(0);
    });

    test('all external resources use HTTPS', () => {
      // Check all external script sources
      const scripts = document.querySelectorAll('script[src^="http"]');
      scripts.forEach((script) => {
        const src = script.getAttribute('src');
        expect(src).toMatch(/^https:\/\//);
      });

      // Check all external stylesheet links
      const stylesheets = document.querySelectorAll('link[rel="stylesheet"][href^="http"]');
      stylesheets.forEach((stylesheet) => {
        const href = stylesheet.getAttribute('href');
        expect(href).toMatch(/^https:\/\//);
      });

      // Check all images with external sources
      const images = document.querySelectorAll('img[src^="http"]');
      images.forEach((img) => {
        const src = img.getAttribute('src');
        expect(src).toMatch(/^https:\/\//);
      });
    });

    test('meta tags with URLs use HTTPS', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      const ogUrl = document.querySelector('meta[property="og:url"]');
      const canonical = document.querySelector('link[rel="canonical"]');

      if (ogImage) {
        const content = ogImage.getAttribute('content');
        expect(content).toMatch(/^https:\/\//);
      }

      if (ogUrl) {
        const content = ogUrl.getAttribute('content');
        expect(content).toMatch(/^https:\/\//);
      }

      if (canonical) {
        const href = canonical.getAttribute('href');
        expect(href).toMatch(/^https:\/\//);
      }
    });
  });

  // Test Case 3: Check for inline event handlers
  describe('TC3: No inline event handlers', () => {
    test('no inline onclick handlers in HTML', () => {
      const elementsWithOnclick = document.querySelectorAll('[onclick]');
      expect(elementsWithOnclick.length).toBe(0);
    });

    test('no inline onmouseover handlers in HTML', () => {
      const elementsWithOnmouseover = document.querySelectorAll('[onmouseover]');
      expect(elementsWithOnmouseover.length).toBe(0);
    });

    test('no inline onmouseout handlers in HTML', () => {
      const elementsWithOnmouseout = document.querySelectorAll('[onmouseout]');
      expect(elementsWithOnmouseout.length).toBe(0);
    });

    test('no inline onfocus handlers in HTML', () => {
      const elementsWithOnfocus = document.querySelectorAll('[onfocus]');
      expect(elementsWithOnfocus.length).toBe(0);
    });

    test('no inline onblur handlers in HTML', () => {
      const elementsWithOnblur = document.querySelectorAll('[onblur]');
      expect(elementsWithOnblur.length).toBe(0);
    });

    test('no inline onload handlers in HTML', () => {
      const elementsWithOnload = document.querySelectorAll('[onload]');
      expect(elementsWithOnload.length).toBe(0);
    });

    test('no inline onerror handlers in HTML', () => {
      const elementsWithOnerror = document.querySelectorAll('[onerror]');
      expect(elementsWithOnerror.length).toBe(0);
    });

    test('no inline onsubmit handlers in HTML', () => {
      const elementsWithOnsubmit = document.querySelectorAll('[onsubmit]');
      expect(elementsWithOnsubmit.length).toBe(0);
    });

    test('no inline onchange handlers in HTML', () => {
      const elementsWithOnchange = document.querySelectorAll('[onchange]');
      expect(elementsWithOnchange.length).toBe(0);
    });

    test('no inline onkeydown handlers in HTML', () => {
      const elementsWithOnkeydown = document.querySelectorAll('[onkeydown]');
      expect(elementsWithOnkeydown.length).toBe(0);
    });

    test('no inline onkeyup handlers in HTML', () => {
      const elementsWithOnkeyup = document.querySelectorAll('[onkeyup]');
      expect(elementsWithOnkeyup.length).toBe(0);
    });

    test('no inline onkeypress handlers in HTML', () => {
      const elementsWithOnkeypress = document.querySelectorAll('[onkeypress]');
      expect(elementsWithOnkeypress.length).toBe(0);
    });

    test('comprehensive check for all common inline event handlers', () => {
      const inlineEventHandlers = [
        'onclick', 'ondblclick', 'onmousedown', 'onmouseup', 'onmouseover',
        'onmouseout', 'onmousemove', 'onmouseenter', 'onmouseleave',
        'onfocus', 'onblur', 'onchange', 'onsubmit', 'onreset', 'onselect',
        'onkeydown', 'onkeyup', 'onkeypress',
        'onload', 'onerror', 'onunload', 'onbeforeunload', 'onresize', 'onscroll',
        'ondrag', 'ondragstart', 'ondragend', 'ondragenter', 'ondragleave', 'ondragover', 'ondrop',
        'ontouchstart', 'ontouchmove', 'ontouchend', 'ontouchcancel',
        'oncontextmenu', 'oncopy', 'oncut', 'onpaste'
      ];

      const violations = [];

      inlineEventHandlers.forEach((handler) => {
        const elements = document.querySelectorAll(`[${handler}]`);
        if (elements.length > 0) {
          elements.forEach((el) => {
            violations.push({
              handler: handler,
              element: el.tagName,
              outerHTML: el.outerHTML.substring(0, 100),
            });
          });
        }
      });

      expect(violations).toEqual([]);
    });

    test('no javascript: URLs in href attributes', () => {
      const javascriptLinks = document.querySelectorAll('a[href^="javascript:"]');
      expect(javascriptLinks.length).toBe(0);
    });
  });

  // Additional security checks
  describe('Additional security validations', () => {
    test('external CDN resources use HTTPS', () => {
      const cdnResources = document.querySelectorAll(
        'script[src*="cdnjs"], link[href*="cdnjs"]'
      );

      cdnResources.forEach((resource) => {
        const url = resource.getAttribute('src') || resource.getAttribute('href');
        expect(url).toMatch(/^https:\/\//);
      });
    });

    test('page has proper canonical URL with HTTPS', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();

      const href = canonical.getAttribute('href');
      expect(href).toMatch(/^https:\/\//);
    });

    test('all clickable links have proper security configuration', () => {
      const allExternalLinks = document.querySelectorAll('a[href^="https://"]');

      allExternalLinks.forEach((link) => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        // If it opens in new tab, must have security attributes
        if (target === '_blank') {
          const hasNoopener = rel && rel.includes('noopener');
          const hasNoreferrer = rel && rel.includes('noreferrer');
          expect(hasNoopener || hasNoreferrer).toBe(true);
        }
      });
    });
  });
});
