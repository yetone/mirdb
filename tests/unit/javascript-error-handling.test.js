/**
 * Unit Tests for JavaScript Error Handling on MirDB Homepage
 *
 * Scenario: Verify the page loads without JavaScript errors and degrades gracefully
 *
 * Test Case 4: Verify script loading order
 * Expected: Scripts load in correct order without race conditions
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('JavaScript Error Handling - Script Loading Order', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('TC4: Script Loading Order', () => {
    test('scripts should be placed at the end of body for non-blocking load', () => {
      const body = document.body;
      const scripts = body.querySelectorAll('script[src]');

      // There should be external scripts
      expect(scripts.length).toBeGreaterThan(0);

      // Scripts should be at the end of body (after main content)
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');

      scripts.forEach((script) => {
        // Script should come after main content
        const scriptPosition = getElementPosition(script, body);
        const mainPosition = getElementPosition(main, body);
        const footerPosition = getElementPosition(footer, body);

        // Scripts should be after footer (at the very end of body)
        expect(scriptPosition).toBeGreaterThan(footerPosition);
      });
    });

    test('scripts should not be in the head section', () => {
      const head = document.head;
      const headScripts = head.querySelectorAll('script[src]');

      // No external scripts should be in head (which would block rendering)
      expect(headScripts.length).toBe(0);
    });

    test('Prism.js main script should load before component scripts', () => {
      const scripts = Array.from(document.querySelectorAll('script[src]'));

      const prismMainIndex = scripts.findIndex((s) =>
        s.src.includes('prism.min.js') || s.src.includes('prism.js')
      );

      const prismBashIndex = scripts.findIndex((s) =>
        s.src.includes('prism-bash')
      );

      // If both scripts exist, main should come before bash component
      if (prismMainIndex !== -1 && prismBashIndex !== -1) {
        expect(prismMainIndex).toBeLessThan(prismBashIndex);
      }
    });

    test('scripts should not have rendering-blocking configuration', () => {
      const scripts = document.querySelectorAll('script[src]');

      scripts.forEach((script) => {
        const src = script.src || script.getAttribute('src');

        // Scripts should either:
        // 1. Be in body (not head) - already tested above
        // 2. Have async or defer if they were in head
        // Since we verified no scripts in head, this validates the pattern

        // Verify no blocking script configurations
        // Scripts at end of body are inherently non-blocking
        const isInBody = script.parentElement === document.body;
        expect(isInBody).toBe(true);
      });
    });

    test('no inline scripts with potential race conditions', () => {
      // Check for inline scripts that might cause race conditions
      const inlineScripts = document.querySelectorAll('script:not([src])');

      // If there are inline scripts, they shouldn't reference Prism before it's loaded
      inlineScripts.forEach((script) => {
        const content = script.textContent || '';

        // If inline script references Prism, it should check if Prism exists
        if (content.includes('Prism')) {
          // It should have a guard or be after Prism script
          const scriptPosition = getElementPosition(script, document.body);
          const prismScript = document.querySelector('script[src*="prism.min.js"]');

          if (prismScript) {
            const prismPosition = getElementPosition(prismScript, document.body);
            // Inline script using Prism should be after Prism loads
            expect(scriptPosition).toBeGreaterThan(prismPosition);
          }
        }
      });
    });

    test('script sources should use HTTPS or relative paths', () => {
      const scripts = document.querySelectorAll('script[src]');

      scripts.forEach((script) => {
        const src = script.getAttribute('src');

        // Sources should be either:
        // - HTTPS URLs (secure)
        // - Relative paths (local files)
        if (src.startsWith('http')) {
          expect(src.startsWith('https://')).toBe(true);
        }
      });
    });

    test('CDN scripts should be loaded from reliable sources', () => {
      const scripts = document.querySelectorAll('script[src]');

      const cdnScripts = Array.from(scripts).filter((script) => {
        const src = script.getAttribute('src');
        return src && src.startsWith('https://');
      });

      // CDN scripts should be from known reliable CDNs
      const reliableCDNs = [
        'cdnjs.cloudflare.com',
        'cdn.jsdelivr.net',
        'unpkg.com'
      ];

      cdnScripts.forEach((script) => {
        const src = script.getAttribute('src');
        const isFromReliableCDN = reliableCDNs.some((cdn) => src.includes(cdn));
        expect(isFromReliableCDN).toBe(true);
      });
    });
  });

  describe('Graceful Degradation Configuration', () => {
    test('HTML structure should be complete without JavaScript', () => {
      // Core structure elements should exist in HTML (not generated by JS)
      expect(document.querySelector('.hero')).not.toBeNull();
      expect(document.querySelector('#features')).not.toBeNull();
      expect(document.querySelector('#quickstart')).not.toBeNull();
      expect(document.querySelector('.footer')).not.toBeNull();
    });

    test('all content should be in HTML, not dynamically generated', () => {
      // Key content elements
      const heading = document.querySelector('.hero h1');
      expect(heading).not.toBeNull();
      expect(heading.textContent).toBe('MirDB');

      const tagline = document.querySelector('.hero .tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.length).toBeGreaterThan(0);

      // Feature cards should be in HTML
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(4);

      // Code blocks should be in HTML
      const codeBlocks = document.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('navigation should work without JavaScript (anchor links)', () => {
      // Get Started button should link to quickstart section
      const getStartedBtn = document.querySelector('a.btn-primary');
      expect(getStartedBtn).not.toBeNull();
      expect(getStartedBtn.getAttribute('href')).toBe('#quickstart');

      // Target section should exist
      const quickstartSection = document.querySelector('#quickstart');
      expect(quickstartSection).not.toBeNull();
    });

    test('external links should have target and rel attributes for security', () => {
      const externalLinks = document.querySelectorAll('a[href^="https://"]');

      externalLinks.forEach((link) => {
        // External links should open in new tab with security attributes
        expect(link.getAttribute('target')).toBe('_blank');
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });
  });
});

/**
 * Helper function to get the position of an element within its parent
 */
function getElementPosition(element, parent) {
  if (!element || !parent) return -1;

  const children = Array.from(parent.children);
  return getNestedPosition(element, children, 0);
}

function getNestedPosition(element, elements, startPosition) {
  for (let i = 0; i < elements.length; i++) {
    if (elements[i] === element) {
      return startPosition + i;
    }
    if (elements[i].children.length > 0) {
      const nested = getNestedPosition(element, Array.from(elements[i].children), 0);
      if (nested !== -1) {
        return startPosition + i + nested / 1000; // Add fractional position for nested elements
      }
    }
  }
  return -1;
}
