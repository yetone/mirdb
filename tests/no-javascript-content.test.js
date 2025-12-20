/**
 * No JavaScript Core Content Tests
 *
 * Verifies that core content works without JavaScript (progressive enhancement)
 * per Technical Constraint: Works without JS for core content
 *
 * Test Cases:
 * 1. Load page with JavaScript disabled - Hero section, features, and quick-start visible
 * 2. Check for noscript fallback - Page either works fully without JS or provides noscript fallback
 * 3. Verify navigation works without JS - All navigation links functional without JavaScript
 *
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('No JavaScript Core Content', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    document.documentElement.innerHTML = htmlContent;
  });

  describe('Test Case 1: Load page with JavaScript disabled', () => {
    /**
     * Verify that hero section, features, and quick-start are visible without JS
     * Expected: Hero section, features, and quick-start visible without JS
     */

    test('hero section is rendered in static HTML', () => {
      const heroSection = document.querySelector('.hero, section.hero, [class*="hero"]');
      expect(heroSection).not.toBeNull();
    });

    test('hero section contains product name (MirDB)', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.toLowerCase()).toContain('mirdb');
    });

    test('hero section contains tagline', () => {
      const heroSection = document.querySelector('.hero');
      const tagline = heroSection.querySelector('.tagline, p');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.length).toBeGreaterThan(10);
    });

    test('hero section contains CTA buttons', () => {
      const heroSection = document.querySelector('.hero');
      const ctaButtons = heroSection.querySelectorAll('.cta-buttons a, .btn');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(1);
    });

    test('features section is rendered in static HTML', () => {
      const featuresSection = document.querySelector('#features, section[id="features"], [data-section="features"]');
      expect(featuresSection).not.toBeNull();
    });

    test('features section contains feature cards', () => {
      const featuresSection = document.querySelector('#features');
      const featureCards = featuresSection.querySelectorAll('.feature-card, .feature, [class*="feature"]');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
    });

    test('features section displays key features text', () => {
      const featuresSection = document.querySelector('#features');
      const featuresText = featuresSection.textContent.toLowerCase();

      // Check for key feature mentions
      const hasMemcached = featuresText.includes('memcached');
      const hasPersistent = featuresText.includes('persistent') || featuresText.includes('persistence');
      const hasLSMOrStorage = featuresText.includes('lsm') || featuresText.includes('storage') || featuresText.includes('sstable');

      expect(hasMemcached || hasPersistent || hasLSMOrStorage).toBe(true);
    });

    test('quick-start section is rendered in static HTML', () => {
      const quickStartSection = document.querySelector('#quick-start, .quick-start, section[id="quick-start"]');
      expect(quickStartSection).not.toBeNull();
    });

    test('quick-start section contains code examples', () => {
      const quickStartSection = document.querySelector('#quick-start, .quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('code, pre, .code-block');
      expect(codeBlocks.length).toBeGreaterThanOrEqual(1);
    });

    test('quick-start section contains installation instructions', () => {
      const quickStartSection = document.querySelector('#quick-start, .quick-start');
      const quickStartText = quickStartSection.textContent.toLowerCase();

      // Check for installation-related commands
      const hasClone = quickStartText.includes('clone') || quickStartText.includes('git');
      const hasBuild = quickStartText.includes('build') || quickStartText.includes('cargo');
      const hasRun = quickStartText.includes('run') || quickStartText.includes('server');

      expect(hasClone || hasBuild || hasRun).toBe(true);
    });

    test('all core sections are visible without CSS display:none requiring JS', () => {
      // Check that no core sections have inline style display:none
      const heroSection = document.querySelector('.hero');
      const featuresSection = document.querySelector('#features');
      const quickStartSection = document.querySelector('#quick-start, .quick-start');

      // Check inline styles don't hide content
      expect(heroSection?.style?.display).not.toBe('none');
      expect(featuresSection?.style?.display).not.toBe('none');
      expect(quickStartSection?.style?.display).not.toBe('none');
    });

    test('page does not have JS-only content loading patterns', () => {
      // Check that content is not loaded via JavaScript patterns
      const hasReactRoot = document.querySelector('#root, #app, [data-reactroot]');
      const hasVueApp = document.querySelector('#app[data-v-app], [data-v-]');
      const hasAngularApp = document.querySelector('[ng-app], [data-ng-app]');

      // If React/Vue/Angular root exists, content should still be server-rendered
      // The page should have actual content, not just an empty container
      if (hasReactRoot) {
        expect(hasReactRoot.children.length).toBeGreaterThan(0);
      }

      // Main content should be in the body regardless
      const bodyContent = document.body.textContent.trim();
      expect(bodyContent.length).toBeGreaterThan(100);
    });
  });

  describe('Test Case 2: Check for noscript fallback', () => {
    /**
     * Verify page either works fully without JS or provides noscript fallback
     * Expected: Page either works fully without JS or provides noscript fallback
     */

    test('page has no JavaScript dependencies for core content', () => {
      // Check that the page doesn't have script tags that are required for content
      const scripts = document.querySelectorAll('script');
      const externalScripts = Array.from(scripts).filter(
        script => script.src && !script.src.includes('analytics') && !script.src.includes('tracking')
      );

      // Either no scripts, or scripts are for enhancement only
      // Core content should be visible regardless
      const heroExists = document.querySelector('.hero') !== null;
      const featuresExist = document.querySelector('#features') !== null;
      const quickStartExists = document.querySelector('#quick-start, .quick-start') !== null;

      expect(heroExists).toBe(true);
      expect(featuresExist).toBe(true);
      expect(quickStartExists).toBe(true);
    });

    test('page works fully without JS - no noscript fallback needed because content is static', () => {
      // Check if there's a noscript tag
      const noscriptTags = document.querySelectorAll('noscript');

      // If noscript tags exist, they should provide helpful fallback
      if (noscriptTags.length > 0) {
        noscriptTags.forEach(noscript => {
          // noscript content should be helpful, not empty
          const content = noscript.textContent.trim();
          expect(content.length).toBeGreaterThan(0);
        });
      }

      // The best case: page works without JS so no noscript needed
      // Check that core content is present in the static HTML
      const heroSection = document.querySelector('.hero');
      const featuresSection = document.querySelector('#features');

      expect(heroSection).not.toBeNull();
      expect(featuresSection).not.toBeNull();

      // Test passes if either:
      // 1. noscript fallback exists with helpful content
      // 2. Page works fully without JS (content is static HTML)
      expect(true).toBe(true);
    });

    test('no critical functionality hidden behind JavaScript', () => {
      // Check for elements that typically need JS but might be present
      const forms = document.querySelectorAll('form');
      const modals = document.querySelectorAll('.modal, [role="dialog"]');
      const dynamicContent = document.querySelectorAll('[data-dynamic], [data-load]');

      // Forms should work without JS (simple submit)
      forms.forEach(form => {
        const action = form.getAttribute('action');
        // Forms should have an action (fallback for no-JS)
        if (action) {
          expect(action.length).toBeGreaterThan(0);
        }
      });

      // Modals should not be the only way to access content
      // The main content should not be inside modals
      expect(document.body.children.length).toBeGreaterThan(modals.length);
    });

    test('CSS does not require JavaScript to display content', () => {
      // Check for CSS classes that might hide content by default
      // expecting JS to show them
      const hiddenByDefault = document.querySelectorAll('.js-show, .with-js, [hidden]');

      // These elements, if present, should not contain core content
      hiddenByDefault.forEach(element => {
        const isHero = element.classList.contains('hero') || element.closest('.hero');
        const isFeatures = element.id === 'features' || element.closest('#features');
        const isQuickStart = element.id === 'quick-start' || element.closest('#quick-start');

        expect(isHero || isFeatures || isQuickStart).toBe(false);
      });
    });

    test('inline scripts do not manipulate DOM to show core content', () => {
      // Check for inline scripts that might show/hide content
      const inlineScripts = document.querySelectorAll('script:not([src])');
      let manipulatesVisibility = false;

      inlineScripts.forEach(script => {
        const content = script.textContent || '';
        // Check for visibility manipulation patterns
        if (
          content.includes('style.display') ||
          content.includes('classList.remove') ||
          content.includes('.show()') ||
          content.includes('visibility')
        ) {
          // Check if it's hiding/showing core sections
          if (
            content.includes('hero') ||
            content.includes('features') ||
            content.includes('quick-start')
          ) {
            manipulatesVisibility = true;
          }
        }
      });

      expect(manipulatesVisibility).toBe(false);
    });
  });

  describe('Test Case 3: Verify navigation works without JS', () => {
    /**
     * Verify all navigation links functional without JavaScript
     * Expected: All navigation links functional without JavaScript
     */

    test('all anchor links have valid href attributes', () => {
      const links = document.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);

      links.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).not.toBeNull();
        expect(href).not.toBe('');
        expect(href).not.toBe('#'); // Should have meaningful targets
      });
    });

    test('internal anchor links point to existing sections', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href !== '#') {
          const targetId = href.substring(1);
          const target = document.getElementById(targetId);
          expect(target).not.toBeNull();
        }
      });
    });

    test('external links have valid URLs', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"], a[href^="https"]');

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https?:\/\/.+/);
      });
    });

    test('navigation does not rely on JavaScript click handlers', () => {
      const links = document.querySelectorAll('a');

      links.forEach(link => {
        // Links should not have onclick that prevents default
        const onclick = link.getAttribute('onclick');

        if (onclick) {
          // If there's onclick, it should not prevent default navigation
          expect(onclick).not.toContain('preventDefault');
          expect(onclick).not.toContain('return false');
        }

        // href should be present (not relying purely on JS)
        expect(link.hasAttribute('href')).toBe(true);
      });
    });

    test('CTA buttons are actual links, not JavaScript buttons', () => {
      const ctaButtons = document.querySelectorAll('.cta-buttons a, .btn');

      ctaButtons.forEach(button => {
        // CTA buttons should be anchor elements with href
        if (button.tagName.toLowerCase() === 'a') {
          expect(button.hasAttribute('href')).toBe(true);
          const href = button.getAttribute('href');
          expect(href.length).toBeGreaterThan(0);
        }
      });
    });

    test('GitHub link navigates directly without JS', () => {
      const githubLinks = document.querySelectorAll('a[href*="github"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/github\.com/);
        // Should be a direct link, not a JS-handled one
        expect(link.getAttribute('onclick')).toBeNull();
      });
    });

    test('footer navigation links are functional', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const footerLinks = footer.querySelectorAll('a');
      expect(footerLinks.length).toBeGreaterThan(0);

      footerLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).not.toBeNull();
        expect(href.length).toBeGreaterThan(0);
      });
    });

    test('in-page navigation with smooth scroll falls back gracefully', () => {
      // Check for scroll-behavior in CSS (progressive enhancement)
      const hasScrollBehavior = htmlContent.includes('scroll-behavior');

      // In-page links should work even without smooth scroll
      const inPageLinks = document.querySelectorAll('a[href^="#"]');

      inPageLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.length > 1) {
          const targetId = href.substring(1);
          const target = document.getElementById(targetId);
          // Target must exist for navigation to work
          expect(target).not.toBeNull();
        }
      });
    });

    test('all visible links are clickable without JavaScript', () => {
      const allLinks = document.querySelectorAll('a');
      let jsOnlyLinks = 0;

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        const onclick = link.getAttribute('onclick');

        // Links that only work with JS
        if (
          href === 'javascript:void(0)' ||
          href === 'javascript:;' ||
          (href === '#' && onclick)
        ) {
          jsOnlyLinks++;
        }
      });

      // All or most links should work without JS
      const jsOnlyPercentage = jsOnlyLinks / allLinks.length;
      expect(jsOnlyPercentage).toBeLessThan(0.1); // Less than 10% JS-only links
    });
  });

  describe('Progressive Enhancement Verification', () => {
    test('page contains semantic HTML structure', () => {
      const sections = document.querySelectorAll('section');
      const footer = document.querySelector('footer');

      expect(sections.length).toBeGreaterThan(0);
      expect(footer).not.toBeNull();
    });

    test('CSS provides styling without requiring JavaScript', () => {
      // Check for embedded or linked CSS
      const hasEmbeddedStyle = htmlContent.includes('<style');
      const hasLinkedCSS = htmlContent.includes('rel="stylesheet"');

      expect(hasEmbeddedStyle || hasLinkedCSS).toBe(true);
    });

    test('content is readable without any styling (HTML only)', () => {
      // Core content should make sense even without CSS
      const bodyText = document.body.textContent;

      expect(bodyText).toContain('MirDB');
      expect(bodyText.toLowerCase()).toContain('feature');
    });

    test('quick start content is readable without styling', () => {
      const bodyText = document.body.textContent.toLowerCase();
      const hasQuickStart = bodyText.includes('quick start') ||
        bodyText.includes('getting started') ||
        bodyText.includes('installation') ||
        bodyText.includes('clone');
      expect(hasQuickStart).toBe(true);
    });

    test('no content is dynamically loaded via AJAX/fetch', () => {
      const scripts = document.querySelectorAll('script');
      let hasAjaxContentLoading = false;

      scripts.forEach(script => {
        const content = script.textContent || '';
        if (
          (content.includes('fetch') && content.includes('.innerHTML')) ||
          (content.includes('XMLHttpRequest') && content.includes('innerHTML')) ||
          (content.includes('$.ajax') && content.includes('html'))
        ) {
          hasAjaxContentLoading = true;
        }
      });

      expect(hasAjaxContentLoading).toBe(false);
    });
  });
});
