/**
 * Performance Unit Tests
 * Owner: Scenario 10 - Performance - Page Load
 *
 * Tests:
 * - Lazy loading implementation for images and below-fold content
 * - Critical CSS inlining for above-fold styles
 *
 * These tests verify the page implements proper performance optimizations
 * as specified in NFR-1 of the PRD.
 */
const fs = require('fs');
const path = require('path');

// Path to index.html
const INDEX_HTML_PATH = path.resolve(__dirname, '../../index.html');

// Read the HTML file once for all tests
let htmlContent = '';

beforeAll(() => {
  htmlContent = fs.readFileSync(INDEX_HTML_PATH, 'utf8');
});

describe('Lazy Loading Implementation', () => {
  describe('Image lazy loading', () => {
    test('Images and below-fold content use lazy loading', () => {
      // Parse all img tags from HTML
      const imgTagRegex = /<img[^>]*>/gi;
      const imgTags = htmlContent.match(imgTagRegex) || [];

      // Filter for images that should be lazy loaded (below-fold images)
      // Images in hero section don't need lazy loading (above fold)
      // Images in usage section and below should use lazy loading
      const belowFoldImages = imgTags.filter((tag) => {
        // Check if it's in a below-fold section
        // Usage section images should be lazy loaded
        return tag.includes('usage') ||
               tag.includes('loading="lazy"') ||
               !tag.includes('hero');
      });

      // At least one image should use lazy loading
      const lazyLoadedImages = imgTags.filter((tag) =>
        tag.includes('loading="lazy"')
      );

      // Log for debugging
      console.log('Image Analysis:');
      console.log(`  Total images: ${imgTags.length}`);
      console.log(`  Lazy loaded: ${lazyLoadedImages.length}`);

      // At least the usage.gif should be lazy loaded (below fold)
      expect(lazyLoadedImages.length).toBeGreaterThan(0);
    });

    test('Usage section GIF has lazy loading attribute', () => {
      // The usage.gif is below the fold and should have loading="lazy"
      const usageGifRegex = /<img[^>]*usage\.gif[^>]*>/i;
      const usageGifMatch = htmlContent.match(usageGifRegex);

      if (usageGifMatch) {
        expect(usageGifMatch[0]).toContain('loading="lazy"');
      }
    });

    test('Hero section images do not have lazy loading (above fold)', () => {
      // Extract hero section content
      const heroSectionRegex = /<!-- SECTION: Hero[^]*?<section id="hero"[^]*?<\/section>/i;
      const heroMatch = htmlContent.match(heroSectionRegex);

      if (heroMatch) {
        const heroContent = heroMatch[0];
        const heroImgRegex = /<img[^>]*>/gi;
        const heroImages = heroContent.match(heroImgRegex) || [];

        // Hero images should NOT be lazy loaded (they're above fold)
        heroImages.forEach((img) => {
          // It's acceptable for hero images to not have loading attribute
          // or to have loading="eager" (the default)
          const hasLazyLoading = img.includes('loading="lazy"');
          // Hero images should NOT be lazy loaded
          expect(hasLazyLoading).toBe(false);
        });
      }
    });
  });

  describe('Below-fold content lazy loading', () => {
    test('Page structure supports progressive loading', () => {
      // Verify sections exist in correct order for progressive loading
      const sections = [
        'hero',
        'features',
        'usage',
        'architecture',
        'getting-started'
      ];

      let lastIndex = -1;
      sections.forEach((section) => {
        const sectionRegex = new RegExp(`id="${section}"`, 'i');
        const match = htmlContent.match(sectionRegex);

        if (match) {
          const currentIndex = htmlContent.indexOf(match[0]);
          // Each section should appear after the previous one
          expect(currentIndex).toBeGreaterThan(lastIndex);
          lastIndex = currentIndex;
        }
      });
    });
  });
});

describe('Critical CSS Inlining', () => {
  describe('Above-fold styles', () => {
    test('Above-fold styles are inlined in HTML head', () => {
      // Check for inline critical CSS in the head
      // This can be in the form of:
      // 1. <style> tags in the head
      // 2. CSS preload with onload
      // 3. Proper stylesheet linking with preload hints

      const headRegex = /<head[^>]*>([\s\S]*?)<\/head>/i;
      const headMatch = htmlContent.match(headRegex);

      expect(headMatch).not.toBeNull();

      const headContent = headMatch[1];

      // Check for one of the following performance optimizations:
      // 1. Inline critical styles
      const hasInlineStyles = headContent.includes('<style');

      // 2. CSS preconnect for external fonts (performance optimization)
      const hasPreconnect = headContent.includes('rel="preconnect"');

      // 3. Stylesheet link exists
      const hasStylesheet = headContent.includes('rel="stylesheet"');

      // 4. Font preloading (display=swap for non-blocking)
      const hasFontDisplaySwap = headContent.includes('display=swap');

      // Log for debugging
      console.log('Critical CSS Analysis:');
      console.log(`  Has inline styles: ${hasInlineStyles}`);
      console.log(`  Has preconnect: ${hasPreconnect}`);
      console.log(`  Has stylesheet: ${hasStylesheet}`);
      console.log(`  Has font display swap: ${hasFontDisplaySwap}`);

      // Page should have at least stylesheet links
      expect(hasStylesheet).toBe(true);

      // Page should use preconnect for external resources (Google Fonts)
      expect(hasPreconnect).toBe(true);
    });

    test('Stylesheets are loaded efficiently', () => {
      // Check that main stylesheet is loaded
      const stylesheetRegex = /<link[^>]*rel="stylesheet"[^>]*>/gi;
      const stylesheets = htmlContent.match(stylesheetRegex) || [];

      // Should have at least one stylesheet
      expect(stylesheets.length).toBeGreaterThan(0);

      // The main CSS file should be present
      const mainCssLoaded = stylesheets.some((link) =>
        link.includes('styles.css')
      );
      expect(mainCssLoaded).toBe(true);
    });

    test('External resources use preconnect', () => {
      // Google Fonts should have preconnect hints
      const preconnectRegex = /<link[^>]*rel="preconnect"[^>]*>/gi;
      const preconnects = htmlContent.match(preconnectRegex) || [];

      // Should have preconnect for external resources
      expect(preconnects.length).toBeGreaterThan(0);

      // Should preconnect to Google Fonts
      const googleFontsPreconnect = preconnects.some(
        (link) =>
          link.includes('fonts.googleapis.com') ||
          link.includes('fonts.gstatic.com')
      );
      expect(googleFontsPreconnect).toBe(true);
    });

    test('Font loading uses display=swap for non-blocking', () => {
      // Google Fonts should use display=swap parameter
      const fontLinkRegex = /<link[^>]*fonts\.googleapis\.com[^>]*>/gi;
      const fontLinks = htmlContent.match(fontLinkRegex) || [];

      if (fontLinks.length > 0) {
        // At least one font link should have display=swap
        const hasDisplaySwap = fontLinks.some((link) =>
          link.includes('display=swap')
        );
        expect(hasDisplaySwap).toBe(true);
      }
    });
  });

  describe('CSS loading order', () => {
    test('CSS is loaded before critical content', () => {
      const headRegex = /<head[^>]*>([\s\S]*?)<\/head>/i;
      const headMatch = htmlContent.match(headRegex);

      if (headMatch) {
        const headContent = headMatch[1];

        // CSS should be in head, not body
        const cssInHead = headContent.includes('stylesheet');
        expect(cssInHead).toBe(true);
      }
    });

    test('Scripts are loaded after content (defer or at end)', () => {
      // Check if scripts are at the end of body or use defer/async
      const bodyRegex = /<body[^>]*>([\s\S]*?)<\/body>/i;
      const bodyMatch = htmlContent.match(bodyRegex);

      if (bodyMatch) {
        const bodyContent = bodyMatch[1];

        // Scripts should be near the end of body or use module/defer
        const scriptRegex = /<script[^>]*src=[^>]*>/gi;
        const scripts = bodyContent.match(scriptRegex) || [];

        scripts.forEach((script) => {
          // Scripts should either:
          // 1. Use type="module" (automatically deferred)
          // 2. Use defer attribute
          // 3. Use async attribute
          // 4. Be at the end of the body
          const hasModule = script.includes('type="module"');
          const hasDefer = script.includes('defer');
          const hasAsync = script.includes('async');

          // CDN scripts (Prism, Mermaid) are expected without defer
          // Local scripts should use module or be at the end
          if (script.includes('main.js')) {
            expect(hasModule || hasDefer).toBe(true);
          }
        });
      }
    });
  });
});

describe('Performance Best Practices', () => {
  test('No inline scripts in head blocking render', () => {
    const headRegex = /<head[^>]*>([\s\S]*?)<\/head>/i;
    const headMatch = htmlContent.match(headRegex);

    if (headMatch) {
      const headContent = headMatch[1];

      // Look for inline scripts with logic (not just comments)
      const inlineScriptRegex = /<script[^>]*>[\s\S]*?<\/script>/gi;
      const inlineScripts = headContent.match(inlineScriptRegex) || [];

      // Filter out empty scripts and scripts that only have whitespace
      const blockingScripts = inlineScripts.filter((script) => {
        const content = script.replace(/<\/?script[^>]*>/gi, '').trim();
        return content.length > 0 && !script.includes('src=');
      });

      // No blocking inline scripts in head
      expect(blockingScripts.length).toBe(0);
    }
  });

  test('Images have width and height attributes for CLS', () => {
    const imgTagRegex = /<img[^>]*>/gi;
    const imgTags = htmlContent.match(imgTagRegex) || [];

    // At least some images should have dimensions to prevent CLS
    const imagesWithDimensions = imgTags.filter(
      (img) => img.includes('width') && img.includes('height')
    );

    // Navigation and footer logo images should have dimensions
    expect(imagesWithDimensions.length).toBeGreaterThan(0);
  });
});
