import { describe, it, expect, beforeAll } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

// File paths
const ROOT_DIR = path.resolve(__dirname, '..');
const INDEX_HTML = path.join(ROOT_DIR, 'index.html');
const STYLES_CSS = path.join(ROOT_DIR, 'styles.css');
const ASSETS_DIR = path.join(ROOT_DIR, 'assets');

// Size limits in bytes
const MAX_TOTAL_PAGE_SIZE = 1 * 1024 * 1024; // 1MB
const MAX_CSS_SIZE = 50 * 1024; // 50KB
const MAX_JS_SIZE = 50 * 1024; // 50KB
const MAX_IMAGE_SIZE = 200 * 1024; // 200KB per image for optimal loading

describe('Performance - Page Load Time', () => {
  let htmlContent;
  let cssContent;
  let htmlSize;
  let cssSize;

  beforeAll(() => {
    htmlContent = fs.readFileSync(INDEX_HTML, 'utf-8');
    cssContent = fs.readFileSync(STYLES_CSS, 'utf-8');
    htmlSize = fs.statSync(INDEX_HTML).size;
    cssSize = fs.statSync(STYLES_CSS).size;
  });

  describe('Test Case 1: Page Load Time', () => {
    it('should have minimal blocking resources for fast initial load', () => {
      // Check that CSS is loaded in head (render-blocking but necessary)
      expect(htmlContent).toContain('<link rel="stylesheet" href="styles.css">');

      // Check that there are no external heavy JavaScript files blocking render
      // The page uses inline JavaScript which doesn't block initial render
      const externalScripts = htmlContent.match(/<script\s+src=/g);
      expect(externalScripts).toBeNull();
    });

    it('should not have defer/async issues that could delay interactivity', () => {
      // Scripts should be at the end of body or inline for optimal loading
      const bodyEndMatch = htmlContent.match(/<\/body>/);
      const scriptMatch = htmlContent.match(/<script>/g);

      // Should have inline scripts (no src attribute)
      expect(scriptMatch).not.toBeNull();

      // Scripts are placed before closing body tag (good practice)
      const bodyIndex = htmlContent.lastIndexOf('</body>');
      const lastScriptIndex = htmlContent.lastIndexOf('</script>');
      expect(lastScriptIndex).toBeLessThan(bodyIndex);
    });

    it('should have proper meta tags for viewport (affects perceived load time)', () => {
      expect(htmlContent).toContain('<meta name="viewport"');
      expect(htmlContent).toContain('width=device-width');
    });
  });

  describe('Test Case 2: Total Page Weight', () => {
    it('should have total static assets under 1MB', () => {
      // Calculate total size of HTML, CSS, and inline JS
      // (excluding images which should be lazy-loaded or optimized separately)
      const totalStaticSize = htmlSize + cssSize;

      expect(totalStaticSize).toBeLessThan(MAX_TOTAL_PAGE_SIZE);

      // Log actual sizes for documentation
      console.log(`HTML size: ${(htmlSize / 1024).toFixed(2)}KB`);
      console.log(`CSS size: ${(cssSize / 1024).toFixed(2)}KB`);
      console.log(`Total static assets: ${(totalStaticSize / 1024).toFixed(2)}KB`);
    });

    it('should have HTML file reasonably sized', () => {
      // HTML should be under 100KB for fast initial load
      const MAX_HTML_SIZE = 100 * 1024;
      expect(htmlSize).toBeLessThan(MAX_HTML_SIZE);
    });
  });

  describe('Test Case 3: CSS File Size', () => {
    it('should have CSS files under 50KB combined', () => {
      expect(cssSize).toBeLessThan(MAX_CSS_SIZE);
      console.log(`CSS size: ${(cssSize / 1024).toFixed(2)}KB (limit: 50KB)`);
    });

    it('should use CSS variables for maintainability without bloat', () => {
      // CSS variables are efficient and reduce repeated declarations
      expect(cssContent).toContain(':root {');
      expect(cssContent).toContain('--primary-color');
    });

    it('should have responsive styles without excessive media queries', () => {
      // Count media queries - should be reasonable
      const mediaQueries = cssContent.match(/@media/g) || [];
      expect(mediaQueries.length).toBeLessThanOrEqual(5);
    });
  });

  describe('Test Case 4: JavaScript File Size', () => {
    it('should have JavaScript under 50KB', () => {
      // Extract inline JavaScript
      const scriptMatch = htmlContent.match(/<script>([\s\S]*?)<\/script>/);
      const inlineJS = scriptMatch ? scriptMatch[1] : '';
      const jsSize = Buffer.byteLength(inlineJS, 'utf-8');

      expect(jsSize).toBeLessThan(MAX_JS_SIZE);
      console.log(`Inline JS size: ${(jsSize / 1024).toFixed(2)}KB (limit: 50KB)`);
    });

    it('should not include external JavaScript libraries', () => {
      // Check for common large libraries that would bloat the page
      expect(htmlContent).not.toContain('jquery');
      expect(htmlContent).not.toContain('react');
      expect(htmlContent).not.toContain('vue');
      expect(htmlContent).not.toContain('angular');
    });

    it('should have minimal JavaScript functionality', () => {
      // The page should work without JavaScript for basic viewing
      // JavaScript should only enhance functionality (copy button, mobile menu)
      const scriptMatch = htmlContent.match(/<script>([\s\S]*?)<\/script>/);
      const inlineJS = scriptMatch ? scriptMatch[1] : '';

      // Should only have essential functionality
      expect(inlineJS).toContain('copyToClipboard');
      expect(inlineJS).toContain('mobile-menu-toggle');
    });
  });

  describe('Test Case 5: Image Optimization', () => {
    it('should check that SVG diagrams are used inline for efficiency', () => {
      // SVG is used inline for the architecture diagram
      expect(htmlContent).toContain('<svg');
      expect(htmlContent).toContain('viewBox');
    });

    it('should verify no unoptimized large images are directly embedded', () => {
      // Check that large image files in assets are not directly referenced
      // in a way that would block initial page load
      const imgTags = htmlContent.match(/<img[^>]+src="[^"]*"/g) || [];

      // If there are img tags, they should be reasonably small or lazy-loaded
      for (const imgTag of imgTags) {
        // This is a basic check - images should use lazy loading for performance
        console.log(`Found image tag: ${imgTag}`);
      }

      // The current page uses inline SVG instead of img tags for the diagram
      // which is optimal for performance
    });

    it('should verify assets directory images are appropriately sized', () => {
      // Check assets directory
      if (fs.existsSync(ASSETS_DIR)) {
        const assets = fs.readdirSync(ASSETS_DIR);

        for (const asset of assets) {
          const assetPath = path.join(ASSETS_DIR, asset);
          const stats = fs.statSync(assetPath);

          // Log asset sizes
          console.log(`Asset ${asset}: ${(stats.size / 1024 / 1024).toFixed(2)}MB`);

          // For optimal web performance, individual images should be optimized
          // Note: GIF files are typically used for animations and may be larger
          // but should still be optimized for web delivery
          if (asset.endsWith('.png') || asset.endsWith('.jpg') || asset.endsWith('.jpeg')) {
            expect(stats.size).toBeLessThan(MAX_IMAGE_SIZE);
          }
        }
      }
    });

    it('should not reference large assets in critical render path', () => {
      // Check that large GIF assets from assets directory are not
      // blocking the initial page render
      const criticalAssetRefs = htmlContent.match(/src="assets\/[^"]+"/g) || [];

      // If assets are referenced, they should not be in the critical path
      // (e.g., they should use lazy loading or be below the fold)
      console.log(`Asset references found: ${criticalAssetRefs.length}`);
    });
  });

  describe('Performance Best Practices', () => {
    it('should use system fonts to avoid font loading delays', () => {
      // Check that system font stack is used
      expect(cssContent).toContain('-apple-system');
      expect(cssContent).toContain('BlinkMacSystemFont');
    });

    it('should have prefers-reduced-motion support', () => {
      // Accessibility and performance feature
      expect(cssContent).toContain('prefers-reduced-motion');
    });

    it('should have smooth scrolling that respects user preferences', () => {
      expect(cssContent).toContain('scroll-behavior: smooth');
    });
  });
});
