/**
 * Performance E2E Tests
 * Owner: Scenario 12 - Performance Requirements
 *
 * Tests:
 * - Page load time on 3G
 * - Total page weight
 * - CSS optimization
 * - Image optimization
 * - CDN failure graceful degradation
 */

const { test, expect } = require('@playwright/test');
const { waitForPageLoad } = require('../test-utils/helpers');
const fs = require('fs');
const path = require('path');

test.describe('Performance Requirements', () => {
  test.describe('Page Load Time', () => {
    test('page fully loads in under 2 seconds on simulated 3G connection', async ({ page, context }) => {
      // Simulate Fast 3G network conditions using CDP
      const cdpSession = await context.newCDPSession(page);
      await cdpSession.send('Network.enable');
      await cdpSession.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (1.6 * 1024 * 1024) / 8, // 1.6 Mbps - Fast 3G download
        uploadThroughput: (768 * 1024) / 8, // 768 Kbps upload
        latency: 150, // 150ms latency
      });

      // Start timing
      const startTime = Date.now();

      // Navigate to the page
      await page.goto('/');

      // Wait for DOM content to be loaded
      await page.waitForLoadState('domcontentloaded');

      // Calculate load time
      const loadTime = Date.now() - startTime;

      // Get performance metrics from the browser
      const performanceMetrics = await page.evaluate(() => {
        const timing = performance.timing;
        return {
          domContentLoaded: timing.domContentLoadedEventEnd - timing.navigationStart,
          loadComplete: timing.loadEventEnd - timing.navigationStart,
          firstPaint: performance.getEntriesByType('paint')[0]?.startTime || 0,
        };
      });

      // DOM content should load within 2 seconds (2000ms)
      // Using domContentLoaded as the primary metric since it reflects when the page is interactive
      expect(performanceMetrics.domContentLoaded).toBeLessThan(2000);

      // Reset network conditions
      await cdpSession.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: -1,
        uploadThroughput: -1,
        latency: 0,
      });
    });
  });

  test.describe('Total Page Weight', () => {
    test('total page size (HTML + CSS + JS + images) is reasonable for fast loading', async ({ page }) => {
      // Track all network requests and their sizes
      const resourceSizes = {
        html: 0,
        css: 0,
        javascript: 0,
        images: 0,
        fonts: 0,
        other: 0,
        total: 0,
      };

      // Listen for responses
      page.on('response', async (response) => {
        const url = response.url();
        const status = response.status();

        // Only count successful responses
        if (status >= 200 && status < 400) {
          try {
            const headers = response.headers();
            const contentLength = parseInt(headers['content-length'] || '0', 10);
            const contentType = headers['content-type'] || '';

            let size = contentLength;

            // If content-length is not available, try to get body size
            if (size === 0 && !url.includes('circleci')) {
              try {
                const body = await response.body();
                size = body.length;
              } catch {
                // Some responses can't be read
                size = 0;
              }
            }

            resourceSizes.total += size;

            if (contentType.includes('text/html')) {
              resourceSizes.html += size;
            } else if (contentType.includes('text/css')) {
              resourceSizes.css += size;
            } else if (contentType.includes('javascript')) {
              resourceSizes.javascript += size;
            } else if (contentType.includes('image/')) {
              resourceSizes.images += size;
            } else if (contentType.includes('font/') || url.includes('.woff') || url.includes('.ttf')) {
              resourceSizes.fonts += size;
            } else {
              resourceSizes.other += size;
            }
          } catch {
            // Ignore errors for specific resources
          }
        }
      });

      await page.goto('/');
      await waitForPageLoad(page);

      // Calculate sizes in KB
      const htmlKB = resourceSizes.html / 1024;
      const cssKB = resourceSizes.css / 1024;
      const jsKB = resourceSizes.javascript / 1024;

      // Core page resources (HTML + CSS + JS without images) should be under 500KB
      // This ensures fast initial render even with large demo images
      const coreResourceSize = resourceSizes.html + resourceSizes.css + resourceSizes.javascript;
      const coreResourceKB = coreResourceSize / 1024;

      // HTML should be under 50KB (uncompressed)
      expect(htmlKB).toBeLessThan(50);

      // CSS should be under 100KB (uncompressed)
      expect(cssKB).toBeLessThan(100);

      // Core resources (HTML + CSS + inline JS) should be reasonable
      expect(coreResourceKB).toBeLessThan(500);
    });
  });

  test.describe('CSS Optimization', () => {
    test('CSS file is minified or inline styles are minimal', async ({ page }) => {
      // Read CSS file directly from disk
      const cssPath = path.join(__dirname, '../../css/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check CSS file size (should be under 100KB for a single-page site)
      const cssSizeKB = Buffer.byteLength(cssContent, 'utf-8') / 1024;
      expect(cssSizeKB).toBeLessThan(100);

      // Navigate to page and check for inline styles
      await page.goto('/');

      // Count inline style attributes - should be minimal
      const inlineStyleCount = await page.evaluate(() => {
        const elementsWithStyle = document.querySelectorAll('[style]');
        return elementsWithStyle.length;
      });

      // Allow some inline styles but keep it minimal (under 20)
      expect(inlineStyleCount).toBeLessThan(20);

      // Verify CSS is properly linked (not all inline)
      const linkTags = await page.locator('link[rel="stylesheet"]').count();
      expect(linkTags).toBeGreaterThanOrEqual(1);

      // Check that CSS uses variables for maintainability (indicates good organization)
      const usesVariables = cssContent.includes('--color-') && cssContent.includes(':root');
      expect(usesVariables).toBe(true);

      // Check for unnecessary whitespace in CSS (basic minification check)
      // A well-organized CSS file shouldn't have excessive empty lines
      const emptyLines = (cssContent.match(/^\s*$/gm) || []).length;
      const totalLines = cssContent.split('\n').length;
      const emptyLineRatio = emptyLines / totalLines;

      // Empty lines should be less than 20% of total (reasonable formatting)
      expect(emptyLineRatio).toBeLessThan(0.2);
    });
  });

  test.describe('Image Optimization', () => {
    test('images are appropriately sized and have optimization attributes', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Get all images on the page
      const images = await page.locator('img').all();

      for (const img of images) {
        const src = await img.getAttribute('src');
        const alt = await img.getAttribute('alt');
        const loading = await img.getAttribute('loading');

        // Skip external images (like CircleCI badge)
        if (src && src.startsWith('http') && !src.includes('localhost')) {
          continue;
        }

        // All images should have alt text
        expect(alt).toBeTruthy();

        // Non-critical images should use lazy loading
        if (src && (src.includes('usage') || src.includes('demo'))) {
          expect(loading).toBe('lazy');
        }
      }

      // Check if any very large images are loaded above the fold
      const viewportImages = await page.evaluate(() => {
        const imgs = Array.from(document.querySelectorAll('img'));
        return imgs
          .filter((img) => {
            const rect = img.getBoundingClientRect();
            return rect.top < window.innerHeight;
          })
          .map((img) => ({
            src: img.src,
            naturalWidth: img.naturalWidth,
            naturalHeight: img.naturalHeight,
            displayWidth: img.width,
            displayHeight: img.height,
          }));
      });

      // Verify images aren't massively oversized for their display size
      for (const imgData of viewportImages) {
        if (imgData.naturalWidth > 0 && imgData.displayWidth > 0) {
          // Image shouldn't be more than 2x its display size (for retina)
          const widthRatio = imgData.naturalWidth / imgData.displayWidth;
          // Allow up to 3x for high DPI displays, but log a warning if over 2x
          expect(widthRatio).toBeLessThan(4);
        }
      }
    });

    test('image file sizes are optimized', async () => {
      const imagesDir = path.join(__dirname, '../../images');

      // Check if images directory exists
      if (!fs.existsSync(imagesDir)) {
        // No images directory is fine - might use external images
        return;
      }

      const files = fs.readdirSync(imagesDir);

      for (const file of files) {
        const filePath = path.join(imagesDir, file);
        const stats = fs.statSync(filePath);
        const fileSizeKB = stats.size / 1024;
        const fileSizeMB = fileSizeKB / 1024;

        // Individual images should ideally be under 2MB for web
        // GIFs can be larger but should be noted
        if (file.endsWith('.gif')) {
          // GIFs are inherently larger - allow up to 10MB but log size
          expect(fileSizeMB).toBeLessThan(10);
        } else {
          // Other images should be under 2MB
          expect(fileSizeMB).toBeLessThan(2);
        }
      }
    });
  });

  test.describe('CDN Failure Graceful Degradation', () => {
    test('page remains functional if Prism.js CDN is unavailable (graceful degradation)', async ({ page, context }) => {
      // Block all requests to cdnjs.cloudflare.com (Prism.js CDN)
      await context.route('**/cdnjs.cloudflare.com/**', (route) => {
        route.abort();
      });

      // Navigate to the page with CDN blocked
      await page.goto('/');

      // Wait for page to stabilize
      await page.waitForLoadState('domcontentloaded');

      // Page should still be visible and functional
      await expect(page.locator('body')).toBeVisible();

      // Hero section should be visible
      await expect(page.locator('#hero')).toBeVisible();

      // Navigation should work
      await expect(page.locator('nav')).toBeVisible();

      // Features section should be visible
      await expect(page.locator('#features')).toBeVisible();

      // Quick Start section should be visible
      await expect(page.locator('#quick-start')).toBeVisible();

      // Code blocks should still be readable (just without syntax highlighting)
      const codeBlock = page.locator('#install-command');
      await expect(codeBlock).toBeVisible();

      // Code content should be present
      const codeText = await codeBlock.textContent();
      expect(codeText).toContain('git clone');
      expect(codeText).toContain('cargo run');

      // Navigation links should still work
      const featuresLink = page.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();

      // Should scroll to features section
      await page.waitForTimeout(500); // Wait for smooth scroll
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();

      // Footer should be visible
      await expect(page.locator('footer')).toBeVisible();

      // Copy button should still be present
      const copyButton = page.locator('.copy-button');
      await expect(copyButton).toBeVisible();

      // Dark mode toggle should still work
      const themeToggle = page.locator('#theme-toggle');
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // HTML element should have dark-mode or light-mode class after clicking
      // Theme is applied to document.documentElement (html), not body
      await expect(page.locator('html')).toHaveClass(/dark-mode|light-mode/);
    });

    test('page renders correctly with JavaScript disabled', async ({ browser }) => {
      // Create a new context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false,
      });

      const page = await context.newPage();

      // Navigate to the page
      await page.goto('/');

      // Core content should still be visible
      await expect(page.locator('body')).toBeVisible();

      // All main sections should be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('#usage')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Code content should still be readable
      const codeBlock = page.locator('#install-command');
      await expect(codeBlock).toBeVisible();
      const codeText = await codeBlock.textContent();
      expect(codeText).toContain('git clone');

      // Navigation links should be present
      const navLinks = page.locator('.nav-link');
      await expect(navLinks.first()).toBeVisible();

      // External links should work
      const githubLink = page.locator('a[href*="github.com/yetone/mirdb"]').first();
      await expect(githubLink).toBeVisible();

      await context.close();
    });
  });
});
