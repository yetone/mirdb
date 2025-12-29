import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Minimal External Dependencies Tests (NFR-5)
 *
 * These tests verify that the MirDB landing page has minimal external dependencies
 * for long-term maintainability, as specified in the Product Requirements Document.
 *
 * Test Cases:
 * 1. Count external CDN resources - Page uses minimal or no external CDN resources
 * 2. Review external script tags - No unnecessary third-party JavaScript libraries loaded
 * 3. Verify page works offline (after initial load) - Static content accessible without external dependencies
 */

test.describe('Minimal External Dependencies (NFR-5)', () => {

  test.describe('Test Case 1: Count external CDN resources', () => {

    test('should have minimal or no external CDN resources in HTML', async () => {
      // Read the HTML file directly
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Patterns that indicate CDN usage
      const cdnPatterns = [
        /https?:\/\/[^"'\s]*cdn[^"'\s]*/gi,           // Generic CDN URLs
        /https?:\/\/cdnjs\.cloudflare\.com/gi,        // Cloudflare CDN
        /https?:\/\/cdn\.jsdelivr\.net/gi,            // jsDelivr CDN
        /https?:\/\/unpkg\.com/gi,                    // unpkg CDN
        /https?:\/\/maxcdn\.bootstrapcdn\.com/gi,     // Bootstrap CDN
        /https?:\/\/stackpath\.bootstrapcdn\.com/gi,  // Bootstrap CDN (stackpath)
        /https?:\/\/code\.jquery\.com/gi,             // jQuery CDN
        /https?:\/\/ajax\.googleapis\.com/gi,         // Google CDN
        /https?:\/\/fonts\.googleapis\.com/gi,        // Google Fonts
        /https?:\/\/fonts\.gstatic\.com/gi,           // Google Fonts static
        /https?:\/\/use\.fontawesome\.com/gi,         // Font Awesome
        /https?:\/\/kit\.fontawesome\.com/gi,         // Font Awesome kit
      ];

      let totalCdnMatches: string[] = [];

      for (const pattern of cdnPatterns) {
        const matches = htmlContent.match(pattern) || [];
        totalCdnMatches = [...totalCdnMatches, ...matches];
      }

      // Remove duplicates
      const uniqueCdnUrls = [...new Set(totalCdnMatches)];

      console.log(`CDN Resources Found: ${uniqueCdnUrls.length}`);
      if (uniqueCdnUrls.length > 0) {
        console.log('  CDN URLs detected:');
        uniqueCdnUrls.forEach(url => console.log(`    - ${url}`));
      }

      // The page should use minimal or no external CDN resources
      // We expect 0 CDN resources for optimal maintainability
      expect(uniqueCdnUrls.length).toBe(0);
    });

    test('should not load external CSS from CDN', async ({ page }) => {
      const cdnStylesheets: string[] = [];

      // Intercept network requests to detect CDN CSS loads
      page.on('request', (request) => {
        const url = request.url();
        const resourceType = request.resourceType();

        if (resourceType === 'stylesheet' && !url.startsWith('http://localhost')) {
          cdnStylesheets.push(url);
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      console.log(`External CSS requests: ${cdnStylesheets.length}`);
      cdnStylesheets.forEach(url => console.log(`  - ${url}`));

      // No external stylesheet requests should be made
      expect(cdnStylesheets.length).toBe(0);
    });

    test('should not load external fonts from CDN', async ({ page }) => {
      const fontRequests: string[] = [];

      // Intercept network requests to detect font loads
      page.on('request', (request) => {
        const url = request.url();
        const resourceType = request.resourceType();

        if (resourceType === 'font' || url.includes('fonts.')) {
          fontRequests.push(url);
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      console.log(`Font requests: ${fontRequests.length}`);
      fontRequests.forEach(url => console.log(`  - ${url}`));

      // No external font requests should be made
      // The page uses system fonts for optimal performance
      expect(fontRequests.length).toBe(0);
    });
  });

  test.describe('Test Case 2: Review external script tags', () => {

    test('should have no unnecessary third-party JavaScript libraries loaded', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Pattern to find external script tags (not inline scripts)
      const externalScriptPattern = /<script[^>]+src=["']([^"']+)["'][^>]*>/gi;
      const externalScripts: string[] = [];

      let match;
      while ((match = externalScriptPattern.exec(htmlContent)) !== null) {
        externalScripts.push(match[1]);
      }

      console.log(`External Script Tags Found: ${externalScripts.length}`);
      externalScripts.forEach(src => console.log(`  - ${src}`));

      // There should be no external script tags
      // All JavaScript should be inline for this simple static page
      expect(externalScripts.length).toBe(0);
    });

    test('should not load third-party JavaScript at runtime', async ({ page }) => {
      const thirdPartyScripts: string[] = [];

      page.on('request', (request) => {
        const url = request.url();
        const resourceType = request.resourceType();

        if (resourceType === 'script' && !url.startsWith('http://localhost')) {
          thirdPartyScripts.push(url);
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      console.log(`Third-party script requests: ${thirdPartyScripts.length}`);
      thirdPartyScripts.forEach(url => console.log(`  - ${url}`));

      // No third-party scripts should be loaded
      expect(thirdPartyScripts.length).toBe(0);
    });

    test('should not include common third-party libraries', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Common third-party JavaScript libraries to check for
      const thirdPartyLibraries = [
        'jquery',
        'react',
        'vue',
        'angular',
        'bootstrap.js',
        'bootstrap.min.js',
        'lodash',
        'moment',
        'axios',
        'gsap',
        'anime.js',
        'swiper',
        'slick',
        'owl.carousel',
      ];

      const foundLibraries: string[] = [];

      for (const library of thirdPartyLibraries) {
        if (htmlContent.toLowerCase().includes(library)) {
          foundLibraries.push(library);
        }
      }

      console.log(`Third-party libraries detected: ${foundLibraries.length}`);
      foundLibraries.forEach(lib => console.log(`  - ${lib}`));

      // No common third-party libraries should be present
      expect(foundLibraries.length).toBe(0);
    });

    test('should only have minimal inline JavaScript for core functionality', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Extract inline script content
      const inlineScriptPattern = /<script[^>]*>(?![\s\S]*src=)([\s\S]*?)<\/script>/gi;
      const inlineScripts: string[] = [];

      let match;
      while ((match = inlineScriptPattern.exec(htmlContent)) !== null) {
        if (match[1].trim().length > 0) {
          inlineScripts.push(match[1].trim());
        }
      }

      console.log(`Inline scripts found: ${inlineScripts.length}`);

      // Calculate total inline JavaScript size
      const totalJsSize = inlineScripts.reduce((acc, script) => acc + Buffer.byteLength(script, 'utf8'), 0);
      const totalJsSizeKB = totalJsSize / 1024;

      console.log(`Total inline JavaScript size: ${totalJsSizeKB.toFixed(2)} KB`);

      // Inline JavaScript should be minimal (< 10KB for simple interactivity)
      // The page only needs basic functionality like copy-to-clipboard and smooth scrolling
      expect(totalJsSizeKB).toBeLessThan(10);

      // Verify the JavaScript is for legitimate functionality
      if (inlineScripts.length > 0) {
        const jsContent = inlineScripts.join('\n');
        // Check for expected functionality
        const hasCopyFunctionality = jsContent.includes('clipboard') || jsContent.includes('copy');
        const hasScrollFunctionality = jsContent.includes('scroll');

        console.log(`  - Has copy functionality: ${hasCopyFunctionality}`);
        console.log(`  - Has scroll functionality: ${hasScrollFunctionality}`);

        // The inline JS should be for known features
        expect(hasCopyFunctionality || hasScrollFunctionality).toBe(true);
      }
    });
  });

  test.describe('Test Case 3: Verify page works offline (after initial load)', () => {

    test('should display static content without network connection', async ({ page, context }) => {
      // First, load the page normally to simulate initial load
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify initial content is visible
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="features-section"]')).toBeVisible();

      // Now go offline
      await context.setOffline(true);

      // Attempt to navigate (should use cached content)
      // Note: This tests if the page content doesn't require additional network requests
      // The page should already be fully rendered

      // Verify all major sections are still visible
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="getting-started-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="configuration-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="technical-specs-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="architecture-section"]')).toBeVisible();

      // Verify interactive elements still work (they use inline JS)
      const copyBtn = page.locator('[data-testid="copy-btn-1"]');
      await expect(copyBtn).toBeVisible();

      // Verify navigation links are present
      const navLinks = page.locator('.nav-link');
      await expect(navLinks).toHaveCount(5);

      // Go back online
      await context.setOffline(false);
    });

    test('should not make external API calls on page load', async ({ page }) => {
      const apiCalls: string[] = [];

      page.on('request', (request) => {
        const url = request.url();
        const resourceType = request.resourceType();

        // Check for API calls (XHR/Fetch requests to external URLs)
        if ((resourceType === 'fetch' || resourceType === 'xhr') && !url.startsWith('http://localhost')) {
          apiCalls.push(url);
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      console.log(`External API calls detected: ${apiCalls.length}`);
      apiCalls.forEach(url => console.log(`  - ${url}`));

      // No external API calls should be made
      expect(apiCalls.length).toBe(0);
    });

    test('should have all content embedded in HTML (no lazy-loaded external content)', async () => {
      const indexPath = path.resolve(__dirname, '../index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check that all major content sections are present in the HTML
      const requiredContent = [
        'MirDB',                                  // Product name
        'Memcached',                              // Key technology
        'LSM',                                    // Architecture mention
        'Persistent',                             // Key feature
        'SET',                                    // Command
        'GET',                                    // Command
        'DELETE',                                 // Command
        '0.0.0.0:12333',                         // Default config
        'github.com',                            // GitHub link (only external dependency - required by PRD)
      ];

      console.log('Checking for embedded content:');
      for (const content of requiredContent) {
        const isPresent = htmlContent.includes(content);
        console.log(`  - "${content}": ${isPresent ? 'Present' : 'Missing'}`);
        expect(isPresent).toBe(true);
      }

      // Check for lazy loading indicators that might require network
      const lazyLoadPatterns = [
        'data-src=',           // Lazy loaded images
        'loading="lazy"',      // Native lazy loading (acceptable for images)
        'data-lazy',           // jQuery lazy loading
        '__NEXT_DATA__',       // Next.js SSR hydration
        'window.__NUXT__',     // Nuxt.js SSR hydration
      ];

      // Some lazy loading is acceptable (like native image lazy loading)
      // But framework-specific hydration patterns indicate SSR/external deps
      const problematicPatterns = [
        '__NEXT_DATA__',
        'window.__NUXT__',
        'data-reactroot',
      ];

      for (const pattern of problematicPatterns) {
        const hasPattern = htmlContent.includes(pattern);
        console.log(`  - Framework pattern "${pattern}": ${hasPattern ? 'FOUND' : 'Not found'}`);
        expect(hasPattern).toBe(false);
      }
    });

    test('should function fully without JavaScript enabled', async ({ page }) => {
      // Block all JavaScript
      await page.route('**/*', (route) => {
        if (route.request().resourceType() === 'script') {
          return route.abort();
        }
        return route.continue();
      });

      await page.goto('/');

      // All static content should be visible without JavaScript
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#getting-started')).toBeVisible();
      await expect(page.locator('#configuration')).toBeVisible();
      await expect(page.locator('#technical-specs')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();

      // Navigation links should be present
      const navLinks = await page.locator('.nav-link').count();
      expect(navLinks).toBeGreaterThan(0);

      // Code blocks should display content
      const codeBlocks = await page.locator('pre code').count();
      expect(codeBlocks).toBeGreaterThan(0);

      // Footer should be visible
      await expect(page.locator('.footer')).toBeVisible();
    });

    test('should have all assets self-contained (no external image/media dependencies)', async ({ page }) => {
      const externalAssets: string[] = [];

      page.on('request', (request) => {
        const url = request.url();
        const resourceType = request.resourceType();

        // Check for external images, media, fonts
        if (['image', 'media', 'font'].includes(resourceType) && !url.startsWith('http://localhost')) {
          externalAssets.push(url);
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      console.log(`External asset requests: ${externalAssets.length}`);
      externalAssets.forEach(url => console.log(`  - ${url}`));

      // No external assets should be loaded
      expect(externalAssets.length).toBe(0);
    });
  });

  test.describe('Build Dependencies Audit', () => {

    test('should have minimal devDependencies in package.json', async () => {
      const packagePath = path.resolve(__dirname, '../package.json');
      const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf-8'));

      const devDeps = Object.keys(packageJson.devDependencies || {});
      const deps = Object.keys(packageJson.dependencies || {});

      console.log(`Package Dependencies Analysis:`);
      console.log(`  Runtime dependencies: ${deps.length}`);
      deps.forEach(dep => console.log(`    - ${dep}`));
      console.log(`  Dev dependencies: ${devDeps.length}`);
      devDeps.forEach(dep => console.log(`    - ${dep}`));

      // Should have zero runtime dependencies (static site)
      expect(deps.length).toBe(0);

      // Dev dependencies should be minimal (only testing tools)
      // Expected: @playwright/test and @axe-core/playwright
      expect(devDeps.length).toBeLessThanOrEqual(5);

      // Verify dev dependencies are only for testing
      const testingDeps = devDeps.filter(dep =>
        dep.includes('playwright') ||
        dep.includes('test') ||
        dep.includes('axe')
      );

      console.log(`  Testing-related dev deps: ${testingDeps.length}`);
      expect(testingDeps.length).toBe(devDeps.length);
    });

    test('should not have build tool dependencies', async () => {
      const packagePath = path.resolve(__dirname, '../package.json');
      const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf-8'));

      const allDeps = {
        ...packageJson.dependencies || {},
        ...packageJson.devDependencies || {},
      };

      // Build tools that would add complexity
      const buildTools = [
        'webpack',
        'rollup',
        'parcel',
        'vite',
        'esbuild',
        'gulp',
        'grunt',
        'babel',
        'typescript',
        'sass',
        'less',
        'postcss',
        'tailwindcss',
      ];

      const foundBuildTools: string[] = [];

      for (const tool of buildTools) {
        if (Object.keys(allDeps).some(dep => dep.includes(tool))) {
          foundBuildTools.push(tool);
        }
      }

      console.log(`Build tools found: ${foundBuildTools.length}`);
      foundBuildTools.forEach(tool => console.log(`  - ${tool}`));

      // No build tools should be required
      expect(foundBuildTools.length).toBe(0);
    });
  });
});
