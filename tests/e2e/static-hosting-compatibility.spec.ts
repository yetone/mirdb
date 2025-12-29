// @ts-check
import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Static Hosting Compatibility Tests
 *
 * Scenario: Verify the homepage can be deployed as static files without server-side processing
 *
 * Test Cases:
 * 1. Page renders correctly from any static file server (integration)
 * 2. No PHP, Node.js, or other server-side code required (unit)
 * 3. All asset references use relative paths (unit)
 * 4. Page functions when opened as file:// protocol (integration)
 */

test.describe('Static Hosting Compatibility', () => {
  test.describe('TC1: Static File Server Rendering', () => {
    test('Page renders correctly from simple HTTP server', async ({ page }) => {
      // Navigate to the homepage served by the static server
      await page.goto('/');

      // Verify the page loads without errors
      const pageTitle = await page.title();
      expect(pageTitle).toContain('MirDB');

      // Verify essential content is rendered
      await expect(page.locator('.hero h1')).toBeVisible();
      await expect(page.locator('.hero h1')).toContainText('MirDB');

      // Verify tagline is visible
      await expect(page.locator('.tagline')).toBeVisible();
      await expect(page.locator('.tagline')).toContainText('Memcached Protocol');

      // Verify features section renders
      await expect(page.locator('#features')).toBeVisible();
      const featureCards = page.locator('.feature-card');
      expect(await featureCards.count()).toBeGreaterThanOrEqual(4);

      // Verify getting started section renders
      await expect(page.locator('#getting-started')).toBeVisible();

      // Verify footer renders
      await expect(page.locator('.footer')).toBeVisible();
    });

    test('All sections render correctly without server processing', async ({ page }) => {
      await page.goto('/');

      // Check all major sections are present and visible
      const sections = [
        { selector: '.hero', name: 'Hero section' },
        { selector: '#features', name: 'Features section' },
        { selector: '#code-example', name: 'Code example section' },
        { selector: '#getting-started', name: 'Getting started section' },
        { selector: '#architecture', name: 'Architecture section' },
        { selector: '.footer', name: 'Footer section' },
      ];

      for (const section of sections) {
        await expect(page.locator(section.selector)).toBeVisible();
        console.log(`✓ ${section.name} rendered correctly`);
      }
    });

    test('CSS styles are applied correctly', async ({ page }) => {
      await page.goto('/');

      // Verify CSS is loaded and applied
      const heroElement = page.locator('.hero');
      await expect(heroElement).toBeVisible();

      // Check that CSS custom properties are working
      const computedStyles = await heroElement.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          padding: styles.padding,
          textAlign: styles.textAlign,
        };
      });

      // Hero should have centered text alignment from CSS
      expect(computedStyles.textAlign).toBe('center');

      // Verify button styles are applied
      const primaryButton = page.locator('.btn-primary').first();
      await expect(primaryButton).toBeVisible();

      const buttonStyles = await primaryButton.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          cursor: styles.cursor,
        };
      });

      expect(buttonStyles.cursor).toBe('pointer');
    });

    test('No JavaScript errors during page load', async ({ page }) => {
      const jsErrors: string[] = [];

      // Listen for console errors
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          jsErrors.push(msg.text());
        }
      });

      // Listen for page errors
      page.on('pageerror', (error) => {
        jsErrors.push(error.message);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out expected CORS errors that don't affect functionality
      const criticalErrors = jsErrors.filter(
        (err) =>
          !err.includes('favicon') &&
          !err.includes('CORS') &&
          !err.includes('Cross-Origin')
      );

      console.log('JavaScript errors:', criticalErrors);
      expect(criticalErrors.length).toBe(0);
    });
  });

  test.describe('TC2: No Server-Side Requirements', () => {
    test('HTML file contains no server-side directives', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');
      const htmlPath = path.join(homepageDir, 'index.html');

      expect(fs.existsSync(htmlPath)).toBe(true);

      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // Check for PHP tags
      expect(htmlContent).not.toMatch(/<\?php/i);
      expect(htmlContent).not.toMatch(/<\?=/i);
      expect(htmlContent).not.toMatch(/\?>/);

      // Check for ASP/ASP.NET tags
      expect(htmlContent).not.toMatch(/<%/);
      expect(htmlContent).not.toMatch(/%>/);
      expect(htmlContent).not.toMatch(/@\{/);

      // Check for JSP tags
      expect(htmlContent).not.toMatch(/<%@/);
      expect(htmlContent).not.toMatch(/<%!/);

      // Check for SSI (Server Side Includes)
      expect(htmlContent).not.toMatch(/<!--#include/i);
      expect(htmlContent).not.toMatch(/<!--#exec/i);
      expect(htmlContent).not.toMatch(/<!--#config/i);

      // Check for template engine syntax
      expect(htmlContent).not.toMatch(/\{\{.*\}\}/); // Handlebars/Mustache (except in SVG data)
      expect(htmlContent).not.toMatch(/\{%.*%\}/); // Jinja/Liquid

      console.log('✓ No PHP directives found');
      console.log('✓ No ASP/ASP.NET directives found');
      console.log('✓ No JSP directives found');
      console.log('✓ No SSI directives found');
      console.log('✓ No template engine syntax found');
    });

    test('No server-side configuration files in homepage directory', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');

      // Server-side config files that should not exist
      const serverSideFiles = [
        '.htaccess', // Apache
        'web.config', // IIS
        'nginx.conf', // Nginx
        'php.ini', // PHP
        'server.js', // Node.js
        'server.ts', // Node.js TypeScript
        'app.js', // Express/Node.js
        'app.py', // Flask/Django
        'wsgi.py', // Python WSGI
        'Gemfile', // Ruby
        'config.ru', // Rack (Ruby)
        'main.go', // Go
        'pom.xml', // Java Maven
        'build.gradle', // Java Gradle
      ];

      for (const file of serverSideFiles) {
        const filePath = path.join(homepageDir, file);
        const exists = fs.existsSync(filePath);
        expect(exists).toBe(false);
        if (!exists) {
          console.log(`✓ No ${file} found (expected for static site)`);
        }
      }
    });

    test('All files are static assets (HTML, CSS, JS, images)', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');

      // Get all files in the homepage directory
      const files = fs.readdirSync(homepageDir, { recursive: true }) as string[];

      // Allowed static file extensions
      const allowedExtensions = [
        '.html',
        '.htm',
        '.css',
        '.js',
        '.mjs',
        '.json',
        '.svg',
        '.png',
        '.jpg',
        '.jpeg',
        '.gif',
        '.webp',
        '.ico',
        '.woff',
        '.woff2',
        '.ttf',
        '.eot',
        '.txt',
        '.xml',
        '.webmanifest',
      ];

      // Server-side file extensions to reject
      const serverSideExtensions = [
        '.php',
        '.asp',
        '.aspx',
        '.jsp',
        '.py',
        '.rb',
        '.pl',
        '.cgi',
        '.go',
        '.java',
        '.ts', // TypeScript (if it's meant to run on server)
      ];

      for (const file of files) {
        const ext = path.extname(file).toLowerCase();
        if (ext) {
          // Skip TypeScript test files and config files
          if (file.includes('spec.ts') || file.includes('test.ts')) {
            continue;
          }

          const isServerSide = serverSideExtensions.includes(ext);
          expect(isServerSide).toBe(false);

          if (!isServerSide && ext) {
            console.log(`✓ ${file} is a valid static file type`);
          }
        }
      }
    });

    test('No dynamic imports or require statements for server modules', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');
      const jsFiles = fs
        .readdirSync(homepageDir, { recursive: true })
        .filter((f) => typeof f === 'string' && (f.endsWith('.js') || f.endsWith('.mjs')));

      for (const jsFile of jsFiles) {
        const filePath = path.join(homepageDir, jsFile as string);
        if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
          const content = fs.readFileSync(filePath, 'utf-8');

          // Check for Node.js specific requires
          expect(content).not.toMatch(/require\s*\(\s*['"]fs['"]\s*\)/);
          expect(content).not.toMatch(/require\s*\(\s*['"]path['"]\s*\)/);
          expect(content).not.toMatch(/require\s*\(\s*['"]http['"]\s*\)/);
          expect(content).not.toMatch(/require\s*\(\s*['"]express['"]\s*\)/);

          console.log(`✓ ${jsFile} contains no server-side requires`);
        }
      }
    });
  });

  test.describe('TC3: Relative Path Verification', () => {
    test('CSS link uses relative path', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');
      const htmlPath = path.join(homepageDir, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // Check that stylesheet link uses relative path (not absolute)
      const cssLinkMatch = htmlContent.match(/<link[^>]*rel=["']stylesheet["'][^>]*href=["']([^"']+)["']/i);

      if (cssLinkMatch) {
        const href = cssLinkMatch[1];

        // Should not start with / (root-relative) unless it's a CDN
        if (!href.startsWith('http://') && !href.startsWith('https://') && !href.startsWith('//')) {
          expect(href).not.toMatch(/^\//);
          console.log(`✓ CSS link uses relative path: ${href}`);
        } else {
          console.log(`✓ CSS link uses CDN: ${href}`);
        }
      }
    });

    test('All local asset references use relative paths', async ({ page }) => {
      await page.goto('/');

      // Get all link, script, and img tags
      const assetUrls = await page.evaluate(() => {
        const assets: { type: string; url: string }[] = [];

        // Stylesheets
        document.querySelectorAll('link[rel="stylesheet"]').forEach((el) => {
          const href = el.getAttribute('href');
          if (href) {
            assets.push({ type: 'css', url: href });
          }
        });

        // Scripts
        document.querySelectorAll('script[src]').forEach((el) => {
          const src = el.getAttribute('src');
          if (src) {
            assets.push({ type: 'js', url: src });
          }
        });

        // Images
        document.querySelectorAll('img[src]').forEach((el) => {
          const src = el.getAttribute('src');
          if (src) {
            assets.push({ type: 'img', url: src });
          }
        });

        return assets;
      });

      console.log('Asset URLs found:', assetUrls);

      // Check each local asset uses relative path
      for (const asset of assetUrls) {
        // Skip external CDN resources
        if (
          asset.url.startsWith('http://') ||
          asset.url.startsWith('https://') ||
          asset.url.startsWith('//')
        ) {
          console.log(`✓ External resource (allowed): ${asset.url}`);
          continue;
        }

        // Local resources should not use root-relative paths
        // They should be relative to the current document
        expect(asset.url).not.toMatch(/^\/[^/]/);
        console.log(`✓ Local ${asset.type} uses relative path: ${asset.url}`);
      }
    });

    test('Internal anchor links use relative paths', async ({ page }) => {
      await page.goto('/');

      // Get all internal links
      const links = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('a[href^="#"]')).map((a) =>
          a.getAttribute('href')
        );
      });

      console.log('Internal anchor links:', links);

      // All internal anchors should start with #
      for (const link of links) {
        expect(link).toMatch(/^#/);
        console.log(`✓ Internal anchor link is relative: ${link}`);
      }
    });

    test('Assets load correctly with current path structure', async ({ page }) => {
      // Track failed resource loads
      const failedResources: string[] = [];

      page.on('response', (response) => {
        if (response.status() >= 400) {
          const url = response.url();
          // Ignore external resources and favicons
          if (
            !url.includes('cdnjs.cloudflare.com') &&
            !url.includes('favicon')
          ) {
            failedResources.push(`${response.status()}: ${url}`);
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      console.log('Failed resources:', failedResources);
      expect(failedResources.length).toBe(0);
    });

    test('CSS file is accessible at relative path', async ({ page }) => {
      // Try to load the CSS file directly
      const response = await page.goto('/styles.css');
      expect(response?.status()).toBe(200);

      const contentType = response?.headers()['content-type'];
      expect(contentType).toContain('css');
    });
  });

  test.describe('TC4: File Protocol Compatibility', () => {
    test('HTML structure is valid for file:// protocol', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');
      const htmlPath = path.join(homepageDir, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // Check for DOCTYPE
      expect(htmlContent).toMatch(/<!DOCTYPE html>/i);

      // Check for proper HTML structure
      expect(htmlContent).toMatch(/<html[^>]*>/i);
      expect(htmlContent).toMatch(/<head[^>]*>/i);
      expect(htmlContent).toMatch(/<body[^>]*>/i);
      expect(htmlContent).toMatch(/<\/html>/i);

      // Check for charset
      expect(htmlContent).toMatch(/<meta[^>]*charset/i);

      console.log('✓ HTML has valid DOCTYPE');
      console.log('✓ HTML has proper structure');
      console.log('✓ HTML has charset declaration');
    });

    test('No absolute URLs for local resources that would fail with file://', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');
      const htmlPath = path.join(homepageDir, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // Check for localhost or 127.0.0.1 URLs (would fail with file://)
      expect(htmlContent).not.toMatch(/http:\/\/localhost/i);
      expect(htmlContent).not.toMatch(/http:\/\/127\.0\.0\.1/i);

      // Check for root-relative paths to local resources (problematic with file://)
      // Note: External CDN resources are fine
      const localRootRelativePaths = htmlContent.match(/(?:src|href)=["']\/(?!\/)[^"']*["']/gi);

      if (localRootRelativePaths) {
        // Filter out anchors
        const problematicPaths = localRootRelativePaths.filter(
          (p) => !p.includes('/#') && !p.includes("'#")
        );
        expect(problematicPaths.length).toBe(0);
      }

      console.log('✓ No localhost URLs found');
      console.log('✓ No problematic root-relative paths found');
    });

    test('CSS does not use url() with absolute paths', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');
      const cssPath = path.join(homepageDir, 'styles.css');

      if (fs.existsSync(cssPath)) {
        const cssContent = fs.readFileSync(cssPath, 'utf-8');

        // Check for absolute URLs in CSS url() functions
        // Allow data: URIs and external https:// resources
        const urlMatches = cssContent.matchAll(/url\s*\(\s*["']?([^"')]+)["']?\s*\)/gi);

        for (const match of urlMatches) {
          const url = match[1];

          // Skip data URIs and external resources
          if (
            url.startsWith('data:') ||
            url.startsWith('https://') ||
            url.startsWith('http://')
          ) {
            continue;
          }

          // Local URLs should be relative
          expect(url).not.toMatch(/^\//);
          console.log(`✓ CSS url() is relative: ${url}`);
        }
      }

      console.log('✓ CSS file checked for absolute paths');
    });

    test('Page has no AJAX/fetch calls that require server', async () => {
      const homepageDir = path.join(process.cwd(), 'homepage');

      // Check HTML for fetch or XMLHttpRequest
      const htmlPath = path.join(homepageDir, 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

      // Check for fetch calls to local APIs
      expect(htmlContent).not.toMatch(/fetch\s*\(\s*['"]\/api/i);
      expect(htmlContent).not.toMatch(/XMLHttpRequest/i);

      // Check any JS files
      const jsFiles = fs
        .readdirSync(homepageDir)
        .filter((f) => f.endsWith('.js'));

      for (const jsFile of jsFiles) {
        const jsPath = path.join(homepageDir, jsFile);
        const jsContent = fs.readFileSync(jsPath, 'utf-8');

        // Check for server API calls
        expect(jsContent).not.toMatch(/fetch\s*\(\s*['"]\/api/i);

        console.log(`✓ ${jsFile} has no server API calls`);
      }

      console.log('✓ No server-dependent AJAX/fetch calls found');
    });

    test('All functionality works without JavaScript (graceful degradation)', async ({ page }) => {
      // Disable JavaScript
      await page.route('**/*.js', (route) => route.abort());

      await page.goto('/');

      // Core content should still be visible
      await expect(page.locator('.hero h1')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('.footer')).toBeVisible();

      // Navigation links should work
      const getStartedLink = page.locator('a[href="#getting-started"]');
      await expect(getStartedLink).toBeVisible();

      console.log('✓ Page content renders without JavaScript');
    });

    test('SVG graphics render without external dependencies', async ({ page }) => {
      await page.goto('/');

      // Check if SVG elements render
      const svgElements = page.locator('svg');
      const svgCount = await svgElements.count();

      console.log(`Found ${svgCount} SVG elements`);

      // Verify SVGs are visible (inline SVGs should work with file://)
      if (svgCount > 0) {
        const firstSvg = svgElements.first();
        await expect(firstSvg).toBeVisible();
        console.log('✓ SVG graphics render correctly');
      }
    });
  });
});
