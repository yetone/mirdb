// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Static Site Requirements Tests
 * Verifies NFR-4: Static site with no server-side dependencies
 */

test.describe('Static Site Requirements', () => {

  test.describe('Test Case 1: Build output contains only static files', () => {

    test('docs directory contains only static file types (HTML, CSS, JS, images)', async () => {
      const docsDir = path.join(process.cwd(), 'docs');

      // Allowed static file extensions
      const allowedExtensions = [
        '.html',
        '.css',
        '.js',
        '.png',
        '.jpg',
        '.jpeg',
        '.gif',
        '.svg',
        '.ico',
        '.webp',
        '.woff',
        '.woff2',
        '.ttf',
        '.eot',
        '.json',
        '.xml',
        '.txt',
        '.map'
      ];

      // Recursively get all files in the docs directory
      function getAllFiles(dirPath, arrayOfFiles = []) {
        const files = fs.readdirSync(dirPath);

        files.forEach(file => {
          const filePath = path.join(dirPath, file);
          if (fs.statSync(filePath).isDirectory()) {
            arrayOfFiles = getAllFiles(filePath, arrayOfFiles);
          } else {
            arrayOfFiles.push(filePath);
          }
        });

        return arrayOfFiles;
      }

      const allFiles = getAllFiles(docsDir);

      // Verify each file has an allowed extension
      const invalidFiles = allFiles.filter(file => {
        const ext = path.extname(file).toLowerCase();
        return !allowedExtensions.includes(ext);
      });

      expect(invalidFiles, `Found non-static files: ${invalidFiles.join(', ')}`).toHaveLength(0);

      // Verify at least one HTML file exists
      const htmlFiles = allFiles.filter(file => path.extname(file).toLowerCase() === '.html');
      expect(htmlFiles.length, 'Should have at least one HTML file').toBeGreaterThan(0);

      // Verify at least one CSS file exists
      const cssFiles = allFiles.filter(file => path.extname(file).toLowerCase() === '.css');
      expect(cssFiles.length, 'Should have at least one CSS file').toBeGreaterThan(0);
    });

    test('index.html exists in docs directory', async () => {
      const indexPath = path.join(process.cwd(), 'docs', 'index.html');
      const exists = fs.existsSync(indexPath);
      expect(exists, 'index.html should exist in docs directory').toBe(true);
    });

    test('styles.css exists in docs directory', async () => {
      const stylesPath = path.join(process.cwd(), 'docs', 'styles.css');
      const exists = fs.existsSync(stylesPath);
      expect(exists, 'styles.css should exist in docs directory').toBe(true);
    });

  });

  test.describe('Test Case 2: Page loads correctly from static file server', () => {

    test('page loads and displays main content', async ({ page }) => {
      // Navigate to the homepage served by the static file server
      const response = await page.goto('/');

      // Verify successful response
      expect(response.status()).toBe(200);

      // Verify content type is HTML
      const contentType = response.headers()['content-type'];
      expect(contentType).toContain('text/html');

      // Verify main content is visible
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('h1')).toHaveText('MirDB');
    });

    test('CSS styles are loaded correctly', async ({ page }) => {
      await page.goto('/');

      // Check that styles.css is loaded
      const stylesheets = await page.evaluate(() => {
        return Array.from(document.styleSheets).map(sheet => sheet.href);
      });

      const hasStylesCss = stylesheets.some(href => href && href.includes('styles.css'));
      expect(hasStylesCss, 'styles.css should be loaded').toBe(true);

      // Verify CSS is applied by checking computed styles
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();
    });

    test('page has valid HTML document structure', async ({ page }) => {
      await page.goto('/');

      // Check for valid document structure
      const doctype = await page.evaluate(() => {
        return document.doctype ? document.doctype.name : null;
      });
      expect(doctype).toBe('html');

      // Check for html element with lang attribute
      const htmlLang = await page.evaluate(() => {
        return document.documentElement.lang;
      });
      expect(htmlLang).toBeTruthy();

      // Check for head and body elements
      const hasHead = await page.evaluate(() => !!document.head);
      const hasBody = await page.evaluate(() => !!document.body);
      expect(hasHead).toBe(true);
      expect(hasBody).toBe(true);

      // Check for meta viewport
      const hasViewport = await page.evaluate(() => {
        return !!document.querySelector('meta[name="viewport"]');
      });
      expect(hasViewport).toBe(true);
    });

    test('all internal links and resources load correctly', async ({ page }) => {
      const failedResources = [];

      // Listen for failed resource requests
      page.on('requestfailed', request => {
        failedResources.push({
          url: request.url(),
          failure: request.failure()?.errorText
        });
      });

      await page.goto('/');

      // Wait for network to be idle
      await page.waitForLoadState('networkidle');

      // Filter out only internal resource failures (not external like GitHub)
      const internalFailures = failedResources.filter(r =>
        !r.url.includes('github.com') &&
        !r.url.includes('githubusercontent.com')
      );

      expect(internalFailures, `Failed to load internal resources: ${JSON.stringify(internalFailures)}`).toHaveLength(0);
    });

  });

  test.describe('Test Case 3: No server-side code in output', () => {

    test('no PHP files in docs directory', async () => {
      const docsDir = path.join(process.cwd(), 'docs');

      function findFilesWithExtension(dirPath, extension, arrayOfFiles = []) {
        const files = fs.readdirSync(dirPath);

        files.forEach(file => {
          const filePath = path.join(dirPath, file);
          if (fs.statSync(filePath).isDirectory()) {
            arrayOfFiles = findFilesWithExtension(filePath, extension, arrayOfFiles);
          } else if (file.endsWith(extension)) {
            arrayOfFiles.push(filePath);
          }
        });

        return arrayOfFiles;
      }

      const phpFiles = findFilesWithExtension(docsDir, '.php');
      expect(phpFiles, 'Should not have PHP files in docs directory').toHaveLength(0);
    });

    test('no Node.js server files in docs directory', async () => {
      const docsDir = path.join(process.cwd(), 'docs');

      // Check for common Node.js server file patterns
      const serverFilePatterns = [
        'server.js',
        'app.js',
        'index.js',
        'server.ts',
        'app.ts',
        'index.ts',
        'express.js',
        'koa.js',
        'fastify.js',
        'package.json'
      ];

      function checkForServerFiles(dirPath, patterns) {
        const files = fs.readdirSync(dirPath);
        const foundServerFiles = [];

        files.forEach(file => {
          const filePath = path.join(dirPath, file);
          if (fs.statSync(filePath).isDirectory()) {
            foundServerFiles.push(...checkForServerFiles(filePath, patterns));
          } else if (patterns.includes(file.toLowerCase())) {
            foundServerFiles.push(filePath);
          }
        });

        return foundServerFiles;
      }

      const serverFiles = checkForServerFiles(docsDir, serverFilePatterns);
      expect(serverFiles, `Found server-side files: ${serverFiles.join(', ')}`).toHaveLength(0);
    });

    test('no Python server files in docs directory', async () => {
      const docsDir = path.join(process.cwd(), 'docs');

      function findFilesWithExtension(dirPath, extension, arrayOfFiles = []) {
        const files = fs.readdirSync(dirPath);

        files.forEach(file => {
          const filePath = path.join(dirPath, file);
          if (fs.statSync(filePath).isDirectory()) {
            arrayOfFiles = findFilesWithExtension(filePath, extension, arrayOfFiles);
          } else if (file.endsWith(extension)) {
            arrayOfFiles.push(filePath);
          }
        });

        return arrayOfFiles;
      }

      const pyFiles = findFilesWithExtension(docsDir, '.py');
      expect(pyFiles, 'Should not have Python files in docs directory').toHaveLength(0);
    });

    test('no Ruby server files in docs directory', async () => {
      const docsDir = path.join(process.cwd(), 'docs');

      function findFilesWithExtension(dirPath, extension, arrayOfFiles = []) {
        const files = fs.readdirSync(dirPath);

        files.forEach(file => {
          const filePath = path.join(dirPath, file);
          if (fs.statSync(filePath).isDirectory()) {
            arrayOfFiles = findFilesWithExtension(filePath, extension, arrayOfFiles);
          } else if (file.endsWith(extension)) {
            arrayOfFiles.push(filePath);
          }
        });

        return arrayOfFiles;
      }

      const rbFiles = findFilesWithExtension(docsDir, '.rb');
      expect(rbFiles, 'Should not have Ruby files in docs directory').toHaveLength(0);
    });

    test('no ASP.NET files in docs directory', async () => {
      const docsDir = path.join(process.cwd(), 'docs');

      function findFilesWithPatterns(dirPath, patterns, arrayOfFiles = []) {
        const files = fs.readdirSync(dirPath);

        files.forEach(file => {
          const filePath = path.join(dirPath, file);
          if (fs.statSync(filePath).isDirectory()) {
            arrayOfFiles = findFilesWithPatterns(filePath, patterns, arrayOfFiles);
          } else if (patterns.some(pattern => file.endsWith(pattern))) {
            arrayOfFiles.push(filePath);
          }
        });

        return arrayOfFiles;
      }

      const aspFiles = findFilesWithPatterns(docsDir, ['.aspx', '.asp', '.cshtml', '.vbhtml']);
      expect(aspFiles, 'Should not have ASP.NET files in docs directory').toHaveLength(0);
    });

    test('no JSP files in docs directory', async () => {
      const docsDir = path.join(process.cwd(), 'docs');

      function findFilesWithExtension(dirPath, extension, arrayOfFiles = []) {
        const files = fs.readdirSync(dirPath);

        files.forEach(file => {
          const filePath = path.join(dirPath, file);
          if (fs.statSync(filePath).isDirectory()) {
            arrayOfFiles = findFilesWithExtension(filePath, extension, arrayOfFiles);
          } else if (file.endsWith(extension)) {
            arrayOfFiles.push(filePath);
          }
        });

        return arrayOfFiles;
      }

      const jspFiles = findFilesWithExtension(docsDir, '.jsp');
      expect(jspFiles, 'Should not have JSP files in docs directory').toHaveLength(0);
    });

    test('HTML files do not contain server-side processing directives', async () => {
      const docsDir = path.join(process.cwd(), 'docs');
      const indexHtmlPath = path.join(docsDir, 'index.html');

      const content = fs.readFileSync(indexHtmlPath, 'utf8');

      // Check for PHP tags
      expect(content).not.toMatch(/<\?php/i);
      expect(content).not.toMatch(/<\?=/i);

      // Check for ASP tags
      expect(content).not.toMatch(/<%/);

      // Check for SSI directives
      expect(content).not.toMatch(/<!--\s*#include/i);
      expect(content).not.toMatch(/<!--\s*#exec/i);

      // Check for Jinja2/Django template tags
      expect(content).not.toMatch(/\{%\s*\w+/);
      expect(content).not.toMatch(/\{\{\s*\w+\s*\}\}/);
    });

    test('no node_modules directory in docs', async () => {
      const nodeModulesPath = path.join(process.cwd(), 'docs', 'node_modules');
      const exists = fs.existsSync(nodeModulesPath);
      expect(exists, 'node_modules should not exist in docs directory').toBe(false);
    });

  });

});
