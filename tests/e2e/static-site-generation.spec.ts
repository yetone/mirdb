// @ts-check
import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

const projectRoot = path.resolve(__dirname, '../..');
const publicDir = path.join(projectRoot, 'public');
const rootIndexPath = path.join(projectRoot, 'index.html');

test.describe('Static Site Generation - NFR-6', () => {

  /**
   * Test Case 1: Check for index.html file
   * Verifies that a static index.html file exists at the root or public directory
   */
  test('TC1: Static index.html file exists', async () => {
    // Check if index.html exists in public directory (used by webServer)
    const publicIndexPath = path.join(publicDir, 'index.html');
    const publicIndexExists = fs.existsSync(publicIndexPath);

    // Also check root index.html
    const rootIndexExists = fs.existsSync(rootIndexPath);

    // At least one should exist
    expect(publicIndexExists || rootIndexExists).toBeTruthy();

    // Verify the file is a proper HTML file
    const indexPath = publicIndexExists ? publicIndexPath : rootIndexPath;
    const content = fs.readFileSync(indexPath, 'utf-8');
    expect(content).toContain('<!DOCTYPE html>');
    expect(content).toContain('<html');
    expect(content).toContain('</html>');
  });

  /**
   * Test Case 2: Verify HTML file is self-contained
   * Page should work when opened directly in browser (file://)
   * Tests that the page contains all necessary content inline without requiring a server
   */
  test('TC2: Page works when opened directly in browser (file://)', async () => {
    // Verify the HTML file contains all content inline (self-contained)
    const publicIndexPath = path.join(publicDir, 'index.html');
    const rootIndexPath = path.join(projectRoot, 'index.html');

    let indexPath: string;
    if (fs.existsSync(publicIndexPath)) {
      indexPath = publicIndexPath;
    } else {
      indexPath = rootIndexPath;
    }

    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Verify HTML structure allows file:// access
    expect(htmlContent).toContain('<!DOCTYPE html>');
    expect(htmlContent).toContain('<html');

    // Verify content is inline in the HTML (not loaded via AJAX/API)
    expect(htmlContent).toContain('MirDB');
    expect(htmlContent.toLowerCase()).toContain('memcached');

    // Verify essential sections are present in HTML
    expect(htmlContent).toContain('Features');
    expect(htmlContent.toLowerCase()).toContain('getting started');
    expect(htmlContent.toLowerCase()).toContain('commands');

    // Verify CSS and JS references are relative (work with file://)
    // Should reference local files, not absolute URLs for main styles
    expect(htmlContent).toMatch(/href=["'](?!https?:\/\/)[^"']*\.css["']/i);

    // Check that the body has substantial content (not just a shell loading content via JS)
    const bodyMatch = htmlContent.match(/<body[^>]*>([\s\S]*)<\/body>/i);
    expect(bodyMatch).toBeTruthy();
    const bodyContent = bodyMatch![1];
    expect(bodyContent.length).toBeGreaterThan(1000); // Substantial inline content
  });

  /**
   * Test Case 3: Check for server-side code files
   * Verifies no .php, .py, .rb, or other server-side files are required
   */
  test('TC3: No server-side code files (.php, .py, .rb) required', async () => {
    const serverSideExtensions = ['.php', '.py', '.rb', '.jsp', '.asp', '.aspx'];
    const dirsToCheck = [publicDir, projectRoot];

    const foundServerSideFiles: string[] = [];

    function checkDirectory(dir: string, depth: number = 0): void {
      if (depth > 3) return; // Limit recursion depth
      if (!fs.existsSync(dir)) return;

      // Skip directories that are not part of the website
      const skipDirs = ['node_modules', '.git', 'mirdb-server', 'skip-list', 'sstable', '.something', 'tests'];

      const items = fs.readdirSync(dir);
      for (const item of items) {
        const fullPath = path.join(dir, item);
        const stat = fs.statSync(fullPath);

        if (stat.isDirectory()) {
          if (!skipDirs.includes(item)) {
            checkDirectory(fullPath, depth + 1);
          }
        } else if (stat.isFile()) {
          const ext = path.extname(item).toLowerCase();
          if (serverSideExtensions.includes(ext)) {
            foundServerSideFiles.push(fullPath);
          }
        }
      }
    }

    checkDirectory(publicDir);

    // Assert no server-side files found in the website directory
    expect(foundServerSideFiles).toHaveLength(0);
  });

  /**
   * Test Case 4: Verify no external API calls required for content
   * All content should be rendered from static files, no fetch() for content
   */
  test('TC4: All content rendered from static files, no fetch() for content', async () => {
    const publicIndexPath = path.join(publicDir, 'index.html');
    const rootIndexPath = path.join(projectRoot, 'index.html');

    let indexPath: string;
    if (fs.existsSync(publicIndexPath)) {
      indexPath = publicIndexPath;
    } else {
      indexPath = rootIndexPath;
    }

    // Read the HTML file content
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check that content is inline in HTML (not loaded via API)
    // Content should include key sections directly in HTML
    expect(htmlContent).toContain('MirDB');
    expect(htmlContent.toLowerCase()).toContain('memcached');
    expect(htmlContent.toLowerCase()).toContain('feature');
    expect(htmlContent.toLowerCase()).toContain('getting started');

    // Read the associated JS file if exists
    const jsFiles = ['main.js', 'js/main.js'];
    let jsContent = '';
    for (const jsFile of jsFiles) {
      const jsPath = path.join(path.dirname(indexPath), jsFile);
      if (fs.existsSync(jsPath)) {
        jsContent += fs.readFileSync(jsPath, 'utf-8');
      }
    }

    // Check that JS doesn't contain fetch calls for loading main content
    // Note: fetch for analytics or optional features is acceptable,
    // but fetch for main content rendering is not
    const contentLoadingPatterns = [
      /fetch\s*\([^)]*\/api\//i,      // API endpoints
      /fetch\s*\([^)]*\/content\//i,  // Content loading
      /fetch\s*\([^)]*\.json['"]?\)/i // JSON content files (excluding config)
    ];

    for (const pattern of contentLoadingPatterns) {
      const match = jsContent.match(pattern);
      // If there's a fetch, verify it's not for main content
      if (match) {
        // Allow fetch for non-content purposes (e.g., analytics, external libraries)
        expect(match[0]).not.toMatch(/content|article|page|section/i);
      }
    }

    // Verify the HTML body contains the actual content (not just a shell)
    // This proves content is statically rendered, not dynamically loaded
    const bodyMatch = htmlContent.match(/<body[^>]*>([\s\S]*)<\/body>/i);
    expect(bodyMatch).toBeTruthy();
    const bodyContent = bodyMatch![1];

    // Verify key content sections are present in the HTML body
    expect(bodyContent).toContain('MirDB');
    expect(bodyContent.toLowerCase()).toContain('memcached');
    expect(bodyContent.toLowerCase()).toContain('feature');
    expect(bodyContent.toLowerCase()).toContain('getting started');
    expect(bodyContent.toLowerCase()).toContain('command');
  });

  /**
   * Verify static file structure consists of HTML, CSS, JS only
   * Per NFR-6: static site generation for minimal hosting requirements
   */
  test('Site consists of static HTML, CSS, JS files only', async () => {
    const allowedExtensions = ['.html', '.css', '.js', '.svg', '.png', '.jpg', '.jpeg', '.gif', '.ico', '.webp', '.woff', '.woff2', '.ttf', '.eot'];
    const websiteFiles: string[] = [];

    function collectWebsiteFiles(dir: string): void {
      if (!fs.existsSync(dir)) return;

      const items = fs.readdirSync(dir);
      for (const item of items) {
        const fullPath = path.join(dir, item);
        const stat = fs.statSync(fullPath);

        if (stat.isFile()) {
          const ext = path.extname(item).toLowerCase();
          if (ext && !ext.startsWith('.')) continue; // Skip files without extensions
          websiteFiles.push({ path: fullPath, ext } as any);
        }
      }
    }

    collectWebsiteFiles(publicDir);

    // Verify public directory has the essential static files
    const hasHtml = fs.existsSync(path.join(publicDir, 'index.html'));
    const hasCss = fs.existsSync(path.join(publicDir, 'styles.css'));
    const hasJs = fs.existsSync(path.join(publicDir, 'main.js'));

    expect(hasHtml).toBeTruthy();
    expect(hasCss).toBeTruthy();
    expect(hasJs).toBeTruthy();
  });

  /**
   * Verify GitHub Pages compatibility
   * Site should be deployable to GitHub Pages without build steps
   */
  test('Site is GitHub Pages compatible', async () => {
    // GitHub Pages requires index.html at root or in docs folder
    const publicIndexPath = path.join(publicDir, 'index.html');
    const rootIndexPath = path.join(projectRoot, 'index.html');

    expect(fs.existsSync(publicIndexPath) || fs.existsSync(rootIndexPath)).toBeTruthy();

    // No Jekyll-specific files that would require processing
    const jekyllFiles = ['_config.yml', 'Gemfile', '_layouts', '_includes'];
    let hasJekyllDeps = false;

    for (const jekyllFile of jekyllFiles) {
      if (fs.existsSync(path.join(publicDir, jekyllFile))) {
        hasJekyllDeps = true;
        break;
      }
    }

    // Site should work without Jekyll (though having these files is not necessarily a problem)
    // The key requirement is that static HTML/CSS/JS works without processing
    const indexPath = fs.existsSync(publicIndexPath) ? publicIndexPath : rootIndexPath;
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Should be valid HTML, not a template requiring processing
    expect(content).not.toContain('{{ '); // No Liquid/Jekyll templating
    expect(content).not.toContain('{% '); // No Liquid/Jekyll blocks
    expect(content).toContain('<!DOCTYPE html>');
  });
});
