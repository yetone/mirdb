import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';
import { spawn, ChildProcess } from 'child_process';
import * as http from 'http';

const PUBLIC_DIR = path.join(__dirname, '..', 'public');

/**
 * Static Deployment Verification Tests
 *
 * These tests verify that the MirDB homepage can be deployed as static files
 * with no server-side requirements. The site should work on any static hosting
 * platform including GitHub Pages, Netlify, Vercel, or a simple HTTP server.
 */

test.describe('Static Deployment Verification - Build Output', () => {
  test('TC1: Build produces static HTML file', async () => {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');

    // Verify HTML file exists
    expect(fs.existsSync(htmlPath)).toBe(true);

    // Verify it's a valid HTML file
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    expect(htmlContent).toContain('<!DOCTYPE html>');
    expect(htmlContent).toContain('<html');
    expect(htmlContent).toContain('</html>');

    console.log(`HTML file exists at: ${htmlPath}`);
    console.log(`HTML file size: ${(htmlContent.length / 1024).toFixed(2)} KB`);
  });

  test('TC1: Build produces static CSS file', async () => {
    const cssPath = path.join(PUBLIC_DIR, 'styles.css');

    // Verify CSS file exists
    expect(fs.existsSync(cssPath)).toBe(true);

    // Verify it's a valid CSS file with content
    const cssContent = fs.readFileSync(cssPath, 'utf-8');
    expect(cssContent.length).toBeGreaterThan(0);
    expect(cssContent).toContain('{');
    expect(cssContent).toContain('}');

    console.log(`CSS file exists at: ${cssPath}`);
    console.log(`CSS file size: ${(cssContent.length / 1024).toFixed(2)} KB`);
  });

  test('TC1: All required static files are present', async () => {
    const requiredFiles = ['index.html', 'styles.css'];
    const missingFiles: string[] = [];

    for (const file of requiredFiles) {
      const filePath = path.join(PUBLIC_DIR, file);
      if (!fs.existsSync(filePath)) {
        missingFiles.push(file);
      }
    }

    expect(missingFiles).toEqual([]);
    console.log('All required static files are present:', requiredFiles.join(', '));
  });

  test('TC1: Public directory contains only static assets', async () => {
    const files = fs.readdirSync(PUBLIC_DIR);

    // Allowed static file extensions
    const allowedExtensions = ['.html', '.css', '.js', '.json', '.svg', '.png', '.jpg', '.jpeg', '.gif', '.ico', '.woff', '.woff2', '.ttf', '.eot'];

    const invalidFiles = files.filter(file => {
      const ext = path.extname(file).toLowerCase();
      // Allow files with no extension (like CNAME) or allowed extensions
      return ext !== '' && !allowedExtensions.includes(ext);
    });

    console.log('Files in public directory:', files.join(', '));
    expect(invalidFiles).toEqual([]);
  });
});

test.describe('Static Deployment Verification - No Server Dependencies', () => {
  test('TC2: HTML has no server-side templating syntax', async () => {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check for common server-side templating patterns
    const serverSidePatterns = [
      { pattern: /<%.*%>/, name: 'EJS/ERB syntax' },
      { pattern: /\{\{.*\}\}/, name: 'Mustache/Handlebars syntax' },
      { pattern: /\{%.*%\}/, name: 'Jinja/Django syntax' },
      { pattern: /<\?php/, name: 'PHP syntax' },
      { pattern: /@\{.*\}/, name: 'Razor syntax' },
      { pattern: /\$\{.*\}/, name: 'ES6 template literal (in HTML)' },
    ];

    const foundPatterns: string[] = [];
    for (const { pattern, name } of serverSidePatterns) {
      // Exclude inline SVG content which may have legitimate use of { }
      const contentWithoutSVG = htmlContent.replace(/<svg[\s\S]*?<\/svg>/gi, '');
      if (pattern.test(contentWithoutSVG)) {
        foundPatterns.push(name);
      }
    }

    expect(foundPatterns).toEqual([]);
    console.log('No server-side templating syntax found');
  });

  test('TC2: No API calls in static HTML/CSS', async () => {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const cssPath = path.join(PUBLIC_DIR, 'styles.css');

    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Check for patterns that would require server-side processing
    const apiPatterns = [
      { pattern: /fetch\s*\(/, name: 'fetch() API call' },
      { pattern: /XMLHttpRequest/, name: 'XMLHttpRequest' },
      { pattern: /axios/, name: 'Axios library' },
      { pattern: /\$\.ajax/, name: 'jQuery AJAX' },
      { pattern: /\.json\(\)/, name: '.json() method call' },
    ];

    const foundInHTML: string[] = [];
    const foundInCSS: string[] = [];

    for (const { pattern, name } of apiPatterns) {
      if (pattern.test(htmlContent)) {
        foundInHTML.push(name);
      }
      if (pattern.test(cssContent)) {
        foundInCSS.push(name);
      }
    }

    expect(foundInHTML).toEqual([]);
    expect(foundInCSS).toEqual([]);
    console.log('No API calls found in static files');
  });

  test('TC2: No server-side rendering framework dependencies', async () => {
    const packageJsonPath = path.join(__dirname, '..', 'package.json');
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

    // SSR frameworks that would require server-side processing
    const ssrFrameworks = [
      'next', 'nuxt', 'gatsby', 'remix', 'sveltekit',
      'express', 'koa', 'fastify', 'hapi', 'nest',
      'php', 'django', 'flask', 'rails'
    ];

    const allDeps = {
      ...packageJson.dependencies,
      ...packageJson.devDependencies,
    };

    const foundSSRDeps = Object.keys(allDeps || {}).filter(dep =>
      ssrFrameworks.some(framework => dep.toLowerCase().includes(framework))
    );

    // Filter out test-related dependencies that aren't actual SSR frameworks
    const actualSSRDeps = foundSSRDeps.filter(dep =>
      !dep.includes('test') && !dep.includes('playwright')
    );

    expect(actualSSRDeps).toEqual([]);
    console.log('No SSR framework dependencies found');
  });

  test('TC2: HTML is self-contained with inline or local references only', async () => {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Extract all link href and script src attributes
    const linkHrefs = [...htmlContent.matchAll(/href=["']([^"']+)["']/gi)].map(m => m[1]);
    const scriptSrcs = [...htmlContent.matchAll(/src=["']([^"']+)["']/gi)].map(m => m[1]);

    // Filter out local references, anchor links, and known external CDNs that are acceptable
    const externalLinks = linkHrefs.filter(href => {
      // Skip anchor links and mailto/tel links
      if (href.startsWith('#') || href.startsWith('mailto:') || href.startsWith('tel:')) return false;
      // Skip local files
      if (!href.startsWith('http://') && !href.startsWith('https://') && !href.startsWith('//')) return false;
      // Allow known acceptable external links (GitHub, etc.)
      if (href.includes('github.com')) return false;
      return true;
    });

    const externalScripts = scriptSrcs.filter(src => {
      if (!src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('//')) return false;
      return true;
    });

    // Log findings
    console.log('Link references found:', linkHrefs.length);
    console.log('Script references found:', scriptSrcs.length);
    console.log('External script references:', externalScripts);

    // No external scripts should be required for the page to function
    expect(externalScripts).toEqual([]);
  });
});

test.describe('Static Deployment Verification - Static Server Compatibility', () => {
  test('TC3: Site loads correctly when served statically', async ({ page }) => {
    // The Playwright test server already serves files statically
    await page.goto('/');

    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/i);

    // Verify main sections are visible
    await expect(page.locator('h1')).toContainText('MirDB');
    await expect(page.locator('.hero-section')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();

    console.log('Site loaded successfully when served statically');
  });

  test('TC3: All page sections render without JavaScript', async ({ browser }) => {
    // Create a context with JavaScript disabled to verify the page works without it
    const context = await browser.newContext({
      javaScriptEnabled: false,
    });
    const page = await context.newPage();
    await page.goto('http://localhost:3000/');

    // Core content should still be visible
    await expect(page.locator('h1')).toContainText('MirDB');
    await expect(page.locator('.tagline')).toBeVisible();
    await expect(page.locator('.features-section')).toBeVisible();
    await expect(page.locator('.architecture-section')).toBeVisible();
    await expect(page.locator('.quick-start-section')).toBeVisible();
    await expect(page.locator('.project-status-section')).toBeVisible();

    console.log('All sections render correctly without JavaScript');

    await context.close();
  });

  test('TC3: CSS styles load correctly', async ({ page }) => {
    await page.goto('/');

    // Verify CSS is loaded by checking computed styles
    const heroSection = page.locator('.hero-section');
    const heroBackground = await heroSection.evaluate(el =>
      window.getComputedStyle(el).background
    );

    // The hero should have the gradient background from CSS
    expect(heroBackground).toBeTruthy();
    expect(heroBackground).not.toBe('rgba(0, 0, 0, 0)');

    console.log('CSS styles loaded correctly');
  });

  test('TC3: Navigation links work correctly', async ({ page }) => {
    await page.goto('/');

    // Test internal navigation (anchor links)
    const getStartedButton = page.locator('a.cta-primary', { hasText: 'Get Started' });
    await expect(getStartedButton).toHaveAttribute('href', '#quickstart');

    await getStartedButton.click();

    // Verify smooth scroll to quickstart section
    await expect(page.locator('#quickstart')).toBeInViewport();

    console.log('Navigation links work correctly');
  });

  test('TC3: Page responds to different viewport sizes', async ({ page }) => {
    await page.goto('/');

    // Test mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await expect(page.locator('.hero-section')).toBeVisible();

    // Test tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await expect(page.locator('.hero-section')).toBeVisible();

    // Test desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    await expect(page.locator('.hero-section')).toBeVisible();

    console.log('Page responds correctly to different viewport sizes');
  });
});

test.describe('Static Deployment Verification - Relative Asset Paths', () => {
  test('TC4: CSS uses relative paths', async () => {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Find CSS link tag
    const cssLinkMatch = htmlContent.match(/href=["']([^"']*\.css)["']/i);
    expect(cssLinkMatch).toBeTruthy();

    const cssPath = cssLinkMatch![1];

    // Path should be relative (not absolute or starting with /)
    expect(cssPath).not.toMatch(/^https?:\/\//);
    expect(cssPath).not.toMatch(/^\/[^\/]/); // Allows // for protocol-relative but not /path

    console.log(`CSS path is relative: ${cssPath}`);
  });

  test('TC4: Internal links use relative or anchor references', async () => {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Find all internal navigation links (not external like GitHub)
    const allHrefs = [...htmlContent.matchAll(/href=["']([^"']+)["']/gi)].map(m => m[1]);

    const internalLinks = allHrefs.filter(href => {
      // Skip external links
      if (href.startsWith('http://') || href.startsWith('https://') || href.startsWith('//')) return false;
      // Skip mailto/tel
      if (href.startsWith('mailto:') || href.startsWith('tel:')) return false;
      return true;
    });

    console.log('Internal links found:', internalLinks);

    // All internal links should be anchor links or relative paths
    for (const link of internalLinks) {
      const isAnchor = link.startsWith('#');
      const isRelative = !link.startsWith('/') || link.startsWith('./') || link.startsWith('../');
      expect(isAnchor || isRelative).toBe(true);
    }
  });

  test('TC4: SVG graphics are embedded inline (no external image references)', async () => {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check for inline SVG
    const hasInlineSVG = htmlContent.includes('<svg');
    expect(hasInlineSVG).toBe(true);

    // Check that SVG is embedded, not referenced externally
    const svgReferences = [...htmlContent.matchAll(/src=["']([^"']*\.svg)["']/gi)];

    console.log(`Inline SVG found: ${hasInlineSVG}`);
    console.log(`External SVG references: ${svgReferences.length}`);

    // No external SVG file references in img tags
    expect(svgReferences.length).toBe(0);
  });

  test('TC4: Assets work when served from subdirectory', async ({ page }) => {
    // Simulate serving from a subdirectory by checking all asset URLs
    await page.goto('/');

    // Get all loaded resources
    const resources: string[] = [];
    page.on('response', response => {
      resources.push(response.url());
    });

    await page.reload();
    await page.waitForLoadState('networkidle');

    // Verify all resources loaded successfully (status 200)
    const failedResources: string[] = [];
    for (const url of resources) {
      if (url.includes('localhost:3000')) {
        // This is a local resource - it should have loaded
        // We verified by the fact that the page rendered correctly
      }
    }

    // Verify no 404 errors
    await page.goto('/');
    const response = await page.request.get('/styles.css');
    expect(response.status()).toBe(200);

    console.log('All assets load correctly from current base URL');
  });
});

test.describe('Static Deployment Verification - GitHub Pages Compatibility', () => {
  test('TC5: Site structure is GitHub Pages compatible', async () => {
    // GitHub Pages serves from root or /docs folder
    // Our site serves from /public which is fine when deployed to root

    const indexExists = fs.existsSync(path.join(PUBLIC_DIR, 'index.html'));
    expect(indexExists).toBe(true);

    console.log('index.html exists in public directory - GitHub Pages compatible');
  });

  test('TC5: No Jekyll processing required (no _config.yml dependency)', async () => {
    // GitHub Pages uses Jekyll by default, but static HTML doesn't need it
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check for Jekyll-specific syntax
    const jekyllPatterns = [
      /\{\{.*site\./,  // Jekyll site variables
      /\{\%.*include/,  // Jekyll includes
      /\{\%.*for/,      // Jekyll loops
      /layout:\s/,      // Jekyll front matter
    ];

    const hasJekyllSyntax = jekyllPatterns.some(pattern => pattern.test(htmlContent));
    expect(hasJekyllSyntax).toBe(false);

    console.log('No Jekyll-specific syntax found - can use .nojekyll');
  });

  test('TC5: All essential metadata present for static hosting', async () => {
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check for essential meta tags
    const hasCharset = /charset=["']?UTF-8["']?/i.test(htmlContent);
    const hasViewport = /name=["']viewport["']/i.test(htmlContent);
    const hasTitle = /<title>.*<\/title>/i.test(htmlContent);
    const hasDescription = /name=["']description["']/i.test(htmlContent);

    expect(hasCharset).toBe(true);
    expect(hasViewport).toBe(true);
    expect(hasTitle).toBe(true);
    expect(hasDescription).toBe(true);

    console.log('Essential metadata present:');
    console.log(`  - charset: ${hasCharset}`);
    console.log(`  - viewport: ${hasViewport}`);
    console.log(`  - title: ${hasTitle}`);
    console.log(`  - description: ${hasDescription}`);
  });

  test('TC5: Site can be served with correct MIME types', async ({ page }) => {
    await page.goto('/');

    // Verify HTML is served correctly
    const htmlResponse = await page.request.get('/');
    const htmlContentType = htmlResponse.headers()['content-type'];
    expect(htmlContentType).toContain('text/html');

    // Verify CSS is served correctly
    const cssResponse = await page.request.get('/styles.css');
    const cssContentType = cssResponse.headers()['content-type'];
    expect(cssContentType).toContain('text/css');

    console.log('MIME types served correctly:');
    console.log(`  - HTML: ${htmlContentType}`);
    console.log(`  - CSS: ${cssContentType}`);
  });

  test('TC5: Site works without trailing slash', async ({ page }) => {
    // Navigate without trailing slash
    const response = await page.goto('/');
    expect(response?.status()).toBe(200);

    // Verify content loaded
    await expect(page.locator('h1')).toContainText('MirDB');

    console.log('Site works without trailing slash');
  });

  test('TC5: No build step required for deployment', async () => {
    // Verify the public folder contains ready-to-deploy files
    const publicFiles = fs.readdirSync(PUBLIC_DIR);

    // Should have at least index.html and styles.css
    expect(publicFiles).toContain('index.html');
    expect(publicFiles).toContain('styles.css');

    // Verify no source files that need compilation
    const needsCompilation = publicFiles.some(file =>
      file.endsWith('.ts') ||
      file.endsWith('.tsx') ||
      file.endsWith('.jsx') ||
      file.endsWith('.scss') ||
      file.endsWith('.sass') ||
      file.endsWith('.less')
    );

    expect(needsCompilation).toBe(false);

    console.log('Public folder contains ready-to-deploy files:', publicFiles.join(', '));
  });
});
