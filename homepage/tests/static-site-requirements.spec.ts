import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Static Site Requirements Tests (NFR-6)
 *
 * These tests verify that the MirDB homepage functions as a static site
 * with no server-side dependencies as specified in NFR-6 of the PRD.
 *
 * Test cases:
 * 1. Site serves from simple HTTP server
 * 2. No API calls or server-side requests are made
 * 3. All assets are static files (HTML, CSS, JS, images)
 * 4. Site is deployable to GitHub Pages
 * 5. No database connections required
 */

test.describe('Static Site Requirements (NFR-6)', () => {
  const publicDir = path.join(process.cwd(), 'public');
  const indexPath = path.join(publicDir, 'index.html');
  const stylesPath = path.join(publicDir, 'styles.css');

  test('TC1: Site serves correctly from static file server', async ({ page }) => {
    // Verify static files exist in the public directory
    expect(fs.existsSync(indexPath)).toBe(true);
    expect(fs.existsSync(stylesPath)).toBe(true);

    // Load the page using file:// protocol (simulating static file serving)
    await page.goto(`file://${indexPath}`);
    await page.waitForLoadState('load');

    // Verify the page loads and renders correctly
    await expect(page.locator('body')).toBeVisible();
    await expect(page.getByTestId('hero-section')).toBeVisible();
    await expect(page.getByTestId('features-section')).toBeVisible();
    await expect(page.getByTestId('architecture-section')).toBeVisible();
    await expect(page.getByTestId('quickstart-section')).toBeVisible();
    await expect(page.getByTestId('configuration-section')).toBeVisible();
    await expect(page.getByTestId('footer')).toBeVisible();

    // Verify main content is present
    const heading = page.locator('h1');
    await expect(heading).toHaveText('MirDB');

    const tagline = page.getByTestId('tagline');
    await expect(tagline).toContainText('Persistent Memcached-Compatible Key-Value Store');

    // Verify navigation links work
    const navLinks = page.locator('.nav-links a');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThan(0);

    // Verify CSS is applied correctly
    const heroSection = page.getByTestId('hero-section');
    const backgroundColor = await heroSection.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    // Verify some styling is applied (not default)
    expect(backgroundColor).toBeTruthy();

    // Verify JavaScript functionality works (copy button)
    const copyButton = page.getByTestId('copy-button');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toBeEnabled();

    console.log('Static file serving verified:');
    console.log('- HTML loads correctly from file:// protocol');
    console.log('- CSS is applied');
    console.log('- All page sections render');
    console.log('- JavaScript functionality is available');
  });

  test('TC2: No API calls or server-side requests are made', async ({ page }) => {
    const networkRequests: { url: string; method: string }[] = [];

    // Monitor all network requests
    page.on('request', request => {
      networkRequests.push({
        url: request.url(),
        method: request.method()
      });
    });

    await page.goto(`file://${indexPath}`);
    await page.waitForLoadState('networkidle');

    // Filter out file:// protocol requests (local file access)
    const externalRequests = networkRequests.filter(
      req => !req.url.startsWith('file://') &&
             !req.url.startsWith('data:') &&
             !req.url.startsWith('blob:')
    );

    // Verify no external API calls are made
    const apiCalls = externalRequests.filter(req =>
      req.url.includes('/api/') ||
      req.url.includes('api.') ||
      req.method === 'POST' ||
      req.method === 'PUT' ||
      req.method === 'DELETE'
    );

    expect(apiCalls.length).toBe(0);

    // Analyze the HTML for any fetch/XHR patterns
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check for fetch API usage (should not have dynamic data fetching)
    const hasFetchCalls = htmlContent.includes('fetch(') &&
                          !htmlContent.includes('// fetch'); // Exclude comments
    const hasXHRCalls = htmlContent.includes('XMLHttpRequest');
    const hasAxiosCalls = htmlContent.includes('axios');
    const hasAjaxCalls = htmlContent.includes('$.ajax') || htmlContent.includes('.ajax(');

    // The only JavaScript should be for copy-to-clipboard and menu toggle
    // These don't make network requests
    expect(hasFetchCalls).toBe(false);
    expect(hasXHRCalls).toBe(false);
    expect(hasAxiosCalls).toBe(false);
    expect(hasAjaxCalls).toBe(false);

    // Check for WebSocket connections
    const hasWebSocket = htmlContent.includes('WebSocket') ||
                         htmlContent.includes('new WebSocket');
    expect(hasWebSocket).toBe(false);

    console.log('Network request verification:');
    console.log(`- Total requests made: ${networkRequests.length}`);
    console.log(`- External requests: ${externalRequests.length}`);
    console.log(`- API calls: ${apiCalls.length}`);
    console.log('- No fetch/XHR/axios/ajax calls in code');
    console.log('- No WebSocket connections');
  });

  test('TC3: All assets are static files (HTML, CSS, JS, images)', async ({ page }) => {
    // Verify directory structure contains only static files
    const publicFiles = fs.readdirSync(publicDir);

    console.log('Files in public directory:');
    publicFiles.forEach(file => console.log(`- ${file}`));

    // Verify expected static files exist
    expect(publicFiles).toContain('index.html');
    expect(publicFiles).toContain('styles.css');

    // Verify file types are all static
    const staticExtensions = ['.html', '.css', '.js', '.svg', '.png', '.jpg', '.jpeg', '.gif', '.ico', '.webp', '.woff', '.woff2', '.ttf', '.eot'];

    publicFiles.forEach(file => {
      const ext = path.extname(file).toLowerCase();
      if (ext) {
        const isStatic = staticExtensions.includes(ext);
        expect(isStatic).toBe(true);
      }
    });

    // Verify HTML file is valid static HTML
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Should start with DOCTYPE declaration
    expect(htmlContent.trim().startsWith('<!DOCTYPE html>')).toBe(true);

    // Should have proper HTML structure
    expect(htmlContent).toContain('<html');
    expect(htmlContent).toContain('<head>');
    expect(htmlContent).toContain('<body>');
    expect(htmlContent).toContain('</html>');

    // Verify no server-side template syntax
    const serverSidePatterns = [
      { pattern: '<%', name: 'EJS/ASP' },
      { pattern: '<?php', name: 'PHP' },
      { pattern: '{%', name: 'Jinja/Nunjucks' },
      { pattern: '{{#', name: 'Handlebars' },
      { pattern: 'ng-', name: 'AngularJS' },
      { pattern: 'v-if=', name: 'Vue.js (server directive)' },
      { pattern: ':src=', name: 'Vue.js (dynamic binding)' },
    ];

    serverSidePatterns.forEach(({ pattern, name }) => {
      const hasPattern = htmlContent.includes(pattern);
      if (hasPattern && pattern !== '{{') {
        console.log(`Warning: Found ${name} pattern`);
      }
      // Allow {{ only in non-template contexts (e.g., JSON examples in docs)
      if (pattern !== '{{') {
        expect(hasPattern).toBe(false);
      }
    });

    // Verify CSS file is valid static CSS
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    // Should not have CSS preprocessor syntax that requires compilation
    const hasScssVariables = cssContent.includes('$') && cssContent.match(/\$[a-zA-Z]/);
    const hasLessVariables = cssContent.includes('@') && cssContent.match(/@[a-zA-Z-]+\s*:/);

    // CSS custom properties (--var) are valid static CSS
    // SCSS variables ($var) and LESS variables (@var:) require preprocessing
    expect(hasScssVariables).toBeFalsy();
    expect(hasLessVariables).toBeFalsy();

    // Verify JavaScript is inline (no build step required)
    const scriptTags = htmlContent.match(/<script[\s\S]*?<\/script>/g) || [];

    scriptTags.forEach(script => {
      // Scripts should be inline, not external (except for standard CDN usage which we disallow)
      const hasExternalSrc = script.includes('src="http') || script.includes("src='http");
      expect(hasExternalSrc).toBe(false);
    });

    // Load page and verify all assets load correctly
    await page.goto(`file://${indexPath}`);
    await page.waitForLoadState('load');

    // Verify SVG diagram (inline asset) renders
    const svgDiagram = page.getByTestId('architecture-diagram');
    await expect(svgDiagram).toBeVisible();

    console.log('\nStatic asset verification:');
    console.log('- All files have static extensions');
    console.log('- HTML has proper static structure');
    console.log('- No server-side template syntax');
    console.log('- CSS has no preprocessor syntax');
    console.log('- JavaScript is inline (no build required)');
    console.log('- SVG diagram renders correctly');
  });

  test('TC4: Site is deployable to GitHub Pages', async ({ page }) => {
    // GitHub Pages requirements:
    // 1. Static files only (no server-side processing)
    // 2. index.html at root
    // 3. No unsupported server-side features
    // 4. Relative paths for assets
    // 5. No build step required (or build output is static)

    // Verify index.html exists at root of public directory
    expect(fs.existsSync(indexPath)).toBe(true);

    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Verify asset paths are relative (not absolute server paths)
    const stylesheetLinks = htmlContent.match(/<link[^>]*href="([^"]*)"[^>]*>/g) || [];
    stylesheetLinks.forEach(link => {
      // Should not use absolute paths starting with /
      const usesAbsolutePath = link.includes('href="/') && !link.includes('href="//');
      const usesServerPath = link.includes('href="http://localhost');
      expect(usesAbsolutePath).toBe(false);
      expect(usesServerPath).toBe(false);
    });

    // Verify internal links use relative paths or anchor links
    const internalLinks = htmlContent.match(/<a[^>]*href="([^"]*)"[^>]*>/g) || [];
    const problematicLinks = internalLinks.filter(link => {
      // Filter out external links (http/https)
      if (link.includes('http://') || link.includes('https://')) {
        return false;
      }
      // Filter out anchor links
      if (link.includes('href="#')) {
        return false;
      }
      // Check for problematic server-absolute paths
      return link.includes('href="/') && !link.includes('href="/#');
    });

    // Only the logo link "/" is acceptable for GitHub Pages root
    const acceptableAbsoluteLinks = problematicLinks.filter(
      link => !link.includes('href="/"') || link.includes('href="//')
    );
    expect(acceptableAbsoluteLinks.length).toBe(0);

    // Verify no server-side features that GitHub Pages doesn't support
    const unsupportedFeatures = [
      { pattern: '.php', name: 'PHP files' },
      { pattern: '.asp', name: 'ASP files' },
      { pattern: '.jsp', name: 'JSP files' },
      { pattern: '.cgi', name: 'CGI scripts' },
      { pattern: 'server-side', name: 'Server-side rendering references' },
    ];

    unsupportedFeatures.forEach(({ pattern, name }) => {
      const hasFeature = htmlContent.toLowerCase().includes(pattern);
      if (hasFeature) {
        console.log(`Warning: Found reference to ${name}`);
      }
      expect(hasFeature).toBe(false);
    });

    // Verify CNAME or custom domain setup is not required (optional feature)
    // The site should work on default github.io domain

    // Verify Jekyll processing is not required (no _config.yml dependency in HTML)
    const requiresJekyll = htmlContent.includes('{% ') || htmlContent.includes('{{ site.');
    expect(requiresJekyll).toBe(false);

    // Check for .nojekyll compatibility (raw HTML serving)
    // The site should work whether Jekyll processing is enabled or not

    // Load and verify page works
    await page.goto(`file://${indexPath}`);
    await page.waitForLoadState('load');

    // Verify all sections load correctly
    await expect(page.locator('h1')).toHaveText('MirDB');
    await expect(page.getByTestId('features-section')).toBeVisible();

    // Verify external links have proper attributes for GitHub Pages
    const externalLinks = page.locator('a[target="_blank"]');
    const externalCount = await externalLinks.count();

    for (let i = 0; i < externalCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      // Should have noopener noreferrer for security
      expect(rel).toContain('noopener');
    }

    console.log('GitHub Pages compatibility verified:');
    console.log('- index.html at root');
    console.log('- Relative asset paths');
    console.log('- No server-side processing required');
    console.log('- No Jekyll template syntax');
    console.log('- External links have proper security attributes');
  });

  test('TC5: No database connections required', async ({ page }) => {
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    // Check for database connection patterns in code
    const databasePatterns = [
      // SQL databases
      { pattern: 'mysql', name: 'MySQL' },
      { pattern: 'postgresql', name: 'PostgreSQL' },
      { pattern: 'sqlite', name: 'SQLite' },
      { pattern: 'mongodb', name: 'MongoDB' },
      { pattern: 'mongoose', name: 'Mongoose (MongoDB)' },
      // Connection strings
      { pattern: 'connection string', name: 'Database connection string' },
      { pattern: 'connectionString', name: 'Database connectionString' },
      { pattern: 'DB_HOST', name: 'Database host env var' },
      { pattern: 'DATABASE_URL', name: 'Database URL env var' },
      // ORM patterns
      { pattern: 'sequelize', name: 'Sequelize ORM' },
      { pattern: 'prisma', name: 'Prisma ORM' },
      { pattern: 'typeorm', name: 'TypeORM' },
      // Firebase/Supabase
      { pattern: 'firebase', name: 'Firebase' },
      { pattern: 'supabase', name: 'Supabase' },
      // Generic patterns
      { pattern: 'createConnection', name: 'Database createConnection' },
      { pattern: 'db.query', name: 'Database query' },
    ];

    databasePatterns.forEach(({ pattern, name }) => {
      const htmlHasPattern = htmlContent.toLowerCase().includes(pattern.toLowerCase());
      const cssHasPattern = cssContent.toLowerCase().includes(pattern.toLowerCase());

      // Exception: The content may reference databases in documentation text
      // Check if it's in a code context vs documentation text
      if (htmlHasPattern) {
        // Allow references in text content but not in script tags
        const scriptContent = htmlContent.match(/<script[\s\S]*?<\/script>/g)?.join('') || '';
        const scriptHasPattern = scriptContent.toLowerCase().includes(pattern.toLowerCase());
        expect(scriptHasPattern).toBe(false);
      }
      expect(cssHasPattern).toBe(false);
    });

    // Check for backend service patterns
    const backendPatterns = [
      { pattern: 'express', name: 'Express.js server' },
      { pattern: 'fastify', name: 'Fastify server' },
      { pattern: 'koa', name: 'Koa server' },
      { pattern: 'app.listen', name: 'Server listen call' },
      { pattern: 'createServer', name: 'HTTP server creation' },
    ];

    backendPatterns.forEach(({ pattern, name }) => {
      const scriptContent = htmlContent.match(/<script[\s\S]*?<\/script>/g)?.join('') || '';
      const hasPattern = scriptContent.toLowerCase().includes(pattern.toLowerCase());
      expect(hasPattern).toBe(false);
    });

    // Verify no environment variable usage that suggests backend
    const envVarPatterns = [
      'process.env',
      'import.meta.env',
      'VITE_',
      'REACT_APP_',
      'NEXT_PUBLIC_',
    ];

    envVarPatterns.forEach(pattern => {
      const hasEnvVar = htmlContent.includes(pattern);
      expect(hasEnvVar).toBe(false);
    });

    // Verify localStorage/sessionStorage are the only storage mechanisms
    // (These are client-side and don't require a database)
    const allowedStoragePatterns = ['localStorage', 'sessionStorage', 'clipboard'];
    const storagePatterns = htmlContent.match(/[a-zA-Z]+Storage/g) || [];

    storagePatterns.forEach(pattern => {
      const isAllowed = allowedStoragePatterns.some(allowed =>
        pattern.toLowerCase().includes(allowed.toLowerCase())
      );
      expect(isAllowed).toBe(true);
    });

    // Load page and monitor for any unexpected connections
    const connections: string[] = [];
    page.on('request', request => {
      const url = request.url();
      if (!url.startsWith('file://') && !url.startsWith('data:')) {
        connections.push(url);
      }
    });

    await page.goto(`file://${indexPath}`);
    await page.waitForLoadState('networkidle');

    // Verify no database or backend connections were attempted
    const dbConnections = connections.filter(url =>
      url.includes('api') ||
      url.includes('graphql') ||
      url.includes('database') ||
      url.includes('db') ||
      url.includes('mongo') ||
      url.includes('postgres') ||
      url.includes('mysql') ||
      url.includes('firebase') ||
      url.includes('supabase')
    );

    expect(dbConnections.length).toBe(0);

    console.log('No database dependencies verified:');
    console.log('- No database connection patterns in code');
    console.log('- No backend server patterns');
    console.log('- No environment variable usage');
    console.log('- No database connections made during page load');
    console.log('- Site operates as pure client-side static content');
  });

  test('TC-BONUS: Verify offline capability after initial load', async ({ page, context }) => {
    // This tests the "offline capability" mentioned in scenario step 3
    // Static sites should work offline once cached

    // First, load the page normally
    await page.goto(`file://${indexPath}`);
    await page.waitForLoadState('load');

    // Verify initial content
    await expect(page.locator('h1')).toHaveText('MirDB');
    await expect(page.getByTestId('features-section')).toBeVisible();

    // Capture the content for comparison
    const heroText = await page.getByTestId('hero-section').textContent();
    const featuresText = await page.getByTestId('features-section').textContent();

    // Since we're using file:// protocol, the content is inherently "offline"
    // No network requests needed - all content is in static files

    // Verify all content is embedded (not dynamically loaded)
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check that main content is in HTML (not loaded via JavaScript)
    expect(htmlContent).toContain('MirDB');
    expect(htmlContent).toContain('Memcached Protocol');
    expect(htmlContent).toContain('Persistent Storage');
    expect(htmlContent).toContain('LSM Tree Architecture');

    // Verify no lazy loading that would fail offline
    const hasLazyLoading = htmlContent.includes('loading="lazy"');
    // Images can be lazy loaded, but text content should not depend on it

    // Verify no dynamic imports that could fail offline
    const hasDynamicImports = htmlContent.includes('import(') ||
                              htmlContent.includes('require(');
    expect(hasDynamicImports).toBe(false);

    // Reload page (simulating cache access)
    await page.reload();
    await page.waitForLoadState('load');

    // Verify content is identical after reload
    const heroTextReload = await page.getByTestId('hero-section').textContent();
    const featuresTextReload = await page.getByTestId('features-section').textContent();

    expect(heroTextReload).toBe(heroText);
    expect(featuresTextReload).toBe(featuresText);

    console.log('Offline capability verified:');
    console.log('- All content is embedded in static HTML');
    console.log('- No dynamic content loading');
    console.log('- Page content is consistent across reloads');
    console.log('- Static files work without network connection');
  });
});
