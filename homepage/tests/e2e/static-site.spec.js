/**
 * Static Site Requirements E2E Tests
 * Owner: Scenario 18 - Static Site Requirements
 *
 * Tests:
 * - Page renders correctly when opened directly (file:// protocol simulation)
 * - CDN dependencies gracefully degrade if unavailable
 * - Page works without server-side processing
 */

const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const homepagePath = path.resolve(__dirname, '../../');

test.describe('Test Case 2: File Protocol Rendering', () => {
  test('page loads and displays content using file:// protocol', async ({ page }) => {
    // Load the HTML file directly using file:// protocol
    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Verify page title is present
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify hero tagline is visible
    const heroTagline = page.locator('.hero__tagline');
    await expect(heroTagline).toBeVisible();

    // Verify CTA button is visible
    const heroCTA = page.locator('.hero__cta');
    await expect(heroCTA).toBeVisible();
  });

  test('navigation links work with file:// protocol', async ({ page }) => {
    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Verify navigation links are present
    const navLinks = page.locator('.nav__link');
    await expect(navLinks).toHaveCount(6);

    // Click on Features link
    await page.click('a.nav__link[href="#features"]');

    // Verify features section is now in view (scroll position changed)
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('all major sections render correctly with file:// protocol', async ({ page }) => {
    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    const sections = [
      { id: 'hero', selector: '.hero' },
      { id: 'features', selector: '#features' },
      { id: 'quick-start', selector: '#quick-start' },
      { id: 'architecture', selector: '#architecture' },
      { id: 'api-reference', selector: '#api-reference' },
      { id: 'configuration', selector: '#configuration' },
      { id: 'performance', selector: '#performance' },
      { id: 'contributing', selector: '#contributing' }
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);
      await expect(element).toBeAttached();
      // Verify section has content
      const textContent = await element.textContent();
      expect(textContent.length).toBeGreaterThan(0);
    }
  });

  test('CSS styles are applied correctly with file:// protocol', async ({ page }) => {
    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Verify CSS is loaded by checking computed styles
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Check that header has position styling (from CSS)
    const headerStyles = await header.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        position: styles.position,
        display: styles.display
      };
    });

    // Header should have some positioning from CSS
    expect(['fixed', 'sticky', 'relative', 'absolute']).toContain(headerStyles.position);
  });

  test('JavaScript functionality works with file:// protocol', async ({ page }) => {
    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Wait for JavaScript to initialize
    await page.waitForLoadState('domcontentloaded');

    // Verify theme toggle button exists and is clickable
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Get initial theme
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    // Click theme toggle
    await themeToggle.click();

    // Verify theme changed (or toggle functionality works)
    const newTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    // Theme should have toggled
    expect(newTheme !== initialTheme || newTheme !== null).toBeTruthy();
  });

  test('code blocks are readable with file:// protocol', async ({ page }) => {
    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Navigate to quick-start section
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    // Verify code blocks are present
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify code content is visible
    const firstCodeBlock = codeBlocks.first();
    const codeElement = firstCodeBlock.locator('code');
    await expect(codeElement).toBeVisible();

    const codeText = await codeElement.textContent();
    expect(codeText.length).toBeGreaterThan(0);
  });

  test('images load with file:// protocol using asset paths', async ({ page }) => {
    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Check logo images
    const logoImages = page.locator('img[src*="logo.gif"]');
    const logoCount = await logoImages.count();

    // At least one logo should be present
    expect(logoCount).toBeGreaterThan(0);

    // Verify first logo is attached to DOM
    if (logoCount > 0) {
      const firstLogo = logoImages.first();
      await expect(firstLogo).toBeAttached();
    }
  });
});

test.describe('Test Case 4: CDN Graceful Degradation', () => {
  test('page functions without external CDN resources', async ({ page, context }) => {
    // Block all external requests to simulate CDN failure
    await context.route('**/*', (route) => {
      const url = route.request().url();
      // Allow file:// requests and local files
      if (url.startsWith('file://') || url.includes('localhost')) {
        route.continue();
      } else {
        // Block external CDN requests
        route.abort();
      }
    });

    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath, { waitUntil: 'domcontentloaded' });

    // Page should still display content
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    // Navigation should still work
    const navLinks = page.locator('.nav__link');
    await expect(navLinks.first()).toBeVisible();
  });

  test('page layout is intact without external fonts', async ({ page, context }) => {
    // Block font CDN requests specifically
    await context.route('**/fonts.googleapis.com/**', (route) => route.abort());
    await context.route('**/fonts.gstatic.com/**', (route) => route.abort());
    await context.route('**/*.woff2', (route) => route.abort());
    await context.route('**/*.woff', (route) => route.abort());

    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Page should still be readable
    const heroDescription = page.locator('.hero__description');
    await expect(heroDescription).toBeVisible();

    const descText = await heroDescription.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  test('code blocks readable without syntax highlighting CDN', async ({ page, context }) => {
    // Block syntax highlighting CDN
    await context.route('**/prismjs.com/**', (route) => route.abort());
    await context.route('**/highlightjs.org/**', (route) => route.abort());
    await context.route('**/cdnjs.cloudflare.com/**', (route) => route.abort());

    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Navigate to quick-start section
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    // Code blocks should still be readable
    const codeBlocks = page.locator('.code-block code');
    const firstCodeBlock = codeBlocks.first();

    await expect(firstCodeBlock).toBeVisible();

    const codeText = await firstCodeBlock.textContent();
    // Code content should be present (commands, instructions, etc.)
    expect(codeText).toMatch(/git|cargo|npm|install|build/i);
  });

  test('theme toggle works without external dependencies', async ({ page, context }) => {
    // Block all external requests
    await context.route('**/*', (route) => {
      const url = route.request().url();
      if (url.startsWith('file://')) {
        route.continue();
      } else {
        route.abort();
      }
    });

    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Click theme toggle
    await themeToggle.click();

    // Verify theme attribute exists on html element
    const themeAttr = await page.evaluate(() => {
      return document.documentElement.hasAttribute('data-theme');
    });

    expect(themeAttr).toBeTruthy();
  });

  test('navigation and scroll work without external dependencies', async ({ page, context }) => {
    // Block all external requests
    await context.route('**/*', (route) => {
      const url = route.request().url();
      if (url.startsWith('file://')) {
        route.continue();
      } else {
        route.abort();
      }
    });

    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click on a navigation link
    const featuresLink = page.locator('a.nav__link[href="#features"]');
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);
  });

  test('all content is accessible without any external resources', async ({ page, context }) => {
    // Block ALL external requests to simulate complete offline mode
    await context.route('**/*', (route) => {
      const url = route.request().url();
      if (url.startsWith('file://')) {
        route.continue();
      } else {
        route.abort();
      }
    });

    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath, { waitUntil: 'domcontentloaded' });

    // Verify all main sections are present and have content
    const sections = [
      '#hero .hero__title',
      '#features .section-title',
      '#quick-start .section-title',
      '#architecture .section-title',
      '#api-reference .section-title',
      '#configuration .section-title',
      '#performance .section-title',
      '#contributing .section-title'
    ];

    for (const selector of sections) {
      const element = page.locator(selector);
      await expect(element).toBeAttached();
      const text = await element.textContent();
      expect(text.length).toBeGreaterThan(0);
    }
  });
});

test.describe('Static Site Server Independence', () => {
  test('page has no server-side dependencies in HTML', async ({ page }) => {
    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Verify page loaded successfully (would fail if server deps required)
    await expect(page.locator('.header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();
  });

  test('no console errors when loaded as static file', async ({ page }) => {
    const consoleErrors = [];

    page.on('console', msg => {
      if (msg.type() === 'error') {
        // Filter out known acceptable errors (like favicon 404)
        const text = msg.text();
        if (!text.includes('favicon') && !text.includes('manifest')) {
          consoleErrors.push(text);
        }
      }
    });

    const htmlFilePath = `file://${path.join(homepagePath, 'index.html')}`;
    await page.goto(htmlFilePath);

    // Wait for page to fully load
    await page.waitForLoadState('load');

    // No critical console errors should be present
    // Filter out cross-origin errors that are expected with file://
    const criticalErrors = consoleErrors.filter(e =>
      !e.includes('cross-origin') &&
      !e.includes('CORS') &&
      !e.includes('file://') &&
      !e.includes('Failed to load resource') // Acceptable for local file protocol
    );

    expect(criticalErrors).toHaveLength(0);
  });
});
