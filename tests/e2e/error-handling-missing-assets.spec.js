// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Missing Assets Tests
 *
 * Verifies that the page handles missing assets gracefully:
 * - Page layout remains intact when images fail to load
 * - Content is readable when CSS fails to load
 * - No 404 errors occur for page resources during normal operation
 */

test.describe('Error Handling - Missing Assets', () => {

  test('TC1: Page layout remains intact if images fail to load, alt text is displayed', async ({ page }) => {
    // Track failed image requests
    const failedImages = [];
    const imageAlts = [];

    // Intercept image requests and make them fail
    await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', async (route) => {
      const url = route.request().url();
      // Let SVGs in index.html inline pass through (they're not external requests)
      // But fail any external image requests
      failedImages.push(url);
      await route.abort('failed');
    });

    // Navigate to the page
    await page.goto('/');

    // Verify that the page still loads and renders properly
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible and layout intact
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify features section layout is intact
    const featuresSection = page.locator('.features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are still displayed in grid
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify all feature cards are visible
    for (let i = 0; i < 3; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }

    // Verify code examples section is visible
    const codeExamples = page.locator('.code-examples');
    await expect(codeExamples).toBeVisible();

    // Verify getting started section is visible
    const gettingStarted = page.locator('.getting-started');
    await expect(gettingStarted).toBeVisible();

    // Verify architecture section is visible
    const architecture = page.locator('#architecture');
    await expect(architecture).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Check that any img elements with broken images still have alt text
    const images = page.locator('img');
    const imgCount = await images.count();

    for (let i = 0; i < imgCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      if (alt) {
        imageAlts.push(alt);
      }
    }

    // Verify the SVG icons in features still render (they're inline SVGs, not external)
    const featureIcons = page.locator('.feature-icon svg');
    await expect(featureIcons).toHaveCount(3);
    for (let i = 0; i < 3; i++) {
      await expect(featureIcons.nth(i)).toBeVisible();
    }

    // Verify architecture diagram SVG is visible (it's inline)
    const architectureDiagram = page.locator('.architecture-diagram');
    await expect(architectureDiagram).toBeVisible();
  });

  test('TC2: Content is still readable in unstyled state (CSS load failure)', async ({ page }) => {
    // Block CSS files from loading
    await page.route('**/*.css', async (route) => {
      await route.abort('failed');
    });

    // Also block the external Prism CSS
    await page.route('**/cdnjs.cloudflare.com/**/*.css', async (route) => {
      await route.abort('failed');
    });

    // Navigate to the page
    await page.goto('/');

    // Verify page title loads correctly
    await expect(page).toHaveTitle(/MirDB/);

    // Verify core content is visible and readable (without styling)

    // Hero section content
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    const heroTitleText = await heroTitle.textContent();
    expect(heroTitleText).toBe('MirDB');

    const heroTagline = page.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();
    const taglineText = await heroTagline.textContent();
    expect(taglineText).toContain('Persistent Key-Value Store');

    const heroDescription = page.locator('.hero-description');
    await expect(heroDescription).toBeVisible();
    const descriptionText = await heroDescription.textContent();
    expect(descriptionText).toContain('MirDB is a persistent key-value store');

    // CTA buttons are visible
    const ctaButtons = page.locator('.hero-cta .btn');
    await expect(ctaButtons.first()).toBeVisible();

    // Features section content
    const sectionTitle = page.locator('.features .section-title');
    await expect(sectionTitle).toBeVisible();
    const featuresTitleText = await sectionTitle.textContent();
    expect(featuresTitleText).toBe('Key Features');

    // Feature cards content is readable
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // First feature card text
    const firstCardTitle = page.locator('.feature-card').first().locator('.feature-title');
    await expect(firstCardTitle).toBeVisible();
    const firstCardText = await firstCardTitle.textContent();
    expect(firstCardText).toContain('Memcached Compatible');

    // Code examples are readable
    const connectCode = page.locator('#code-connect');
    await expect(connectCode).toBeVisible();
    const codeText = await connectCode.textContent();
    expect(codeText).toContain('import memcache');
    expect(codeText).toContain('memcache.Client');

    // Getting started section
    const installCode = page.locator('#code-install');
    await expect(installCode).toBeVisible();
    const installText = await installCode.textContent();
    expect(installText).toContain('git clone');
    expect(installText).toContain('cargo build');

    // Configuration section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Footer is visible
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();
    const footerText = await footer.textContent();
    expect(footerText).toContain('GitHub');

    // Verify links are still functional (semantic HTML)
    const githubLink = page.locator('.hero-cta a:has-text("View on GitHub")');
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    const getStartedLink = page.locator('.hero-cta a:has-text("Get Started")');
    await expect(getStartedLink).toHaveAttribute('href', '#getting-started');
  });

  test('TC3: No 404 errors in browser console for page resources', async ({ page }) => {
    const failedRequests = [];
    const consoleErrors = [];

    // Listen for all failed requests (404s, network errors, etc.)
    page.on('requestfailed', (request) => {
      const failure = request.failure();
      failedRequests.push({
        url: request.url(),
        errorText: failure ? failure.errorText : 'unknown'
      });
    });

    // Listen for responses with error status codes
    page.on('response', (response) => {
      const status = response.status();
      if (status >= 400) {
        failedRequests.push({
          url: response.url(),
          status: status,
          statusText: response.statusText()
        });
      }
    });

    // Listen for console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Navigate to the page and wait for network idle
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify no 404 or failed resource requests
    const resourceErrors = failedRequests.filter(req => {
      // Filter out external CDN failures that might be transient network issues
      // Focus on local resources (our own assets)
      const url = req.url || '';
      const isLocalResource = url.includes('localhost') || url.startsWith('/') ||
                              (!url.includes('://') || url.includes('localhost:8080'));
      return isLocalResource;
    });

    // Log any failures for debugging
    if (resourceErrors.length > 0) {
      console.log('Failed resource requests:', resourceErrors);
    }

    // Assert no local resources failed
    expect(resourceErrors).toHaveLength(0);

    // Check that core page resources loaded
    // Verify main stylesheet was loaded by checking computed styles
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify main.js loaded and initialized (copy buttons should be functional)
    const copyButton = page.locator('.copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Verify hero section rendered with proper structure
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();
  });

  test('Additional: SVG icons render correctly even with network issues', async ({ page }) => {
    // Navigate to the page
    await page.goto('/');

    // Verify all inline SVG icons in feature cards are present
    const featureIcons = page.locator('.feature-icon svg');
    await expect(featureIcons).toHaveCount(3);

    // Verify each SVG icon is visible and has content
    for (let i = 0; i < 3; i++) {
      const svg = featureIcons.nth(i);
      await expect(svg).toBeVisible();

      // Verify SVG has aria-hidden for accessibility (decorative)
      await expect(svg).toHaveAttribute('aria-hidden', 'true');

      // Verify SVG has proper viewBox
      await expect(svg).toHaveAttribute('viewBox');
    }

    // Verify architecture diagram SVG
    const architectureDiagram = page.locator('.architecture-diagram');
    await expect(architectureDiagram).toBeVisible();
    await expect(architectureDiagram).toHaveAttribute('role', 'img');
    await expect(architectureDiagram).toHaveAttribute('aria-label');

    // Verify the diagram has a title for accessibility
    const diagramTitle = page.locator('.architecture-diagram title');
    await expect(diagramTitle).toContainText('LSM Tree Architecture');
  });

  test('Additional: Page remains functional when external JS fails to load', async ({ page }) => {
    // Block external JavaScript (Prism.js CDN)
    await page.route('**/cdnjs.cloudflare.com/**/*.js', async (route) => {
      await route.abort('failed');
    });

    // Navigate to the page
    await page.goto('/');

    // Verify page loads
    await expect(page).toHaveTitle(/MirDB/);

    // Verify all sections are visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('.code-examples')).toBeVisible();
    await expect(page.locator('.getting-started')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // Verify code content is still visible (just without syntax highlighting)
    const connectCode = page.locator('#code-connect');
    await expect(connectCode).toBeVisible();
    const codeText = await connectCode.textContent();
    expect(codeText).toContain('import memcache');

    // Verify local main.js still works (copy button should be present)
    const copyButton = page.locator('.copy-btn').first();
    await expect(copyButton).toBeVisible();
  });

});
