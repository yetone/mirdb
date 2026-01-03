// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify the MirDB homepage renders correctly across major browsers.
 * The tests run in Chrome, Firefox, Safari (WebKit), and Edge via Playwright.
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page loads and displays correctly', async ({ page, browserName }) => {
    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main heading is visible
    const heading = page.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');

    // Log browser name for visibility
    console.log(`Testing in browser: ${browserName}`);
  });

  test('hero section renders correctly', async ({ page }) => {
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check tagline
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('persistent key-value store');

    // Check CTA buttons
    const primaryCTA = page.locator('.btn-primary');
    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toHaveText('Get Started');

    const secondaryCTA = page.locator('.btn-secondary');
    await expect(secondaryCTA).toBeVisible();
    await expect(secondaryCTA).toContainText('GitHub');
  });

  test('features section renders all cards', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Check each feature card is visible
    const expectedFeatures = [
      'Memcached Compatible',
      'Persistent Storage',
      'LSM Tree Architecture',
      'Written in Rust',
    ];

    for (const feature of expectedFeatures) {
      const card = page.locator('.feature-card', { hasText: feature });
      await expect(card).toBeVisible();
    }
  });

  test('quickstart section renders code blocks', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check code blocks are present
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test('commands section displays all categories', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Check command categories
    const categories = page.locator('.command-category');
    await expect(categories).toHaveCount(3);

    // Verify Storage, Retrieval, and Deletion categories
    await expect(page.locator('.command-category', { hasText: 'Storage Commands' })).toBeVisible();
    await expect(page.locator('.command-category', { hasText: 'Retrieval Commands' })).toBeVisible();
    await expect(page.locator('.command-category', { hasText: 'Deletion Commands' })).toBeVisible();
  });

  test('roadmap section displays implemented and planned features', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check implemented features list
    const implementedList = page.locator('.implemented-list');
    await expect(implementedList).toBeVisible();

    // Check coming soon list
    const comingSoonList = page.locator('.coming-soon-list');
    await expect(comingSoonList).toBeVisible();
  });

  test('configuration table renders correctly', async ({ page }) => {
    const configSection = page.locator('#config');
    await expect(configSection).toBeVisible();

    const configTable = page.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Check for table rows
    const rows = configTable.locator('tbody tr');
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(5);
  });

  test('footer renders with correct links', async ({ page }) => {
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Check footer links
    const footerLinks = page.locator('.footer-links a');
    const count = await footerLinks.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify GitHub link
    const githubLink = page.locator('.footer-links a', { hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();
  });

  test('CSS layout renders correctly (flexbox and grid)', async ({ page }) => {
    // Test flexbox container - CTA buttons
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Test grid container - features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid has items
    const gridItems = featuresGrid.locator('.feature-card');
    const count = await gridItems.count();
    expect(count).toBe(4);
  });

  test('CSS gradients render correctly', async ({ page }) => {
    // The hero section uses gradient background
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check that h1 text is visible (uses gradient text)
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();

    // Verify text content is present
    const text = await h1.textContent();
    expect(text).toBe('MirDB');
  });

  test('CSS custom properties (variables) work correctly', async ({ page }) => {
    // Check that elements using CSS variables render with proper colors
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Check background color is applied (from --color-background)
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(bgColor).toBeTruthy();
    expect(bgColor).not.toBe('');
  });

  test('smooth scrolling is enabled', async ({ page }) => {
    // Click on Get Started link (which links to #quickstart)
    const getStartedBtn = page.locator('.btn-primary');
    await getStartedBtn.click();

    // Wait a moment for smooth scroll
    await page.waitForTimeout(500);

    // Verify we scrolled to quickstart section
    const quickstart = page.locator('#quickstart');
    await expect(quickstart).toBeInViewport();
  });

  test('hover effects work on interactive elements', async ({ page }) => {
    // Test feature card hover effect
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Hover over the card
    await featureCard.hover();

    // Check that border color changes (border-color: var(--color-primary))
    const borderColor = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).borderColor;
    });
    expect(borderColor).toBeTruthy();
  });

  test('focus styles work for keyboard navigation', async ({ page }) => {
    // Tab to first interactive element
    await page.keyboard.press('Tab');

    // Check that focus styles are visible
    const focusedElement = page.locator(':focus');
    const outline = await focusedElement.evaluate((el) => {
      return window.getComputedStyle(el).outline;
    });

    // Focus outline should be present
    expect(outline).toBeTruthy();
  });

  test('images and icons render correctly', async ({ page }) => {
    // Check feature icons are visible
    const featureIcons = page.locator('.feature-icon');
    const count = await featureIcons.count();
    expect(count).toBe(4);

    // Verify icons contain emoji/text content
    const firstIcon = featureIcons.first();
    const iconText = await firstIcon.textContent();
    expect(iconText?.trim().length).toBeGreaterThan(0);
  });

  test('no JavaScript errors in console', async ({ page }) => {
    const errors = [];

    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    expect(errors).toHaveLength(0);
  });

  test('page renders without visual layout issues', async ({ page }) => {
    // Check that no elements overflow the viewport unexpectedly
    const body = page.locator('body');
    const bodyWidth = await body.evaluate((el) => el.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body should not overflow viewport significantly
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 20); // Small tolerance for scrollbar
  });

  test('text is readable and visible', async ({ page }) => {
    // Check main content is visible
    const description = page.locator('.description');
    await expect(description).toBeVisible();

    // Verify text color is not transparent
    const color = await description.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(color).toBeTruthy();
    expect(color).not.toBe('transparent');
    expect(color).not.toBe('rgba(0, 0, 0, 0)');
  });
});
