// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 * NFR-4: Page must render correctly in latest versions of Chrome, Firefox, Safari, and Edge
 *
 * These tests verify that the MirDB landing page renders correctly and functions
 * properly across all major browsers (Chromium, Firefox, WebKit/Safari, Edge).
 */

test.describe('Cross-Browser Page Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Basic page rendering in each browser
  test('page loads successfully with correct title', async ({ page }) => {
    await expect(page).toHaveTitle(/MirDB/);
    // Verify the page body is visible
    const body = page.locator('body');
    await expect(body).toBeVisible();
  });

  // Test Case 2: Hero section renders correctly
  test('hero section renders correctly', async ({ page }) => {
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    // Check product name is displayed
    const productName = page.locator('.product-name');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Check tagline is displayed
    const tagline = page.locator('.tagline, .hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent');
    await expect(tagline).toContainText('Memcached');

    // Check CTA buttons are visible
    const primaryCTA = page.locator('.cta-primary, .btn-primary');
    await expect(primaryCTA).toBeVisible();

    const secondaryCTA = page.locator('.cta-secondary');
    await expect(secondaryCTA).toBeVisible();
  });

  // Test Case 3: Features section renders correctly
  test('features section renders correctly', async ({ page }) => {
    const featuresSection = page.locator('.features-section');
    await expect(featuresSection).toBeVisible();

    // Check feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify each feature card has content
    for (let i = 0; i < 3; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      await expect(card.locator('h3')).toBeVisible();
      await expect(card.locator('p')).toBeVisible();
    }
  });

  // Test Case 4: Commands section renders correctly
  test('commands section renders correctly', async ({ page }) => {
    const commandsSection = page.locator('.commands-section');
    await expect(commandsSection).toBeVisible();

    // Check command items
    const commandItems = page.locator('.command-item, .command-card');
    const count = await commandItems.count();
    expect(count).toBeGreaterThanOrEqual(5);

    // Verify commands are visible
    const commands = ['GET', 'SET', 'DELETE', 'ADD', 'REPLACE'];
    for (const cmd of commands) {
      const commandCode = page.locator(`.command-item code:has-text("${cmd}"), .command-card code:has-text("${cmd}")`);
      await expect(commandCode.first()).toBeVisible();
    }
  });

  // Test Case 5: Quick start section renders correctly
  test('quick start section renders correctly', async ({ page }) => {
    const quickStartSection = page.locator('.quickstart-section');
    await expect(quickStartSection).toBeVisible();

    // Check code block exists
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    // Check pre/code element exists
    const codeElement = codeBlock.locator('pre code');
    await expect(codeElement).toBeVisible();
  });

  // Test Case 6: Footer renders correctly
  test('footer renders correctly', async ({ page }) => {
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Check footer links
    const footerLinks = page.locator('.footer-links a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(2);

    // Check GitHub link exists (use first() to handle multiple github links)
    const githubLink = page.locator('.footer-links a[href*="github"]').first();
    await expect(githubLink).toBeVisible();
  });
});

test.describe('Cross-Browser CSS Layout Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case: CSS Grid layout in features section
  test('features grid displays correctly with CSS Grid', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed style
    const display = await featuresGrid.evaluate((el) =>
      window.getComputedStyle(el).display
    );
    expect(display).toBe('grid');

    // Verify cards are laid out properly (not stacked on desktop)
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    if (viewportWidth >= 900) {
      const boundingBox = await featuresGrid.boundingBox();
      expect(boundingBox).not.toBeNull();
      if (boundingBox) {
        // Grid should have reasonable width
        expect(boundingBox.width).toBeGreaterThan(600);
      }
    }
  });

  // Test Case: Flexbox layout in CTA buttons
  test('CTA buttons container uses flexbox correctly', async ({ page }) => {
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    const display = await ctaButtons.evaluate((el) =>
      window.getComputedStyle(el).display
    );
    expect(display).toBe('flex');

    const flexWrap = await ctaButtons.evaluate((el) =>
      window.getComputedStyle(el).flexWrap
    );
    expect(flexWrap).toBe('wrap');
  });

  // Test Case: Commands grid layout
  test('commands grid displays correctly', async ({ page }) => {
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    const display = await commandsGrid.evaluate((el) =>
      window.getComputedStyle(el).display
    );
    expect(display).toBe('grid');
  });

  // Test Case: Linear gradient backgrounds render
  test('hero section gradient background renders', async ({ page }) => {
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const backgroundImage = await heroSection.evaluate((el) =>
      window.getComputedStyle(el).backgroundImage
    );
    // Should contain linear-gradient
    expect(backgroundImage).toContain('linear-gradient');
  });

  // Test Case: Background clip for text gradient
  test('product name text gradient renders correctly', async ({ page }) => {
    const productName = page.locator('.product-name');
    await expect(productName).toBeVisible();

    // The text should be visible and have proper styling
    const color = await productName.evaluate((el) => {
      const style = window.getComputedStyle(el);
      // Background clip text might make color transparent
      return style.backgroundImage;
    });

    // Should have gradient background
    expect(color).toContain('linear-gradient');
  });

  // Test Case: Box-sizing is border-box
  test('elements use border-box box-sizing', async ({ page }) => {
    const container = page.locator('.container').first();
    await expect(container).toBeVisible();

    const boxSizing = await container.evaluate((el) =>
      window.getComputedStyle(el).boxSizing
    );
    expect(boxSizing).toBe('border-box');
  });
});

test.describe('Cross-Browser Interactive Elements', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case: Primary CTA button is clickable
  test('primary CTA button is clickable and navigates', async ({ page }) => {
    const primaryCTA = page.locator('.cta-primary, .btn-primary');
    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toBeEnabled();

    // Check the href attribute
    const href = await primaryCTA.getAttribute('href');
    expect(href).toBeTruthy();
  });

  // Test Case: GitHub button opens in new tab
  test('GitHub button has correct attributes for new tab', async ({ page }) => {
    const githubButton = page.locator('.hero-section a[href*="github"]');
    await expect(githubButton).toBeVisible();

    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubButton.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  // Test Case: Footer links are functional
  test('footer links are accessible and functional', async ({ page }) => {
    const footerLinks = page.locator('.footer-links a');
    const count = await footerLinks.count();

    for (let i = 0; i < count; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
    }
  });

  // Test Case: Hover effects work (transitions)
  test('CTA buttons have transition properties', async ({ page }) => {
    const primaryCTA = page.locator('.cta-primary, .btn-primary');
    await expect(primaryCTA).toBeVisible();

    const transition = await primaryCTA.evaluate((el) =>
      window.getComputedStyle(el).transition
    );
    // Should have some transition defined
    expect(transition).not.toBe('none');
    expect(transition.length).toBeGreaterThan(0);
  });
});

test.describe('Cross-Browser Typography', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case: System font stack is applied
  test('body uses system font stack', async ({ page }) => {
    const body = page.locator('body');
    const fontFamily = await body.evaluate((el) =>
      window.getComputedStyle(el).fontFamily
    );
    // Should include system fonts
    expect(fontFamily.toLowerCase()).toMatch(/(system-ui|-apple-system|segoe|roboto|sans-serif)/);
  });

  // Test Case: Headings are rendered correctly
  test('headings render with correct sizes', async ({ page }) => {
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();

    const fontSize = await h1.evaluate((el) =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    // H1 should be reasonably large (at least 24px)
    expect(fontSize).toBeGreaterThanOrEqual(24);
  });

  // Test Case: Code blocks use monospace font
  test('code blocks use monospace font', async ({ page }) => {
    const codeBlock = page.locator('.code-block code');
    await expect(codeBlock).toBeVisible();

    const fontFamily = await codeBlock.evaluate((el) =>
      window.getComputedStyle(el).fontFamily
    );
    // Should include monospace fonts
    expect(fontFamily.toLowerCase()).toMatch(/(monaco|menlo|mono|courier)/);
  });
});

test.describe('Cross-Browser Responsive Design', () => {
  // Test Case: Desktop viewport
  test('page renders correctly at desktop viewport (1280px)', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();
  });

  // Test Case: Tablet viewport
  test('page renders correctly at tablet viewport (768px)', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();
  });

  // Test Case: Mobile viewport
  test('page renders correctly at mobile viewport (375px)', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    // CTA buttons should be visible
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();
  });

  // Test Case: No horizontal scroll at mobile viewport
  test('no horizontal scroll at mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const clientWidth = await page.evaluate(() => document.body.clientWidth);

    // Body should not overflow horizontally
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // +1 for rounding
  });
});

test.describe('Cross-Browser Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case: Page has lang attribute
  test('HTML element has lang attribute', async ({ page }) => {
    const lang = await page.locator('html').getAttribute('lang');
    expect(lang).toBe('en');
  });

  // Test Case: Viewport meta tag is present
  test('viewport meta tag is present and correct', async ({ page }) => {
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toContain('width=device-width');
    expect(viewport).toContain('initial-scale=1');
  });

  // Test Case: External links have noopener
  test('external links have security attributes', async ({ page }) => {
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  // Test Case: Images have alt text (if any)
  test('images have alt attributes', async ({ page }) => {
    const images = page.locator('img');
    const count = await images.count();

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      // alt attribute should be present (can be empty for decorative images)
      expect(alt).not.toBeNull();
    }
  });
});
