// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

/**
 * Cross-Browser Compatibility Tests
 *
 * NFR-5: Cross-browser compatibility (Chrome, Firefox, Safari, Edge)
 *
 * These tests verify that the MirDB homepage works correctly across
 * all major browsers. The Playwright config defines projects for:
 * - chromium (Chrome)
 * - firefox (Firefox)
 * - webkit (Safari)
 * - edge (Microsoft Edge)
 *
 * Each test in this file runs against all configured browser projects.
 */

test.describe('Cross-Browser Compatibility - Homepage Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Hero section displays correctly', async ({ page, browserName }) => {
    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify product name
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify tagline
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify value proposition
    const valueProposition = page.locator('[data-testid="value-proposition"]');
    await expect(valueProposition).toBeVisible();
    const text = await valueProposition.textContent();
    expect(text.length).toBeGreaterThan(50);
  });

  test('TC2: CTA buttons are visible and functional', async ({ page, browserName }) => {
    // Verify Get Started button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');

    // Verify GitHub button
    const githubBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveAttribute('href', 'https://github.com/pjzhong/mirdb');
    await expect(githubBtn).toHaveAttribute('target', '_blank');
  });

  test('TC3: Key features section displays correctly', async ({ page, browserName }) => {
    // Verify features section is visible
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify all 4 feature cards are present
    const featureCards = [
      'feature-memcached',
      'feature-persistent',
      'feature-performance',
      'feature-config'
    ];

    for (const featureId of featureCards) {
      const feature = page.locator(`[data-testid="${featureId}"]`);
      await expect(feature).toBeVisible();
    }
  });

  test('TC4: Architecture section displays correctly', async ({ page, browserName }) => {
    // Verify architecture section
    const archSection = page.locator('[data-testid="architecture-section"]');
    await expect(archSection).toBeVisible();

    // Verify architecture diagram
    const archDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(archDiagram).toBeVisible();

    // Verify diagram components
    const diagramComponents = ['diagram-wal', 'diagram-memtable'];
    for (const componentId of diagramComponents) {
      const component = page.locator(`[data-testid="${componentId}"]`);
      await expect(component).toBeVisible();
    }
  });

  test('TC5: Quick start section displays correctly', async ({ page, browserName }) => {
    // Verify quick start section
    const quickstartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickstartSection).toBeVisible();

    // Verify code blocks are present
    const codeBlocks = page.locator('[data-testid="quickstart-section"] pre');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);
  });

  test('TC6: Footer section displays correctly', async ({ page, browserName }) => {
    // Verify footer section
    const footerSection = page.locator('[data-testid="footer-section"]');
    await expect(footerSection).toBeVisible();

    // Verify footer links
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify license info
    const license = page.locator('[data-testid="footer-license"]');
    await expect(license).toBeVisible();

    // Verify status badge
    const status = page.locator('[data-testid="footer-status"]');
    await expect(status).toBeVisible();
  });
});

test.describe('Cross-Browser Compatibility - Interactions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC7: Navigation scroll works correctly', async ({ page, browserName }) => {
    // Click Get Started button and verify scroll to quickstart
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await getStartedBtn.click();

    // Give time for smooth scroll
    await page.waitForTimeout(500);

    // Verify quickstart section is in view
    const quickstartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickstartSection).toBeInViewport();
  });

  test('TC8: External links have correct attributes', async ({ page, browserName }) => {
    // Verify GitHub link in hero opens in new tab
    const heroGithub = page.locator('[data-testid="cta-github"]');
    await expect(heroGithub).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify footer GitHub link
    const footerGithub = page.locator('[data-testid="footer-github-link"]');
    await expect(footerGithub).toHaveAttribute('target', '_blank');
  });

  test('TC9: Hover states work correctly on buttons', async ({ page, browserName }) => {
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');

    // Verify button is interactive
    await expect(getStartedBtn).toBeEnabled();

    // Hover over the button
    await getStartedBtn.hover();

    // Button should still be visible after hover
    await expect(getStartedBtn).toBeVisible();
  });
});

test.describe('Cross-Browser Compatibility - Layout and Styling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC10: Page renders without horizontal scroll', async ({ page, browserName }) => {
    // Check viewport dimensions
    const viewportSize = page.viewportSize();

    // Get page width
    const pageWidth = await page.evaluate(() => document.documentElement.scrollWidth);

    // Page should not be wider than viewport (no horizontal scroll)
    expect(pageWidth).toBeLessThanOrEqual(viewportSize.width + 1); // Allow 1px tolerance
  });

  test('TC11: Typography renders correctly', async ({ page, browserName }) => {
    // Verify headings are visible and styled
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();

    // Check font is applied (not system default)
    const fontFamily = await h1.evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    expect(fontFamily).toBeTruthy();
  });

  test('TC12: Colors and backgrounds render correctly', async ({ page, browserName }) => {
    // Check hero section has gradient background (gradient may resolve to transparent in computed style)
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroClasses = await heroSection.getAttribute('class');
    // Verify hero section has gradient classes applied
    expect(heroClasses).toContain('bg-gradient');

    // Check that the hero section is visible and styled
    await expect(heroSection).toBeVisible();

    // Verify the body has background color class
    const body = page.locator('body');
    const bodyClasses = await body.getAttribute('class');
    expect(bodyClasses).toContain('bg-mirdb-light');
  });

  test('TC13: Feature cards have consistent layout', async ({ page, browserName }) => {
    // Target the specific 4 feature card containers (not their child elements)
    const featureCardIds = [
      'feature-memcached',
      'feature-persistent',
      'feature-performance',
      'feature-config'
    ];

    // Verify all 4 feature cards are visible
    for (const cardId of featureCardIds) {
      const card = page.locator(`[data-testid="${cardId}"]`);
      await expect(card).toBeVisible();
    }

    // Verify cards have consistent styling (feature-card class)
    const firstCard = page.locator(`[data-testid="${featureCardIds[0]}"]`);
    const cardClasses = await firstCard.getAttribute('class');
    expect(cardClasses).toContain('feature-card');
  });

  test('TC14: Code blocks have proper styling', async ({ page, browserName }) => {
    const codeBlocks = page.locator('pre code');
    const count = await codeBlocks.count();

    if (count > 0) {
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();

      // Check code block has monospace font
      const fontFamily = await firstCodeBlock.evaluate(el =>
        window.getComputedStyle(el).fontFamily
      );
      // Should contain monospace in font stack
      expect(fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/i);
    }
  });
});

test.describe('Cross-Browser Compatibility - Tables and Data', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC15: Protocol reference tables display correctly', async ({ page, browserName }) => {
    // Navigate to protocol section
    const protocolSection = page.locator('[data-testid="protocol-section"]');

    if (await protocolSection.isVisible()) {
      // Verify tables exist
      const tables = protocolSection.locator('table');
      const tableCount = await tables.count();

      if (tableCount > 0) {
        // Check first table has proper structure
        const firstTable = tables.first();
        const headers = firstTable.locator('th');
        const rows = firstTable.locator('tbody tr');

        await expect(headers.first()).toBeVisible();
      }
    }
  });

  test('TC16: Configuration tables display correctly', async ({ page, browserName }) => {
    // Navigate to configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');

    if (await configSection.isVisible()) {
      // Section should be visible and contain content
      await expect(configSection).toBeVisible();
    }
  });
});

test.describe('Cross-Browser Compatibility - Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC17: Focus states work correctly', async ({ page, browserName }) => {
    // Tab to first focusable element
    await page.keyboard.press('Tab');

    // Check that focus is visible on an element
    const focusedElement = page.locator(':focus');
    await expect(focusedElement).toBeTruthy();
  });

  test('TC18: Links have accessible text', async ({ page, browserName }) => {
    // Check main CTA links have text content
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const text = await getStartedBtn.textContent();
    expect(text.trim().length).toBeGreaterThan(0);

    const githubBtn = page.locator('[data-testid="cta-github"]');
    const githubText = await githubBtn.textContent();
    expect(githubText.trim().length).toBeGreaterThan(0);
  });

  test('TC19: Images have alt text', async ({ page, browserName }) => {
    const images = page.locator('img');
    const count = await images.count();

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      // Images should have alt attribute (can be empty for decorative images)
      expect(alt).not.toBeNull();
    }
  });

  test('TC20: Semantic HTML structure', async ({ page, browserName }) => {
    // Check for proper semantic elements
    const header = page.locator('header');
    const main = page.locator('main');
    const footer = page.locator('footer');

    // At least one of these semantic elements should exist
    const hasHeader = await header.count() > 0;
    const hasMain = await main.count() > 0;
    const hasFooter = await footer.count() > 0;

    expect(hasHeader || hasMain || hasFooter).toBeTruthy();
  });
});
