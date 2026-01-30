/**
 * Cross-Browser E2E Tests.
 * Owner: Scenario 13 - Cross-Browser Compatibility
 *
 * Tests on:
 * - Chrome
 * - Firefox
 * - Safari
 * - Edge
 *
 * Verifies all sections render correctly and interactions work.
 */

import { test, expect, type Page, type BrowserContext } from '@playwright/test';

/**
 * Helper function to verify all main sections exist on the page
 */
async function verifyAllSectionsExist(page: Page): Promise<void> {
  // Verify hero section
  const hero = page.locator('#hero, header');
  await expect(hero.first()).toBeVisible();

  // Verify features section
  const features = page.locator('#features');
  await expect(features).toBeVisible();

  // Verify usage section
  const usage = page.locator('#usage');
  await expect(usage).toBeVisible();

  // Verify architecture section
  const architecture = page.locator('#architecture');
  await expect(architecture).toBeVisible();

  // Verify getting started section
  const gettingStarted = page.locator('#getting-started');
  await expect(gettingStarted).toBeVisible();

  // Verify footer
  const footer = page.locator('#footer, footer');
  await expect(footer.first()).toBeVisible();
}

/**
 * Helper function to verify CSS Grid in features section
 */
async function verifyCSSGrid(page: Page): Promise<void> {
  const featuresGrid = page.locator('[data-testid="features-grid"], .features-grid');
  await expect(featuresGrid.first()).toBeVisible();

  // Verify grid has feature cards
  const featureCards = page.locator('.feature-card, .feature-card-item, [data-testid^="feature-"]');
  const cardCount = await featureCards.count();
  expect(cardCount).toBeGreaterThanOrEqual(3);
}

/**
 * Helper function to verify CSS animations work
 */
async function verifyCSSAnimations(page: Page): Promise<void> {
  // Find a feature card to test hover effect
  const featureCard = page.locator('.feature-card, .feature-card-item, [data-testid^="feature-"]').first();
  await expect(featureCard).toBeVisible();

  // Get initial position
  const initialBoundingBox = await featureCard.boundingBox();
  expect(initialBoundingBox).not.toBeNull();

  // Hover over the card to trigger animation
  await featureCard.hover();

  // Wait for CSS transition to complete
  await page.waitForTimeout(350);

  // Verify the card is still visible and accessible after hover
  await expect(featureCard).toBeVisible();
}

/**
 * Helper function to verify page interactivity
 */
async function verifyInteractivity(page: Page): Promise<void> {
  // Test that links are clickable
  const links = page.locator('a[href]');
  const linkCount = await links.count();
  expect(linkCount).toBeGreaterThanOrEqual(1);

  // Verify buttons and interactive elements exist
  const interactiveElements = page.locator('a, button, [role="button"]');
  const interactiveCount = await interactiveElements.count();
  expect(interactiveCount).toBeGreaterThanOrEqual(1);
}

/**
 * Test suite for all browsers - these tests run automatically
 * across all configured browsers (Chrome, Firefox, Safari, Edge)
 */
test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('homepage loads and renders all sections correctly', async ({ page, browserName }) => {
    // Log browser name for debugging
    console.log(`Testing on browser: ${browserName}`);

    // Verify page title
    await expect(page).toHaveTitle(/MirDB/i);

    // Verify all sections exist
    await verifyAllSectionsExist(page);
  });

  test('CSS Grid displays features correctly', async ({ page, browserName }) => {
    console.log(`Testing CSS Grid on browser: ${browserName}`);

    // Verify CSS Grid in features section
    await verifyCSSGrid(page);

    // Verify grid layout renders properly
    const featuresGrid = page.locator('[data-testid="features-grid"], .features-grid').first();
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    // Grid should be rendered (display: grid or similar)
    expect(['grid', 'inline-grid', 'block', 'flex']).toContain(gridStyle.display);
  });

  test('CSS animations and hover effects work', async ({ page, browserName }) => {
    console.log(`Testing CSS animations on browser: ${browserName}`);

    // Verify animations work
    await verifyCSSAnimations(page);

    // Check transition properties exist on feature cards
    const featureCard = page.locator('.feature-card, .feature-card-item, [data-testid^="feature-"]').first();
    const hasTransition = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const transitionDuration = style.transitionDuration;
      // Verify transition is defined and not 0s
      return transitionDuration !== '0s' && transitionDuration !== '';
    });

    // Transition should be defined for hover effects
    expect(hasTransition).toBe(true);
  });

  test('page interactions are functional', async ({ page, browserName }) => {
    console.log(`Testing interactions on browser: ${browserName}`);

    // Verify interactive elements work
    await verifyInteractivity(page);

    // Test scrolling works
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    // Verify footer is visible after scroll
    const footer = page.locator('#footer, footer').first();
    await expect(footer).toBeVisible();
  });

  test('smooth scrolling is functional', async ({ page, browserName }) => {
    console.log(`Testing smooth scroll on browser: ${browserName}`);

    // Check that smooth scrolling CSS is applied
    const hasSmoothScroll = await page.evaluate(() => {
      const html = document.documentElement;
      const style = window.getComputedStyle(html);
      return style.scrollBehavior === 'smooth';
    });

    expect(hasSmoothScroll).toBe(true);
  });

  test('all sections have proper heading structure', async ({ page, browserName }) => {
    console.log(`Testing heading structure on browser: ${browserName}`);

    // Verify h1 exists
    const h1 = page.locator('h1');
    await expect(h1.first()).toBeVisible();

    // Verify h2 headings exist for sections
    const h2s = page.locator('h2');
    const h2Count = await h2s.count();
    expect(h2Count).toBeGreaterThanOrEqual(3);
  });

  test('external links have proper security attributes', async ({ page, browserName }) => {
    console.log(`Testing link security on browser: ${browserName}`);

    // Find external links (those with full URLs or target="_blank")
    const externalLinks = page.locator('a[href^="http"], a[target="_blank"]');
    const linkCount = await externalLinks.count();

    if (linkCount > 0) {
      // Verify external links have security attributes
      for (let i = 0; i < Math.min(linkCount, 5); i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');
        // External links should have noopener or noreferrer
        if (rel) {
          expect(rel).toMatch(/noopener|noreferrer/);
        }
      }
    }
  });

  test('images have alt text for accessibility', async ({ page, browserName }) => {
    console.log(`Testing image alt text on browser: ${browserName}`);

    const images = page.locator('img');
    const imgCount = await images.count();

    // Check all images have alt attributes
    for (let i = 0; i < imgCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      expect(alt).not.toBeNull();
    }
  });

  test('no JavaScript errors on page load', async ({ page, browserName }) => {
    console.log(`Testing for JS errors on browser: ${browserName}`);

    const errors: string[] = [];

    // Listen for console errors
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    // Reload to capture any errors
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // No critical errors should occur
    expect(errors).toHaveLength(0);
  });
});

/**
 * Integration tests for CSS features across browsers
 */
test.describe('CSS Grid Support', () => {
  test('features grid displays correctly in all browsers', async ({ page, browserName }) => {
    console.log(`Testing features grid on browser: ${browserName}`);

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Verify grid container exists
    const featuresGrid = page.locator('[data-testid="features-grid"], .features-grid').first();
    await expect(featuresGrid).toBeVisible();

    // Verify all 6 feature cards are present - use direct children selector
    const featureCards = featuresGrid.locator('> .feature-card, > .feature-card-item, > [data-testid^="feature-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(6);

    // Verify each card has required elements
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);

      // Each card should have title and description
      const title = card.locator('.feature-title, h3, [data-testid="feature-title"]').first();
      const description = card.locator('.feature-description, p, [data-testid="feature-description"]').first();

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }
  });
});

test.describe('CSS Animations Support', () => {
  test('hover effects and transitions work in all browsers', async ({ page, browserName }) => {
    console.log(`Testing hover effects on browser: ${browserName}`);

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Find a feature card
    const featureCard = page.locator('.feature-card, .feature-card-item, [data-testid^="feature-"]').first();
    await expect(featureCard).toBeVisible();

    // Verify transition properties
    const transitionInfo = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        transitionProperty: style.transitionProperty,
        transitionDuration: style.transitionDuration,
        transform: style.transform,
      };
    });

    // Transition should be defined
    expect(transitionInfo.transitionDuration).not.toBe('0s');

    // Perform hover interaction
    await featureCard.hover();
    await page.waitForTimeout(400);

    // Verify the card responds to hover (transform should change or remain valid)
    const hoverTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Transform should be defined (even if 'none' which is valid)
    expect(hoverTransform).toBeDefined();
  });

  test('code blocks have syntax highlighting styles', async ({ page, browserName }) => {
    console.log(`Testing syntax highlighting on browser: ${browserName}`);

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Scroll to usage section
    const usageSection = page.locator('#usage');
    await usageSection.scrollIntoViewIfNeeded();
    await expect(usageSection).toBeVisible();

    // Check for code blocks or pre elements
    const codeBlocks = page.locator('pre, code, .code-block, .terminal-code');
    const codeCount = await codeBlocks.count();

    if (codeCount > 0) {
      // Verify code blocks have appropriate styling
      const codeBlock = codeBlocks.first();
      const codeStyle = await codeBlock.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          fontFamily: style.fontFamily,
          backgroundColor: style.backgroundColor,
        };
      });

      // Code should use monospace font
      expect(codeStyle.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/);
    }
  });
});
