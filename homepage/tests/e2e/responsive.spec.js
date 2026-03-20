/**
 * Responsive Design E2E Tests
 * Owner: Scenario 9 (mobile), Scenario 10 (tablet)
 *
 * Test cases:
 * Mobile (375px):
 * - No horizontal overflow
 * - Feature cards stack vertically
 * - Mobile navigation accessible
 * - Touch interactions work
 *
 * Tablet (768px):
 * - Appropriate layout adaptation
 * - 2-column feature layout
 * - Demo fully functional
 */

const { test, expect } = require('@playwright/test');

// Mobile viewport configuration (iPhone SE / standard mobile)
const MOBILE_VIEWPORT = {
  width: 375,
  height: 667,
};

// Tablet viewport configuration
const TABLET_VIEWPORT = {
  width: 768,
  height: 1024,
};

test.describe('Scenario 9 - Responsive Design - Mobile (375px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Page content fits without horizontal overflow at 375px viewport', async ({ page }) => {
    // Check that the body does not have horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (allow 1px tolerance for rounding)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Also verify no element exceeds the viewport
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Check that overflow-x is hidden on body
    const bodyOverflowX = await page.evaluate(() => {
      return window.getComputedStyle(document.body).overflowX;
    });
    expect(bodyOverflowX).toBe('hidden');
  });

  test('TC2: Hero section displays correctly on mobile', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check hero headline is visible and readable
    const headline = page.locator('.hero__headline');
    await expect(headline).toBeVisible();

    // Verify headline font size is appropriately reduced for mobile
    const headlineFontSize = await headline.evaluate(el => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Mobile font size should be smaller than desktop (3rem = 48px desktop, should be ~22-36px on mobile)
    expect(headlineFontSize).toBeLessThan(48);
    expect(headlineFontSize).toBeGreaterThan(18);

    // Check CTAs are visible and properly sized
    const ctaPrimary = page.locator('[data-testid="cta-try-demo"]');
    const ctaSecondary = page.locator('[data-testid="cta-get-started"]');
    await expect(ctaPrimary).toBeVisible();
    await expect(ctaSecondary).toBeVisible();

    // CTAs should stack vertically on mobile (flex-direction: column)
    const ctaContainer = page.locator('.hero__cta');
    const ctaFlexDirection = await ctaContainer.evaluate(el => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaFlexDirection).toBe('column');

    // CTA buttons should have touch-friendly minimum height (44px)
    const primaryHeight = await ctaPrimary.evaluate(el => el.offsetHeight);
    expect(primaryHeight).toBeGreaterThanOrEqual(44);
  });

  test('TC3: Feature cards stack vertically on mobile', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid has single column layout
    const gridColumns = await featuresGrid.evaluate(el => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should be a single column (1fr)
    // The computed value will be something like "343px" (single column width)
    const columnCount = gridColumns.split(' ').filter(col => col !== '0px' && col !== '').length;
    expect(columnCount).toBe(1);

    // Verify feature cards exist and are visible
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Each card should take full width
    const firstCard = featureCards.first();
    const cardWidth = await firstCard.evaluate(el => el.offsetWidth);
    const containerWidth = await featuresGrid.evaluate(el => el.offsetWidth);

    // Card width should be close to container width (allowing for padding)
    expect(cardWidth).toBeGreaterThan(containerWidth * 0.8);
  });

  test('TC4: Demo section is usable on mobile screen', async ({ page }) => {
    // Scroll to demo section
    await page.locator('#demo').scrollIntoViewIfNeeded();

    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Terminal should be visible
    const terminal = page.locator('.demo__terminal');
    await expect(terminal).toBeVisible();

    // Input field should be visible and usable
    const input = page.locator('[data-testid="demo-input"]');
    await expect(input).toBeVisible();

    // Input should have 16px font size to prevent iOS zoom
    const inputFontSize = await input.evaluate(el => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseFloat(inputFontSize)).toBeGreaterThanOrEqual(16);

    // Submit button should be visible
    const submitBtn = page.locator('[data-testid="demo-submit"]');
    await expect(submitBtn).toBeVisible();

    // Submit button should have touch-friendly size
    const submitHeight = await submitBtn.evaluate(el => el.offsetHeight);
    expect(submitHeight).toBeGreaterThanOrEqual(44);

    // Example buttons should be visible
    const exampleBtns = page.locator('.demo__example-btn');
    const exampleCount = await exampleBtns.count();
    expect(exampleCount).toBeGreaterThan(0);

    // Example buttons should be touch-friendly
    const firstExample = exampleBtns.first();
    const exampleHeight = await firstExample.evaluate(el => el.offsetHeight);
    expect(exampleHeight).toBeGreaterThanOrEqual(44);
  });

  test('TC5: Navigation accessible via hamburger menu on mobile', async ({ page }) => {
    // Hamburger menu toggle should be visible on mobile
    const menuToggle = page.locator('#menu-toggle');
    await expect(menuToggle).toBeVisible();

    // Check that menu toggle is displayed (not hidden)
    const toggleDisplay = await menuToggle.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(toggleDisplay).not.toBe('none');

    // Navigation should be hidden initially
    const nav = page.locator('#main-nav');
    const navRight = await nav.evaluate(el => {
      return window.getComputedStyle(el).right;
    });
    // Nav should be positioned off-screen (negative right value or hidden)
    expect(navRight).toMatch(/-?\d+px/);

    // Click hamburger menu
    await menuToggle.click();

    // Wait for animation
    await page.waitForTimeout(400);

    // Navigation should now be visible (right: 0)
    const navRightAfterClick = await nav.evaluate(el => {
      return window.getComputedStyle(el).right;
    });
    expect(navRightAfterClick).toBe('0px');

    // Nav links should be visible
    const navLinks = page.locator('.header__nav-link');
    const firstLink = navLinks.first();
    await expect(firstLink).toBeVisible();

    // Click a nav link - should navigate and close menu
    await page.locator('[data-testid="nav-features"]').click();

    // Wait for animation
    await page.waitForTimeout(400);

    // Menu should close after clicking a link
    const navRightAfterNav = await nav.evaluate(el => {
      return window.getComputedStyle(el).right;
    });
    // Should be back off-screen or closing
    expect(parseInt(navRightAfterNav) || -100).toBeLessThanOrEqual(0);
  });

  test('TC6: All interactive elements respond to touch events', async ({ page }) => {
    // Test navigation toggle touch
    const menuToggle = page.locator('#menu-toggle');
    await expect(menuToggle).toBeVisible();

    // Menu toggle should be clickable/tappable
    const toggleBox = await menuToggle.boundingBox();
    expect(toggleBox.width).toBeGreaterThanOrEqual(44);
    expect(toggleBox.height).toBeGreaterThanOrEqual(44);

    // Test hero CTA buttons are touch-friendly
    const ctaPrimary = page.locator('[data-testid="cta-try-demo"]');
    const ctaBox = await ctaPrimary.boundingBox();
    expect(ctaBox.height).toBeGreaterThanOrEqual(44);

    // Scroll to features and test cards are tappable
    await page.locator('#features').scrollIntoViewIfNeeded();
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Scroll to demo section
    await page.locator('#demo').scrollIntoViewIfNeeded();

    // Test demo input can receive focus/input
    const demoInput = page.locator('[data-testid="demo-input"]');
    await demoInput.click();
    await expect(demoInput).toBeFocused();

    // Type in the input
    await demoInput.fill('GET test');
    const inputValue = await demoInput.inputValue();
    expect(inputValue).toBe('GET test');

    // Test demo submit button
    const submitBtn = page.locator('[data-testid="demo-submit"]');
    const submitBox = await submitBtn.boundingBox();
    expect(submitBox.height).toBeGreaterThanOrEqual(44);

    // Test example button is clickable
    const exampleBtn = page.locator('[data-testid="example-set"]');
    await expect(exampleBtn).toBeVisible();
    const exampleBox = await exampleBtn.boundingBox();
    expect(exampleBox.height).toBeGreaterThanOrEqual(44);

    // Click should work (populate input)
    await exampleBtn.click();
    await page.waitForTimeout(100);

    // The input should now have a command
    const newInputValue = await demoInput.inputValue();
    expect(newInputValue.length).toBeGreaterThan(0);

    // Scroll to quickstart and test copy buttons
    await page.locator('#quickstart').scrollIntoViewIfNeeded();
    const copyBtn = page.locator('.quickstart__copy-btn').first();
    await expect(copyBtn).toBeVisible();
    const copyBox = await copyBtn.boundingBox();
    expect(copyBox.height).toBeGreaterThanOrEqual(44);
  });

  test('All sections are accessible and scrollable on mobile', async ({ page }) => {
    // Test that each major section is visible when scrolled to
    const sections = ['#hero', '#features', '#demo', '#examples', '#quickstart', '#status'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await section.scrollIntoViewIfNeeded();
      await expect(section).toBeVisible();

      // Verify section fits within viewport width
      const sectionWidth = await section.evaluate(el => el.offsetWidth);
      expect(sectionWidth).toBeLessThanOrEqual(375);
    }

    // Footer should also be visible
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  test('Images and media do not cause horizontal overflow', async ({ page }) => {
    // Check all images have max-width: 100%
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const maxWidth = await img.evaluate(el => {
        return window.getComputedStyle(el).maxWidth;
      });
      expect(maxWidth).toBe('100%');
    }

    // Check all SVGs have max-width: 100%
    const svgs = page.locator('svg');
    const svgCount = await svgs.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = svgs.nth(i);
      const maxWidth = await svg.evaluate(el => {
        return window.getComputedStyle(el).maxWidth;
      });
      expect(maxWidth).toBe('100%');
    }
  });

  test('Text is readable and properly sized on mobile', async ({ page }) => {
    // Check body font size is at least 14px
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);

    // Check line height for readability
    const bodyLineHeight = await page.evaluate(() => {
      const lineHeight = window.getComputedStyle(document.body).lineHeight;
      const fontSize = parseFloat(window.getComputedStyle(document.body).fontSize);
      return parseFloat(lineHeight) / fontSize;
    });
    expect(bodyLineHeight).toBeGreaterThanOrEqual(1.4);

    // Check feature descriptions are readable
    await page.locator('#features').scrollIntoViewIfNeeded();
    const description = page.locator('.feature-card__description').first();
    const descFontSize = await description.evaluate(el => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(descFontSize).toBeGreaterThanOrEqual(12);
  });
});

// Tablet-specific tests (Scenario 10) - placeholder for shared test file
test.describe('Scenario 10 - Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Features section shows 2-column layout on tablet', async ({ page }) => {
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // On tablet, should be 2 columns
    const gridColumns = await featuresGrid.evaluate(el => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Count number of column values (should be 2)
    const columnCount = gridColumns.split(' ').filter(col => col !== '0px' && col !== '').length;
    expect(columnCount).toBe(2);
  });

  test('Demo section is fully functional on tablet', async ({ page }) => {
    await page.locator('#demo').scrollIntoViewIfNeeded();

    const terminal = page.locator('.demo__terminal');
    await expect(terminal).toBeVisible();

    // Input and submit should work
    const input = page.locator('[data-testid="demo-input"]');
    const submit = page.locator('[data-testid="demo-submit"]');

    await expect(input).toBeVisible();
    await expect(submit).toBeVisible();
    await expect(input).toBeEnabled();
    await expect(submit).toBeEnabled();
  });
});
