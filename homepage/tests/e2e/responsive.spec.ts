/**
 * Responsive Design E2E Tests
 *
 * This file contains viewport-specific tests for mobile, tablet, and desktop views.
 * Scenario 8: Mobile tests (viewport < 768px)
 * Scenario 9: Tablet tests (768px - 1024px)
 * Scenario 10: Desktop tests (> 1024px)
 */

import { test, expect } from '@playwright/test';

// Mobile viewport dimensions (iPhone SE / typical mobile)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

/**
 * Scenario 8: Mobile Responsive Design Tests
 * Owner: Scenario 8 - Responsive Design - Mobile
 */
test.describe('Mobile Responsive Design (@mobile)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before each test
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Page renders without horizontal scrollbar on body', async ({ page }) => {
    // Verify no horizontal scrollbar by checking if body scroll width equals viewport width
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body content should not exceed viewport width
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Additional check: verify no overflow-x scrolling on html/body
    const htmlOverflowX = await page.evaluate(() => {
      const html = document.documentElement;
      return html.scrollWidth <= html.clientWidth;
    });
    expect(htmlOverflowX).toBe(true);
  });

  test('TC2: Navigation shows hamburger menu icon on mobile', async ({ page }) => {
    // Hamburger menu button should be visible on mobile
    const hamburgerButton = page.getByTestId('mobile-menu-button');
    await expect(hamburgerButton).toBeVisible();

    // Desktop nav list should be hidden on mobile
    const navList = page.locator('header nav ul[role="menubar"]');
    await expect(navList).toBeHidden();
  });

  test('TC3: Mobile navigation menu expands showing all navigation links', async ({ page }) => {
    // Click hamburger menu button
    const hamburgerButton = page.getByTestId('mobile-menu-button');
    await hamburgerButton.click();

    // Mobile menu should be open
    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toBeVisible();

    // Verify all navigation links are visible in mobile menu
    const homeLink = page.getByTestId('mobile-nav-link-home');
    const featuresLink = page.getByTestId('mobile-nav-link-features');
    const docsLink = page.getByTestId('mobile-nav-link-docs');
    const githubLink = page.getByTestId('mobile-nav-link-github');

    await expect(homeLink).toBeVisible();
    await expect(featuresLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Verify menu has correct aria attributes
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false');
    await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true');
  });

  test('TC4: Hero content is readable and CTA button is accessible on mobile', async ({ page }) => {
    // Hero section should be visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Headline should be visible and readable
    const headline = page.getByTestId('hero-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('MirDB');

    // Tagline should be visible
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toBeVisible();

    // CTA button should be visible and clickable
    const ctaButton = page.getByTestId('hero-cta');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toBeEnabled();

    // CTA button should be within viewport (not cut off)
    const ctaBox = await ctaButton.boundingBox();
    expect(ctaBox).not.toBeNull();
    if (ctaBox) {
      expect(ctaBox.x).toBeGreaterThanOrEqual(0);
      expect(ctaBox.x + ctaBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }
  });

  test('TC5: Code blocks have horizontal scroll within their container', async ({ page }) => {
    // Scroll to QuickStart section where code blocks are located
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Find code blocks
    const codeBlocks = page.getByTestId('code-block');
    const codeBlockCount = await codeBlocks.count();

    // Ensure we have at least one code block
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check first code block has overflow-x: auto (enabling horizontal scroll)
    const firstCodeBlock = codeBlocks.first();
    const preElement = firstCodeBlock.locator('pre');

    // Verify the pre element has overflow-x: auto
    const overflowX = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(overflowX).toBe('auto');

    // Verify code block doesn't break the page layout
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();
    if (codeBlockBox) {
      // Code block container should not exceed viewport width
      expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 1); // +1 for rounding
    }
  });

  test('TC6: Feature cards stack vertically in single column on mobile', async ({ page }) => {
    // Scroll to Features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get the features grid
    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid template columns - should be single column on mobile
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // On mobile (375px), grid should have 1 column (value should be a single column width)
    // gridTemplateColumns will be a single value like "343px" for 1 column
    const columnCount = gridColumns.split(' ').length;
    expect(columnCount).toBe(1);

    // Verify feature cards are stacked (each card takes full width)
    const featureCards = featuresGrid.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    if (cardCount > 1) {
      // Get positions of first two cards
      const firstCardBox = await featureCards.first().boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      expect(firstCardBox).not.toBeNull();
      expect(secondCardBox).not.toBeNull();

      if (firstCardBox && secondCardBox) {
        // Cards should be stacked vertically (second card's Y > first card's Y)
        expect(secondCardBox.y).toBeGreaterThan(firstCardBox.y);

        // Cards should be horizontally aligned (same X position)
        expect(Math.abs(firstCardBox.x - secondCardBox.x)).toBeLessThan(5);
      }
    }
  });

  test('Mobile menu closes when clicking a navigation link', async ({ page }) => {
    // Open mobile menu
    const hamburgerButton = page.getByTestId('mobile-menu-button');
    await hamburgerButton.click();

    // Click on Features link
    const featuresLink = page.getByTestId('mobile-nav-link-features');
    await featuresLink.click();

    // Menu should close
    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true');
  });

  test('Mobile menu closes when pressing Escape key', async ({ page }) => {
    // Open mobile menu
    const hamburgerButton = page.getByTestId('mobile-menu-button');
    await hamburgerButton.click();

    // Verify menu is open
    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false');

    // Press Escape key
    await page.keyboard.press('Escape');

    // Menu should close
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true');
  });

  test('All sections are visible and accessible on mobile', async ({ page }) => {
    // Verify all main sections are present and visible when scrolled to
    const sections = [
      { id: 'hero', testId: 'hero-section' },
      { id: 'features', selector: '#features' },
      { id: 'quickstart', selector: '#quickstart' },
      { id: 'performance', selector: '#performance' },
      { id: 'comparison', selector: '#comparison' },
    ];

    for (const section of sections) {
      const sectionElement = section.testId
        ? page.getByTestId(section.testId)
        : page.locator(section.selector!);

      await sectionElement.scrollIntoViewIfNeeded();
      await expect(sectionElement).toBeVisible();
    }
  });

  test('Text remains readable on mobile viewport', async ({ page }) => {
    // Check that main text elements have reasonable font sizes
    const headline = page.getByTestId('hero-headline');
    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Headline should be at least 24px on mobile for readability
    expect(headlineFontSize).toBeGreaterThanOrEqual(24);

    // Check tagline font size
    const tagline = page.getByTestId('hero-tagline');
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Tagline should be at least 16px for readability
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);
  });
});

/**
 * Scenario 9: Tablet Responsive Design Tests
 * Owner: Scenario 9 - Responsive Design - Tablet
 * Tests that the homepage displays correctly on tablet devices (768px - 1024px)
 */
test.describe('Tablet Responsive Design (768px - 1024px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet size (768px width)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('page renders with tablet-optimized layout at 768px viewport', async ({ page }) => {
    // Verify the page is visible and rendered
    await expect(page).toHaveTitle(/MirDB/i);

    // Verify main sections are visible
    const hero = page.locator('section').first();
    await expect(hero).toBeVisible();

    // Verify body is visible and page content is rendered
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify no horizontal scroll at tablet width
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding tolerance
  });

  test('navigation shows full menu on tablet (768px+)', async ({ page }) => {
    // At 768px, the navigation should show the full menu (not hamburger)
    const navList = page.locator('[role="menubar"]');
    await expect(navList).toBeVisible();

    // Verify navigation links are visible
    const homeLink = page.getByTestId('nav-link-home');
    const featuresLink = page.getByTestId('nav-link-features');
    const docsLink = page.getByTestId('nav-link-docs');
    const githubLink = page.getByTestId('nav-link-github');

    await expect(homeLink).toBeVisible();
    await expect(featuresLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Mobile menu button should be hidden at tablet size
    const mobileMenuButton = page.getByTestId('mobile-menu-button');
    await expect(mobileMenuButton).not.toBeVisible();
  });

  test('feature cards display in 2-column grid on tablet', async ({ page }) => {
    // Set viewport to 800px - a proper tablet width (above the 768px breakpoint)
    // CSS uses max-width: 768px for 1 column, so we need > 768px for 2 columns
    await page.setViewportSize({ width: 800, height: 1024 });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const grid = page.getByTestId('features-grid');
    await expect(grid).toBeVisible();

    // Check that grid uses 2 columns at tablet size
    const gridStyle = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns
      };
    });

    expect(gridStyle.display).toBe('grid');
    // At tablet (769px-1024px), CSS has grid-template-columns: repeat(2, 1fr)
    // This results in 2 column tracks
    const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(2);
  });

  test('comparison table is readable without horizontal scroll on tablet', async ({ page }) => {
    // Scroll to comparison section
    const comparisonSection = page.locator('#comparison');
    await comparisonSection.scrollIntoViewIfNeeded();
    await expect(comparisonSection).toBeVisible();

    // Get the comparison table wrapper
    const tableWrapper = page.getByTestId('comparison-table');
    await expect(tableWrapper).toBeVisible();

    // Check that the table fits within the viewport (no horizontal scroll needed)
    const tableInfo = await tableWrapper.evaluate((wrapper) => {
      const table = wrapper.querySelector('table');
      if (!table) return null;

      return {
        wrapperScrollWidth: wrapper.scrollWidth,
        wrapperClientWidth: wrapper.clientWidth,
        tableWidth: table.offsetWidth,
        wrapperWidth: wrapper.offsetWidth,
        hasHorizontalScroll: wrapper.scrollWidth > wrapper.clientWidth
      };
    });

    expect(tableInfo).not.toBeNull();

    // At 768px viewport, the table should fit without requiring horizontal scroll
    // Table has min-width: 600px which is less than 768px
    // Allow for some tolerance due to padding
    expect(tableInfo!.hasHorizontalScroll).toBe(false);
  });

  test('all major sections are accessible on tablet', async ({ page }) => {
    // Verify all major sections mentioned in REQ-9 are visible and accessible

    // Hero section
    const hero = page.locator('section').first();
    await expect(hero).toBeVisible();

    // Features section
    const features = page.locator('#features');
    await features.scrollIntoViewIfNeeded();
    await expect(features).toBeVisible();

    // Quick Start section
    const quickstart = page.locator('#quickstart');
    await quickstart.scrollIntoViewIfNeeded();
    await expect(quickstart).toBeVisible();

    // Performance section
    const performance = page.locator('#performance');
    await performance.scrollIntoViewIfNeeded();
    await expect(performance).toBeVisible();

    // Comparison section
    const comparison = page.locator('#comparison');
    await comparison.scrollIntoViewIfNeeded();
    await expect(comparison).toBeVisible();

    // Footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  test('tablet layout at 1024px (upper tablet boundary)', async ({ page }) => {
    // Test at upper tablet boundary (1024px)
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Navigation should still show full menu
    const navList = page.locator('[role="menubar"]');
    await expect(navList).toBeVisible();

    // Features grid should have 2 columns at exactly 1024px (max-width: 1024px applies)
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    const grid = page.getByTestId('features-grid');
    const gridStyle = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return computed.gridTemplateColumns;
    });

    // At exactly 1024px, the max-width: 1024px media query applies
    const columnCount = gridStyle.split(' ').length;
    expect(columnCount).toBe(2);
  });
});

// Desktop viewport dimensions (typical desktop)
const DESKTOP_VIEWPORT = { width: 1280, height: 800 };

/**
 * Scenario 10: Desktop Responsive Design Tests
 * Owner: Scenario 10 - Responsive Design - Desktop
 */
test.describe('Desktop Responsive Design (@desktop)', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport before each test
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Page renders with desktop layout and maximum content width', async ({ page }) => {
    // Verify page renders at 1280px viewport width
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(viewportWidth).toBe(DESKTOP_VIEWPORT.width);

    // Verify page has appropriate content width (not excessively wide)
    const container = page.locator('.container, [class*="container"]').first();
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();

    if (containerBox) {
      // Container should have a reasonable max-width for readability
      // Typically max-width is 1280px or less for content
      expect(containerBox.width).toBeLessThanOrEqual(DESKTOP_VIEWPORT.width);
      expect(containerBox.width).toBeGreaterThan(768); // Should utilize desktop space
    }

    // Verify no horizontal scrollbar on body
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.body.scrollWidth > document.body.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('TC2: Full navigation bar visible with all links on desktop', async ({ page }) => {
    // Desktop nav list should be visible
    const navList = page.locator('header nav ul[role="menubar"]');
    await expect(navList).toBeVisible();

    // Verify all navigation links are visible
    const homeLink = page.getByTestId('nav-link-home');
    const featuresLink = page.getByTestId('nav-link-features');
    const docsLink = page.getByTestId('nav-link-docs');
    const githubLink = page.getByTestId('nav-link-github');

    await expect(homeLink).toBeVisible();
    await expect(featuresLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(githubLink).toBeVisible();

    // Hamburger menu button should be hidden on desktop
    const hamburgerButton = page.getByTestId('mobile-menu-button');
    await expect(hamburgerButton).toBeHidden();

    // Verify nav links are horizontally aligned (not stacked)
    const homeBox = await homeLink.boundingBox();
    const featuresBox = await featuresLink.boundingBox();

    expect(homeBox).not.toBeNull();
    expect(featuresBox).not.toBeNull();

    if (homeBox && featuresBox) {
      // Links should be on the same row (similar Y position)
      expect(Math.abs(homeBox.y - featuresBox.y)).toBeLessThan(10);
      // Features link should be to the right of Home link
      expect(featuresBox.x).toBeGreaterThan(homeBox.x);
    }
  });

  test('TC3: Feature cards display in 3-column grid on desktop', async ({ page }) => {
    // Scroll to Features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get the features grid
    const featuresGrid = page.getByTestId('features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid template columns - should be 3 columns on desktop (>1024px)
    const gridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // gridTemplateColumns will be like "400px 400px 400px" for 3 columns
    const columnCount = gridColumns.split(' ').length;
    expect(columnCount).toBe(3);

    // Verify feature cards are arranged in rows (3 per row)
    const featureCards = featuresGrid.locator('[data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    // Should have at least 3 cards for a meaningful grid test
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Get positions of first three cards
    const firstCardBox = await featureCards.nth(0).boundingBox();
    const secondCardBox = await featureCards.nth(1).boundingBox();
    const thirdCardBox = await featureCards.nth(2).boundingBox();

    expect(firstCardBox).not.toBeNull();
    expect(secondCardBox).not.toBeNull();
    expect(thirdCardBox).not.toBeNull();

    if (firstCardBox && secondCardBox && thirdCardBox) {
      // All three cards should be on the same row (similar Y position)
      expect(Math.abs(firstCardBox.y - secondCardBox.y)).toBeLessThan(5);
      expect(Math.abs(secondCardBox.y - thirdCardBox.y)).toBeLessThan(5);

      // Cards should be horizontally arranged
      expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x);
      expect(thirdCardBox.x).toBeGreaterThan(secondCardBox.x);
    }
  });

  test('TC4: Hero section uses full width with centered content', async ({ page }) => {
    // Hero section should be visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Get hero section dimensions
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();

    if (heroBox) {
      // Hero should span full viewport width
      expect(heroBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT.width - 1);
    }

    // Verify content is centered
    const heroHeadline = page.getByTestId('hero-headline');
    const heroHeadlineBox = await heroHeadline.boundingBox();
    expect(heroHeadlineBox).not.toBeNull();

    if (heroHeadlineBox && heroBox) {
      // Calculate center position of headline relative to viewport
      const headlineCenter = heroHeadlineBox.x + heroHeadlineBox.width / 2;
      const viewportCenter = DESKTOP_VIEWPORT.width / 2;

      // Headline center should be close to viewport center (within 50px tolerance)
      expect(Math.abs(headlineCenter - viewportCenter)).toBeLessThan(50);
    }

    // Verify tagline is also centered
    const heroTagline = page.getByTestId('hero-tagline');
    const heroTaglineBox = await heroTagline.boundingBox();
    expect(heroTaglineBox).not.toBeNull();

    if (heroTaglineBox) {
      const taglineCenter = heroTaglineBox.x + heroTaglineBox.width / 2;
      const viewportCenter = DESKTOP_VIEWPORT.width / 2;
      expect(Math.abs(taglineCenter - viewportCenter)).toBeLessThan(50);
    }

    // Verify CTA button is visible and centered
    const ctaButton = page.getByTestId('hero-cta');
    await expect(ctaButton).toBeVisible();
    const ctaBox = await ctaButton.boundingBox();
    expect(ctaBox).not.toBeNull();

    if (ctaBox) {
      const ctaCenter = ctaBox.x + ctaBox.width / 2;
      const viewportCenter = DESKTOP_VIEWPORT.width / 2;
      expect(Math.abs(ctaCenter - viewportCenter)).toBeLessThan(50);
    }
  });

  test('All sections utilize full desktop width appropriately', async ({ page }) => {
    // Verify all main sections are present and properly displayed on desktop
    const sections = [
      { id: 'hero', testId: 'hero-section' },
      { id: 'features', selector: '#features' },
      { id: 'quickstart', selector: '#quickstart' },
      { id: 'performance', selector: '#performance' },
      { id: 'comparison', selector: '#comparison' },
    ];

    for (const section of sections) {
      const sectionElement = section.testId
        ? page.getByTestId(section.testId)
        : page.locator(section.selector!);

      await sectionElement.scrollIntoViewIfNeeded();
      await expect(sectionElement).toBeVisible();

      // Verify section spans appropriate width
      const sectionBox = await sectionElement.boundingBox();
      expect(sectionBox).not.toBeNull();

      if (sectionBox) {
        // Section should utilize most of the viewport width
        expect(sectionBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT.width * 0.9);
      }
    }
  });

  test('Desktop typography scales appropriately', async ({ page }) => {
    // Check that text elements have appropriate font sizes on desktop
    const headline = page.getByTestId('hero-headline');
    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Headline should be at least 32px on desktop for readability
    // (larger than mobile minimum of 24px)
    expect(headlineFontSize).toBeGreaterThanOrEqual(32);

    // Check tagline font size
    const tagline = page.getByTestId('hero-tagline');
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Tagline should be at least 16px on desktop for readability
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);
  });
});
