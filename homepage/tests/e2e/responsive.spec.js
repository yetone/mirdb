/**
 * Responsive Design E2E Tests
 * Owner: Scenario 11 - Responsive Design
 *
 * Tests:
 * - Desktop layout (1920x1080)
 * - Laptop layout (1366x768)
 * - Tablet layout (768x1024)
 * - Mobile layout (375x667)
 * - Mobile navigation toggle
 * - Code blocks scrollable on mobile
 */

// @ts-check
const { test, expect } = require('@playwright/test');

// Viewport configurations
const viewports = {
  desktop: { width: 1920, height: 1080 },
  laptop: { width: 1366, height: 768 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 667 }
};

test.describe('Responsive Design - Desktop (1920x1080)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.desktop);
    await page.goto('/');
  });

  test('should display full navigation visible', async ({ page }) => {
    const navList = page.locator('.nav__list');
    await expect(navList).toBeVisible();

    // Navigation should not have mobile menu styles
    const navListDisplay = await navList.evaluate(el =>
      window.getComputedStyle(el).getPropertyValue('display')
    );
    expect(navListDisplay).toBe('flex');

    // Mobile menu toggle should be hidden
    const mobileToggle = page.locator('.mobile-menu-toggle');
    const toggleDisplay = await mobileToggle.evaluate(el =>
      window.getComputedStyle(el).getPropertyValue('display')
    );
    expect(toggleDisplay).toBe('none');
  });

  test('should have content readable without horizontal scrolling', async ({ page }) => {
    // Check that body does not have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.body.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('should display all sections properly', async ({ page }) => {
    // Verify all main sections are visible
    const sections = ['hero', 'features', 'quick-start', 'architecture',
                      'api-reference', 'configuration', 'performance', 'contributing'];

    for (const sectionId of sections) {
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeAttached();
    }
  });

  test('should display features grid in multiple columns', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    const gridColumns = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).getPropertyValue('grid-template-columns')
    );
    // Should have multiple columns (not single column)
    const columnCount = gridColumns.split(' ').filter(v => v !== 'none' && v !== '').length;
    expect(columnCount).toBeGreaterThan(1);
  });
});

test.describe('Responsive Design - Laptop (1366x768)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.laptop);
    await page.goto('/');
  });

  test('should display layout with all content accessible', async ({ page }) => {
    // Navigation should be visible
    const navList = page.locator('.nav__list');
    await expect(navList).toBeVisible();

    // Header should be present
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Footer should be present
    const footer = page.locator('.footer');
    await expect(footer).toBeAttached();
  });

  test('should adapt grid layouts appropriately', async ({ page }) => {
    // Features grid should adapt
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Architecture components should be visible
    const archComponents = page.locator('.architecture-components');
    await expect(archComponents).toBeVisible();
  });

  test('should have no horizontal scrolling', async ({ page }) => {
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.body.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

test.describe('Responsive Design - Tablet (768x1024)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.tablet);
    await page.goto('/');
  });

  test('should show hamburger menu on tablet', async ({ page }) => {
    const mobileToggle = page.locator('.mobile-menu-toggle');
    const toggleDisplay = await mobileToggle.evaluate(el =>
      window.getComputedStyle(el).getPropertyValue('display')
    );
    expect(toggleDisplay).toBe('flex');
  });

  test('should have readable text', async ({ page }) => {
    // Hero title should be visible and readable
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();

    // Check font size is appropriate
    const fontSize = await heroTitle.evaluate(el =>
      parseFloat(window.getComputedStyle(el).getPropertyValue('font-size'))
    );
    expect(fontSize).toBeGreaterThanOrEqual(20); // At least 20px
  });

  test('should have properly sized images', async ({ page }) => {
    // Hero logo should be visible
    const heroLogo = page.locator('.hero__logo');
    await expect(heroLogo).toBeVisible();

    // Logo should not overflow its container
    const logoBox = await heroLogo.boundingBox();
    const viewportWidth = viewports.tablet.width;
    expect(logoBox?.width).toBeLessThanOrEqual(viewportWidth);
  });

  test('should have navigation hidden by default', async ({ page }) => {
    // Nav list should be hidden initially
    const navList = page.locator('.nav__list');
    const navDisplay = await navList.evaluate(el =>
      window.getComputedStyle(el).getPropertyValue('display')
    );
    expect(navDisplay).toBe('none');
  });
});

test.describe('Responsive Design - Mobile (375x667)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.mobile);
    await page.goto('/');
  });

  test('should display mobile-optimized layout', async ({ page }) => {
    // Mobile menu toggle should be visible
    const mobileToggle = page.locator('.mobile-menu-toggle');
    await expect(mobileToggle).toBeVisible();
  });

  test('should have hamburger menu visible', async ({ page }) => {
    const mobileToggle = page.locator('.mobile-menu-toggle');
    const toggleDisplay = await mobileToggle.evaluate(el =>
      window.getComputedStyle(el).getPropertyValue('display')
    );
    expect(toggleDisplay).toBe('flex');

    // Should have 3 bars
    const bars = mobileToggle.locator('.mobile-menu-toggle__bar');
    await expect(bars).toHaveCount(3);
  });

  test('should have stacked content in single column', async ({ page }) => {
    // Features grid should be single column
    const featuresGrid = page.locator('.features-grid');
    const gridColumns = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).getPropertyValue('grid-template-columns')
    );
    // Single column should have one value (not multiple)
    const columnCount = gridColumns.split(' ').filter(v => v !== 'none' && v !== '' && v !== '0px').length;
    expect(columnCount).toBeLessThanOrEqual(1);
  });

  test('should have touch-friendly buttons', async ({ page }) => {
    // CTA button should be visible
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Check button has adequate height for touch
    const buttonBox = await ctaButton.boundingBox();
    expect(buttonBox?.height).toBeGreaterThanOrEqual(40);
  });

  test('should not have horizontal scrolling on body', async ({ page }) => {
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.body.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});

test.describe('Mobile Navigation Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.mobile);
    await page.goto('/');
  });

  test('should open mobile navigation menu when hamburger is clicked', async ({ page }) => {
    const mobileToggle = page.locator('.mobile-menu-toggle');
    const navList = page.locator('.nav__list');

    // Initially hidden
    await expect(navList).not.toBeVisible();

    // Click hamburger menu
    await mobileToggle.click();

    // Should now be visible
    await expect(navList).toBeVisible();

    // Menu toggle should have active class
    await expect(mobileToggle).toHaveClass(/active/);
  });

  test('should close mobile navigation menu when hamburger is clicked again', async ({ page }) => {
    const mobileToggle = page.locator('.mobile-menu-toggle');
    const navList = page.locator('.nav__list');

    // Open menu
    await mobileToggle.click();
    await expect(navList).toBeVisible();

    // Close menu
    await mobileToggle.click();
    await expect(navList).not.toBeVisible();
  });

  test('should have all navigation links in mobile menu', async ({ page }) => {
    const mobileToggle = page.locator('.mobile-menu-toggle');
    await mobileToggle.click();

    // Check for navigation links
    const navLinks = page.locator('.nav__link');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(5); // At least 5 nav links

    // Verify specific links exist
    await expect(page.locator('.nav__link[href="#features"]')).toBeVisible();
    await expect(page.locator('.nav__link[href="#quick-start"]')).toBeVisible();
    await expect(page.locator('.nav__link[href="#architecture"]')).toBeVisible();
  });

  test('should navigate correctly when nav link is clicked', async ({ page }) => {
    const mobileToggle = page.locator('.mobile-menu-toggle');
    await mobileToggle.click();

    // Click features link
    const featuresLink = page.locator('.nav__link[href="#features"]');
    await featuresLink.click();

    // Menu should close after clicking
    const navList = page.locator('.nav__list');
    await expect(navList).not.toBeVisible();

    // Wait for scroll animation to complete
    await page.waitForTimeout(600);

    // Verify the features section is now in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.3 });
  });

  test('should update aria-expanded attribute on toggle', async ({ page }) => {
    const mobileToggle = page.locator('.mobile-menu-toggle');

    // Initial state
    await expect(mobileToggle).toHaveAttribute('aria-expanded', 'false');

    // Open menu
    await mobileToggle.click();
    await expect(mobileToggle).toHaveAttribute('aria-expanded', 'true');

    // Close menu
    await mobileToggle.click();
    await expect(mobileToggle).toHaveAttribute('aria-expanded', 'false');
  });
});

test.describe('Code Blocks on Mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.mobile);
    await page.goto('/');
  });

  test('should be horizontally scrollable without breaking layout', async ({ page }) => {
    // Navigate to quick-start section
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    // Find code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Body should not have horizontal scroll
    const bodyHasHorizontalScroll = await page.evaluate(() => {
      return document.body.scrollWidth > document.documentElement.clientWidth;
    });
    expect(bodyHasHorizontalScroll).toBe(false);
  });

  test('should have code blocks with proper overflow styling', async ({ page }) => {
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    const codeBlockPre = page.locator('.code-block pre').first();
    await expect(codeBlockPre).toBeVisible();

    // Check overflow-x is auto or scroll
    const overflowX = await codeBlockPre.evaluate(el =>
      window.getComputedStyle(el).getPropertyValue('overflow-x')
    );
    expect(['auto', 'scroll']).toContain(overflowX);
  });

  test('should have readable code font size', async ({ page }) => {
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    const codeElement = page.locator('.code-block code').first();
    await expect(codeElement).toBeVisible();

    const fontSize = await codeElement.evaluate(el =>
      parseFloat(window.getComputedStyle(el).getPropertyValue('font-size'))
    );
    // Code should be at least 10px
    expect(fontSize).toBeGreaterThanOrEqual(10);
  });

  test('should display copy button in code blocks', async ({ page }) => {
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    const copyButton = page.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();

    // Button should be touchable
    const buttonBox = await copyButton.boundingBox();
    expect(buttonBox?.height).toBeGreaterThanOrEqual(30);
  });
});

test.describe('Responsive Content Accessibility', () => {
  test('should maintain proper heading hierarchy across viewports', async ({ page }) => {
    for (const [name, viewport] of Object.entries(viewports)) {
      await page.setViewportSize(viewport);
      await page.goto('/');

      // H1 should exist
      const h1 = page.locator('h1');
      await expect(h1).toBeAttached();

      // H2s should exist in sections
      const h2s = page.locator('h2');
      const h2Count = await h2s.count();
      expect(h2Count).toBeGreaterThan(0);
    }
  });

  test('should keep theme toggle accessible on all viewports', async ({ page }) => {
    for (const [name, viewport] of Object.entries(viewports)) {
      await page.setViewportSize(viewport);
      await page.goto('/');

      const themeToggle = page.locator('.theme-toggle');
      await expect(themeToggle).toBeVisible();
      await expect(themeToggle).toHaveAttribute('aria-label');
    }
  });
});
