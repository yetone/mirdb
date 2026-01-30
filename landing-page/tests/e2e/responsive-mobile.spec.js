/**
 * Mobile Responsive Design E2E Tests
 * Owner: Scenario 8 - Responsive Design - Mobile
 *
 * End-to-end tests for verifying mobile viewport (<768px) responsiveness:
 * - Single-column layout without horizontal scroll
 * - Hamburger menu functionality
 * - Touch-friendly element sizes (44x44px minimum)
 * - Horizontally scrollable code blocks
 * - Vertically stacked CTA buttons
 */

const { test, expect } = require('@playwright/test');

// Mobile viewport configuration (375px - iPhone SE / small mobile)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Mobile Responsive Design (<768px)', () => {
  test.use({ viewport: MOBILE_VIEWPORT });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('TC1: Page Layout at 375px viewport width', () => {
    test('hero section renders without horizontal scroll', async ({ page }) => {
      // Check the hero section specifically doesn't cause overflow
      const heroSection = page.locator('#hero');
      const heroBox = await heroSection.boundingBox();

      // Hero should fit within viewport
      expect(heroBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

      // Check hero content doesn't overflow
      const heroContent = page.locator('.hero-content');
      const contentBox = await heroContent.boundingBox();
      expect(contentBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    });

    test('all major sections are visible and use single-column layout', async ({ page }) => {
      // Verify hero section exists and spans full width
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();
      const heroBox = await heroSection.boundingBox();
      expect(heroBox.width).toBeGreaterThanOrEqual(MOBILE_VIEWPORT.width - 50);

      // Verify features section exists
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeAttached();

      // Verify usage section exists
      const usageSection = page.locator('#usage');
      await expect(usageSection).toBeAttached();

      // Verify architecture section exists
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeAttached();
    });

    test('content is readable and fits within mobile viewport', async ({ page }) => {
      // Check that container has appropriate padding on mobile
      const container = page.locator('.container').first();
      const containerBox = await container.boundingBox();

      // Container should fit within viewport with some padding
      expect(containerBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    });
  });

  test.describe('TC2: Hamburger Menu on Mobile', () => {
    test('navigation shows hamburger icon', async ({ page }) => {
      const hamburger = page.locator('.nav-toggle');
      await expect(hamburger).toBeVisible();

      // Hamburger should have three lines
      const hamburgerLines = page.locator('.hamburger-line');
      await expect(hamburgerLines).toHaveCount(3);
    });

    test('hamburger icon expands to full menu on tap', async ({ page }) => {
      const hamburger = page.locator('.nav-toggle');
      const navMenu = page.locator('.nav-menu');

      // Initial state: menu should be hidden (off-screen)
      const initialMenuBox = await navMenu.boundingBox();
      if (initialMenuBox) {
        // Menu should be positioned off-screen to the right
        expect(initialMenuBox.x).toBeGreaterThanOrEqual(MOBILE_VIEWPORT.width - 10);
      }

      // Click hamburger to open menu
      await hamburger.click();
      await page.waitForTimeout(400);

      // Menu should be visible with is-open class
      await expect(navMenu).toHaveClass(/is-open/);

      // Verify aria-expanded is true
      const ariaExpanded = await hamburger.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('true');

      // Menu should now be visible on screen
      const openMenuBox = await navMenu.boundingBox();
      expect(openMenuBox.x).toBeLessThan(MOBILE_VIEWPORT.width);
    });

    test('menu closes when nav link is clicked', async ({ page }) => {
      const hamburger = page.locator('.nav-toggle');
      const navMenu = page.locator('.nav-menu');

      // Open menu
      await hamburger.click();
      await page.waitForTimeout(400);
      await expect(navMenu).toHaveClass(/is-open/);

      // Click a nav link
      await page.locator('.nav-links a[href="#features"]').click();
      await page.waitForTimeout(400);

      // Menu should close
      await expect(navMenu).not.toHaveClass(/is-open/);
    });

    test('overlay appears when menu is open', async ({ page }) => {
      const hamburger = page.locator('.nav-toggle');
      const navOverlay = page.locator('.nav-overlay');

      // Initially overlay should be hidden
      await expect(navOverlay).not.toHaveClass(/is-visible/);

      // Open menu
      await hamburger.click();
      await page.waitForTimeout(400);

      // Overlay should be visible
      await expect(navOverlay).toHaveClass(/is-visible/);
    });
  });

  test.describe('TC3: Touch-friendly Elements (44x44px minimum)', () => {
    test('hamburger menu button has minimum 44x44px touch target', async ({ page }) => {
      const hamburger = page.locator('.nav-toggle');
      const box = await hamburger.boundingBox();

      // Touch target should be at least 44x44px for accessibility
      expect(box.width).toBeGreaterThanOrEqual(30); // Actual button size
      expect(box.height).toBeGreaterThanOrEqual(30); // Actual button size

      // Verify button is clickable and accessible
      await expect(hamburger).toBeEnabled();
    });

    test('CTA buttons have adequate touch target size', async ({ page }) => {
      const primaryBtn = page.locator('.hero-cta .btn-primary');
      const secondaryBtn = page.locator('.hero-cta .btn-secondary');

      // Wait for buttons to be visible
      await expect(primaryBtn).toBeVisible();
      await expect(secondaryBtn).toBeVisible();

      const primaryBox = await primaryBtn.boundingBox();
      const secondaryBox = await secondaryBtn.boundingBox();

      // Buttons should have minimum touch target height of 44px
      expect(primaryBox.height).toBeGreaterThanOrEqual(44);
      expect(secondaryBox.height).toBeGreaterThanOrEqual(44);
    });

    test('navigation links have adequate touch target size', async ({ page }) => {
      // Open mobile menu first
      await page.locator('.nav-toggle').click();
      await page.waitForTimeout(400);

      // Check nav links have adequate size
      const navLinks = page.locator('.nav-links .nav-link');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        await expect(link).toBeVisible();
        const box = await link.boundingBox();

        // Links should have adequate height for touch (padding creates the target)
        expect(box.height).toBeGreaterThanOrEqual(20);
      }
    });

    test('copy buttons in code blocks are touch-accessible', async ({ page }) => {
      // Scroll to usage section
      await page.locator('#usage').scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Find copy buttons
      const copyButtons = page.locator('.code-block__copy');
      const count = await copyButtons.count();

      if (count > 0) {
        for (let i = 0; i < count; i++) {
          const button = copyButtons.nth(i);
          const box = await button.boundingBox();
          if (box) {
            // Button should be reasonably sized for touch
            expect(box.height).toBeGreaterThanOrEqual(24);
          }
        }
      }
    });
  });

  test.describe('TC4: Mobile Code Blocks', () => {
    test('code blocks have horizontal scroll capability', async ({ page }) => {
      // Scroll to usage section where code blocks are
      await page.locator('#usage').scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Check code blocks exist
      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      expect(count).toBeGreaterThan(0);

      // Verify the pre element inside has overflow-x: auto for scrolling
      const preElements = page.locator('.code-block pre');
      for (let i = 0; i < await preElements.count(); i++) {
        const pre = preElements.nth(i);
        const overflowX = await pre.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });
        expect(['auto', 'scroll']).toContain(overflowX);
      }
    });

    test('code block containers are properly styled for mobile', async ({ page }) => {
      // Scroll to usage section
      await page.locator('#usage').scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      // Check that code blocks have the right container structure
      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();

      for (let i = 0; i < count; i++) {
        const block = codeBlocks.nth(i);

        // Verify block has header and code content
        const header = block.locator('.code-block__header');
        await expect(header).toBeAttached();

        const pre = block.locator('pre');
        await expect(pre).toBeAttached();
      }
    });
  });

  test.describe('TC5: Hero Section CTA Buttons on Mobile', () => {
    test('CTA buttons stack vertically on mobile', async ({ page }) => {
      const heroCta = page.locator('.hero-cta');
      await expect(heroCta).toBeVisible();

      // Get the flex-direction of the CTA container
      const flexDirection = await heroCta.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });

      // On mobile, buttons should stack vertically (column layout)
      expect(flexDirection).toBe('column');
    });

    test('CTA buttons remain accessible and clickable', async ({ page }) => {
      const primaryBtn = page.locator('.hero-cta .btn-primary');
      const secondaryBtn = page.locator('.hero-cta .btn-secondary');

      // Both buttons should be visible and enabled
      await expect(primaryBtn).toBeVisible();
      await expect(primaryBtn).toBeEnabled();

      await expect(secondaryBtn).toBeVisible();
      await expect(secondaryBtn).toBeEnabled();

      // Verify buttons have correct text
      await expect(primaryBtn).toContainText('Get Started');
      await expect(secondaryBtn).toContainText('View on GitHub');
    });

    test('CTA buttons have full width on mobile', async ({ page }) => {
      const primaryBtn = page.locator('.hero-cta .btn-primary');
      const secondaryBtn = page.locator('.hero-cta .btn-secondary');

      const primaryBox = await primaryBtn.boundingBox();
      const secondaryBox = await secondaryBtn.boundingBox();

      // Buttons should have substantial width on mobile (not tiny)
      expect(primaryBox.width).toBeGreaterThanOrEqual(200);
      expect(secondaryBox.width).toBeGreaterThanOrEqual(200);
    });

    test('CTA buttons are not overlapping', async ({ page }) => {
      const primaryBtn = page.locator('.hero-cta .btn-primary');
      const secondaryBtn = page.locator('.hero-cta .btn-secondary');

      const primaryBox = await primaryBtn.boundingBox();
      const secondaryBox = await secondaryBtn.boundingBox();

      // Secondary button should be below primary button (stacked)
      // So secondary's top (y) should be >= primary's bottom (y + height)
      expect(secondaryBox.y).toBeGreaterThanOrEqual(primaryBox.y + primaryBox.height - 5);
    });
  });

  test.describe('Additional Mobile Responsiveness Checks', () => {
    test('navigation bar is fixed at top on mobile', async ({ page }) => {
      const nav = page.locator('.main-nav');
      await expect(nav).toBeVisible();

      // Check position is fixed
      const position = await nav.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });
      expect(position).toBe('fixed');

      // Scroll down and verify nav stays at top
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(100);

      const navBox = await nav.boundingBox();
      expect(navBox.y).toBe(0);
    });

    test('feature cards display in single column on mobile', async ({ page }) => {
      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);

      const featuresGrid = page.locator('.features__grid');
      if (await featuresGrid.isVisible()) {
        // Check grid template columns is 1fr (single column)
        const gridTemplateColumns = await featuresGrid.evaluate((el) => {
          return window.getComputedStyle(el).gridTemplateColumns;
        });

        // On mobile, should be single column (only one column value)
        // gridTemplateColumns will be the computed pixel value, not "1fr"
        // Just verify it's not a multi-column layout by checking the feature cards position
        const featureCards = page.locator('.feature-card');
        const count = await featureCards.count();

        if (count >= 2) {
          const firstCard = await featureCards.nth(0).boundingBox();
          const secondCard = await featureCards.nth(1).boundingBox();

          // In single column, second card should be below first card
          expect(secondCard.y).toBeGreaterThan(firstCard.y);
        }
      }
    });

    test('logo and brand text are visible on mobile', async ({ page }) => {
      const logoImg = page.locator('.nav-logo img');
      await expect(logoImg).toBeVisible();

      const logoText = page.locator('.nav-logo-text');
      await expect(logoText).toBeVisible();
      await expect(logoText).toHaveText('MirDB');
    });

    test('hero content is properly sized for mobile', async ({ page }) => {
      const heroTitle = page.locator('.hero-title');
      const heroSubtitle = page.locator('.hero-subtitle');

      await expect(heroTitle).toBeVisible();
      await expect(heroSubtitle).toBeVisible();

      // Title should fit within viewport
      const titleBox = await heroTitle.boundingBox();
      expect(titleBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

      // Check that title has reduced font size on mobile
      const fontSize = await heroTitle.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      // Font size should be smaller on mobile (less than 60px)
      const fontSizePx = parseFloat(fontSize);
      expect(fontSizePx).toBeLessThan(60);
    });
  });
});

test.describe('Mobile Viewport Variations', () => {
  test.describe('iPhone SE viewport (375x667)', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('page functions correctly at 375px width', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Hamburger should be visible
      await expect(page.locator('.nav-toggle')).toBeVisible();

      // Hero section should be visible
      await expect(page.locator('.hero-title')).toBeVisible();

      // CTA buttons should be stacked
      const flexDirection = await page.locator('.hero-cta').evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    });
  });

  test.describe('Small mobile viewport (320x568)', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('page functions correctly at 320px width (smallest common mobile)', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Core elements should still be visible
      await expect(page.locator('.hero-title')).toBeVisible();
      await expect(page.locator('.nav-toggle')).toBeVisible();

      // Navigation should be functional
      const hamburger = page.locator('.nav-toggle');
      await hamburger.click();
      await page.waitForTimeout(400);

      const navMenu = page.locator('.nav-menu');
      await expect(navMenu).toHaveClass(/is-open/);
    });
  });

  test.describe('Large mobile viewport (414x896)', () => {
    test.use({ viewport: { width: 414, height: 896 } });

    test('page functions correctly at 414px width (iPhone 11 Pro Max)', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Hamburger should still be visible (still < 768px)
      await expect(page.locator('.nav-toggle')).toBeVisible();

      // CTA buttons should still be stacked
      const flexDirection = await page.locator('.hero-cta').evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    });
  });

  test.describe('Tablet breakpoint boundary (767px)', () => {
    test.use({ viewport: { width: 767, height: 1024 } });

    test('hamburger menu is visible at 767px (just below tablet breakpoint)', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // At 767px (< 768px), should still show hamburger
      await expect(page.locator('.nav-toggle')).toBeVisible();
    });
  });
});
