/**
 * E2E tests for responsive design
 * Owner: Scenario 6 - Responsive Design
 *
 * Expected test suites:
 * - Mobile viewport (375x667)
 * - Tablet viewport (768x1024)
 * - Desktop viewport (1920x1080)
 * - Navigation collapse behavior
 * - Grid layout changes
 */

const { test, expect } = require('@playwright/test');

// Viewport sizes
const VIEWPORTS = {
  desktop: { width: 1920, height: 1080 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 667 },
};

// Minimum touch target size (WCAG 2.5.5)
const MIN_TOUCH_TARGET = 44;

test.describe('Responsive Design', () => {
  test.describe('Desktop Viewport (1920x1080)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
    });

    test('Test Case 1: Page renders without horizontal scroll at 1920x1080', async ({ page }) => {
      // Check that page width doesn't exceed viewport
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify all main sections are visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
      await expect(page.locator('#usage')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
    });

    test('Navigation is fully visible on desktop', async ({ page }) => {
      const nav = page.locator('.header-nav');
      await expect(nav).toBeVisible();

      // Mobile menu toggle should be hidden
      const menuToggle = page.locator('.mobile-menu-toggle');
      await expect(menuToggle).toBeHidden();

      // All nav links should be visible
      const navLinks = page.locator('.header-nav a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    });

    test('Features grid displays 3 columns on desktop', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyle = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
          gridTemplateColumns: style.gridTemplateColumns,
        };
      });

      expect(gridStyle.display).toBe('grid');
      // Should have 3 columns (3 fr values or explicit widths)
      const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c.trim());
      expect(columns.length).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('Tablet Viewport (768x1024)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto('/');
    });

    test('Test Case 2: Page renders without horizontal scroll at 768x1024', async ({ page }) => {
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('Test Case 6: Feature grid adjusts to 2 columns on tablet', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyle = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Should have 2 columns
      const columns = gridStyle.split(' ').filter(c => c.trim() && c !== '0px');
      expect(columns.length).toBe(2);
    });

    test('Navigation collapses on tablet', async ({ page }) => {
      // Mobile menu toggle should be visible
      const menuToggle = page.locator('.mobile-menu-toggle');
      await expect(menuToggle).toBeVisible();

      // Navigation should initially be hidden (collapsed)
      const nav = page.locator('.header-nav');
      const isNavHidden = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.visibility === 'hidden' || style.opacity === '0';
      });
      expect(isNavHidden).toBe(true);
    });
  });

  test.describe('Mobile Viewport (375x667)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
    });

    test('Test Case 3: Page renders without horizontal scroll at 375x667', async ({ page }) => {
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('Test Case 4: Navigation is collapsed with hamburger menu visible on mobile', async ({ page }) => {
      // Mobile menu toggle (hamburger) should be visible
      const menuToggle = page.locator('.mobile-menu-toggle');
      await expect(menuToggle).toBeVisible();

      // Hamburger lines should be visible
      const hamburgerLines = page.locator('.hamburger-line');
      await expect(hamburgerLines.first()).toBeVisible();

      // Navigation should be collapsed/hidden
      const nav = page.locator('.header-nav');
      const isNavHidden = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.visibility === 'hidden' || style.opacity === '0';
      });
      expect(isNavHidden).toBe(true);
    });

    test('Test Case 5: Navigation menu expands on mobile menu toggle click', async ({ page }) => {
      const menuToggle = page.locator('.mobile-menu-toggle');
      const nav = page.locator('.header-nav');

      // Initial state - nav should be hidden
      let isNavHidden = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.visibility === 'hidden' || style.opacity === '0';
      });
      expect(isNavHidden).toBe(true);

      // Click the menu toggle
      await menuToggle.click();

      // Wait for transition and check visibility
      await page.waitForTimeout(300);

      // Navigation should now be visible
      const isNavVisible = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.visibility === 'visible' && style.opacity === '1';
      });
      expect(isNavVisible).toBe(true);

      // Menu toggle aria-expanded should be true
      const ariaExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('true');

      // Nav links should be visible
      const navLinks = page.locator('.header-nav a');
      await expect(navLinks.first()).toBeVisible();
    });

    test('Test Case 7: Features grid displays as single column on mobile', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyle = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Should have 1 column (single value)
      const columns = gridStyle.split(' ').filter(c => c.trim() && c !== '0px');
      expect(columns.length).toBe(1);
    });

    test('Test Case 8: Hero section is readable on mobile', async ({ page }) => {
      const heroTitle = page.locator('.hero-title');
      const heroTagline = page.locator('.hero-tagline');

      await expect(heroTitle).toBeVisible();
      await expect(heroTagline).toBeVisible();

      // Check that text is appropriately sized (not too small)
      const titleFontSize = await heroTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      const taglineFontSize = await heroTagline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Title should be at least 24px (1.5rem at 16px base)
      expect(titleFontSize).toBeGreaterThanOrEqual(24);

      // Tagline should be at least 14px
      expect(taglineFontSize).toBeGreaterThanOrEqual(14);

      // Check that content doesn't overflow
      const heroContent = page.locator('.hero-content');
      const heroWidth = await heroContent.evaluate((el) => el.scrollWidth);
      const viewportWidth = VIEWPORTS.mobile.width;
      expect(heroWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('Test Case 9: Code blocks are scrollable on mobile', async ({ page }) => {
      // Scroll to usage section
      await page.locator('#usage').scrollIntoViewIfNeeded();

      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const codeBlock = codeBlocks.nth(i);

        // Check that overflow-x is auto or scroll
        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });

        expect(['auto', 'scroll']).toContain(overflowX);

        // Check that code block container doesn't exceed viewport width
        const containerWidth = await codeBlock.locator('..').evaluate((el) => {
          return el.getBoundingClientRect().width;
        });

        expect(containerWidth).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
      }
    });

    test('Test Case 10: Touch targets are at least 44x44 pixels on mobile', async ({ page }) => {
      // Test mobile menu toggle
      const menuToggle = page.locator('.mobile-menu-toggle');
      const toggleBox = await menuToggle.boundingBox();
      expect(toggleBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(toggleBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

      // Test hero CTA button
      const heroCta = page.locator('.hero-cta');
      const ctaBox = await heroCta.boundingBox();
      expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

      // Test copy buttons when visible
      await page.locator('#usage').scrollIntoViewIfNeeded();
      const copyButtons = page.locator('.copy-btn');
      const copyCount = await copyButtons.count();

      if (copyCount > 0) {
        const firstCopyBtn = copyButtons.first();
        const copyBox = await firstCopyBtn.boundingBox();
        expect(copyBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(copyBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }

      // Open mobile menu and test nav links
      await menuToggle.click();
      await page.waitForTimeout(300);

      const navLinks = page.locator('.header-nav a');
      const navCount = await navLinks.count();

      for (let i = 0; i < navCount; i++) {
        const link = navLinks.nth(i);
        const linkBox = await link.boundingBox();
        expect(linkBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    });

    test('Status container displays single column on mobile', async ({ page }) => {
      await page.locator('#status').scrollIntoViewIfNeeded();

      const statusContainer = page.locator('.status-container');
      await expect(statusContainer).toBeVisible();

      const gridStyle = await statusContainer.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Should have 1 column
      const columns = gridStyle.split(' ').filter(c => c.trim() && c !== '0px');
      expect(columns.length).toBe(1);
    });

    test('Tech stack grid displays single column on mobile', async ({ page }) => {
      await page.locator('#tech-stack').scrollIntoViewIfNeeded();

      const techGrid = page.locator('.tech-stack-grid');
      await expect(techGrid).toBeVisible();

      const gridStyle = await techGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Should have 1 column
      const columns = gridStyle.split(' ').filter(c => c.trim() && c !== '0px');
      expect(columns.length).toBe(1);
    });

    test('Footer displays single column on mobile', async ({ page }) => {
      await page.locator('footer').scrollIntoViewIfNeeded();

      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();

      const footerStyle = await footerContent.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          flexDirection: style.flexDirection,
          display: style.display,
        };
      });

      // Should be flex column
      expect(footerStyle.display).toBe('flex');
      expect(footerStyle.flexDirection).toBe('column');
    });
  });

  test.describe('Navigation Collapse Behavior', () => {
    test('Mobile menu toggle closes menu when clicking link', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');

      const menuToggle = page.locator('.mobile-menu-toggle');
      const nav = page.locator('.header-nav');

      // Open menu
      await menuToggle.click();
      await page.waitForTimeout(300);

      // Click a nav link
      const firstInternalLink = page.locator('.header-nav a[href^="#"]').first();
      await firstInternalLink.click();

      // Wait for any transition
      await page.waitForTimeout(300);

      // Menu should close
      const isNavHidden = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.visibility === 'hidden' || style.opacity === '0';
      });
      expect(isNavHidden).toBe(true);
    });

    test('Mobile menu toggle animation works correctly', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');

      const menuToggle = page.locator('.mobile-menu-toggle');

      // Initial state - aria-expanded should be false
      let ariaExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('false');

      // Click to open
      await menuToggle.click();
      await page.waitForTimeout(100);

      ariaExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('true');

      // Click to close
      await menuToggle.click();
      await page.waitForTimeout(100);

      ariaExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('false');
    });
  });

  test.describe('Layout Consistency Across Viewports', () => {
    test('All sections maintain proper structure across viewports', async ({ page }) => {
      const viewports = [VIEWPORTS.desktop, VIEWPORTS.tablet, VIEWPORTS.mobile];

      for (const viewport of viewports) {
        await page.setViewportSize(viewport);
        await page.goto('/');

        // Check that all main sections exist and are visible when scrolled to
        const sections = ['#hero', '#overview', '#features', '#status', '#usage', '#quickstart', '#tech-stack'];

        for (const section of sections) {
          const sectionElement = page.locator(section);
          await sectionElement.scrollIntoViewIfNeeded();
          await expect(sectionElement).toBeVisible();
        }

        // Check footer
        const footer = page.locator('footer');
        await footer.scrollIntoViewIfNeeded();
        await expect(footer).toBeVisible();
      }
    });

    test('No content overflow at any viewport', async ({ page }) => {
      const viewports = [VIEWPORTS.desktop, VIEWPORTS.tablet, VIEWPORTS.mobile];

      for (const viewport of viewports) {
        await page.setViewportSize(viewport);
        await page.goto('/');

        // Wait for page to fully render
        await page.waitForLoadState('networkidle');

        // Check body doesn't exceed viewport width
        const hasOverflow = await page.evaluate(() => {
          return document.body.scrollWidth > window.innerWidth;
        });

        expect(hasOverflow).toBe(false);
      }
    });
  });
});
