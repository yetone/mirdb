// @ts-check
const { test, expect } = require('@playwright/test');

// Define viewport sizes for testing
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1440, height: 900 },
};

test.describe('Mobile Responsive Design - NFR-2', () => {
  test.describe('Test Case 1: Mobile viewport (375px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
    });

    test('page renders correctly without horizontal overflow', async ({ page }) => {
      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that body doesn't overflow horizontally
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = VIEWPORTS.mobile.width;

      // Body scroll width should not exceed viewport width
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Additionally check that no element causes horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('all main sections are visible and accessible', async ({ page }) => {
      // Verify all main sections render on mobile
      await expect(page.locator('[data-testid="navigation"]')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="comparison-table"]')).toBeVisible();
      await expect(page.locator('[data-testid="quick-start-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="architecture-section"]')).toBeVisible();
    });
  });

  test.describe('Test Case 2: Tablet viewport (768px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.goto('/');
    });

    test('page renders correctly with tablet layout', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Check no horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify key sections are visible
      await expect(page.locator('[data-testid="navigation"]')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Table should still be functional (might have horizontal scroll within container)
      const tableContainer = page.locator('.table-container');
      await expect(tableContainer).toBeVisible();
    });

    test('navigation links are visible and functional', async ({ page }) => {
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // All navigation items should be accessible
      await expect(page.locator('[data-testid="nav-getting-started"]')).toBeVisible();
      await expect(page.locator('[data-testid="nav-documentation"]')).toBeVisible();
      await expect(page.locator('[data-testid="nav-github"]')).toBeVisible();
    });
  });

  test.describe('Test Case 3: Desktop viewport (1440px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.goto('/');
    });

    test('page renders correctly with full desktop layout', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Check no horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // All sections should be fully visible
      await expect(page.locator('[data-testid="navigation"]')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="comparison-table"]')).toBeVisible();
      await expect(page.locator('[data-testid="quick-start-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="architecture-section"]')).toBeVisible();
    });

    test('comparison table is fully visible without scroll', async ({ page }) => {
      const table = page.locator('.comparison-table');
      await expect(table).toBeVisible();

      // Table should have reasonable width for desktop
      const tableBoundingBox = await table.boundingBox();
      expect(tableBoundingBox).not.toBeNull();
      expect(tableBoundingBox.width).toBeGreaterThan(600);
    });

    test('hero section has proper layout', async ({ page }) => {
      // CTA buttons should be side by side on desktop
      const ctaButtons = page.locator('.hero-cta-buttons');
      await expect(ctaButtons).toBeVisible();

      const buttonBox = await ctaButtons.boundingBox();
      expect(buttonBox).not.toBeNull();

      // Hero title should be larger on desktop
      const heroTitle = page.locator('[data-testid="hero-title"]');
      const titleFontSize = await heroTitle.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      // Desktop should have larger font (3.5rem = 56px)
      expect(parseFloat(titleFontSize)).toBeGreaterThanOrEqual(40);
    });
  });

  test.describe('Test Case 4: Navigation on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
    });

    test('navigation is accessible on mobile', async ({ page }) => {
      // Navigation should be visible
      const navigation = page.locator('[data-testid="navigation"]');
      await expect(navigation).toBeVisible();

      // Brand should be visible
      const brand = page.locator('[data-testid="nav-brand"]');
      await expect(brand).toBeVisible();

      // Navigation links should be present (even if styled differently)
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // Each link should be accessible
      const gettingStartedLink = page.locator('[data-testid="nav-getting-started"]');
      const docsLink = page.locator('[data-testid="nav-documentation"]');
      const githubLink = page.locator('[data-testid="nav-github"]');

      await expect(gettingStartedLink).toBeVisible();
      await expect(docsLink).toBeVisible();
      await expect(githubLink).toBeVisible();
    });

    test('navigation links are clickable and keyboard accessible', async ({ page }) => {
      const gettingStartedLink = page.locator('[data-testid="nav-getting-started"]');

      // Should be focusable
      await gettingStartedLink.focus();
      const isFocused = await gettingStartedLink.evaluate((el) => {
        return document.activeElement === el;
      });
      expect(isFocused).toBe(true);

      // Should be clickable
      await expect(gettingStartedLink).toBeEnabled();
    });

    test('navigation does not overflow on mobile', async ({ page }) => {
      const navContainer = page.locator('.nav-container');
      const navBox = await navContainer.boundingBox();

      expect(navBox).not.toBeNull();
      // Navigation should fit within mobile viewport
      expect(navBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
    });
  });

  test.describe('Test Case 5: Feature cards on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
    });

    test('comparison table cells stack/scroll appropriately on mobile', async ({ page }) => {
      // The comparison table has a .table-container with overflow-x: auto for responsive handling
      const tableContainer = page.locator('.table-container');
      await expect(tableContainer).toBeVisible();

      // Table should be contained within a scrollable container on mobile
      const hasOverflowAuto = await tableContainer.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.overflowX === 'auto' || style.overflowX === 'scroll';
      });
      expect(hasOverflowAuto).toBe(true);
    });

    test('CTA buttons stack vertically on mobile', async ({ page }) => {
      const ctaContainer = page.locator('.hero-cta-buttons');
      await expect(ctaContainer).toBeVisible();

      // Check flex-direction is column on mobile (buttons stack)
      const flexDirection = await ctaContainer.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');

      // Both buttons should be visible
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      const githubBtn = page.locator('[data-testid="cta-github"]');

      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();
    });

    test('status badges wrap appropriately on mobile', async ({ page }) => {
      const badgesContainer = page.locator('.hero-badges');
      await expect(badgesContainer).toBeVisible();

      // Badges should have flex-wrap enabled
      const flexWrap = await badgesContainer.evaluate((el) => {
        return window.getComputedStyle(el).flexWrap;
      });
      expect(flexWrap).toBe('wrap');

      // All badges should be visible
      await expect(page.locator('[data-testid="badge-build-status"]')).toBeVisible();
      await expect(page.locator('[data-testid="badge-version"]')).toBeVisible();
      await expect(page.locator('[data-testid="badge-license"]')).toBeVisible();
    });
  });

  test.describe('Test Case 6: Images on mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.goto('/');
    });

    test('logo image scales appropriately on mobile', async ({ page }) => {
      const logo = page.locator('[data-testid="hero-logo"]');
      await expect(logo).toBeVisible();

      const logoBox = await logo.boundingBox();
      expect(logoBox).not.toBeNull();

      // Logo should fit within viewport with padding
      expect(logoBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width - 32); // Allow for padding

      // Logo should have reasonable size (not too small)
      expect(logoBox.width).toBeGreaterThanOrEqual(60);
    });

    test('architecture diagram scales appropriately on mobile', async ({ page }) => {
      // Scroll to architecture section
      const architectureSection = page.locator('[data-testid="architecture-section"]');
      await architectureSection.scrollIntoViewIfNeeded();

      const diagram = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      const diagramBox = await diagram.boundingBox();
      expect(diagramBox).not.toBeNull();

      // Diagram should fit within mobile viewport
      expect(diagramBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

      // Diagram should still be visible and readable (not too small)
      expect(diagramBox.width).toBeGreaterThanOrEqual(200);
    });

    test('badge images scale appropriately', async ({ page }) => {
      const badges = page.locator('.hero-badges .badge img');
      const badgeCount = await badges.count();

      expect(badgeCount).toBeGreaterThan(0);

      // Check each badge image
      for (let i = 0; i < badgeCount; i++) {
        const badge = badges.nth(i);
        await expect(badge).toBeVisible();

        const badgeBox = await badge.boundingBox();
        expect(badgeBox).not.toBeNull();
        // Badges should not overflow viewport
        expect(badgeBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
      }
    });
  });

  test.describe('Cross-breakpoint consistency', () => {
    test('content is readable at all breakpoints', async ({ page }) => {
      for (const [name, viewport] of Object.entries(VIEWPORTS)) {
        await page.setViewportSize(viewport);
        await page.goto('/');

        // Hero title should be visible and readable
        const heroTitle = page.locator('[data-testid="hero-title"]');
        await expect(heroTitle).toBeVisible();

        // Tagline should be visible
        const heroTagline = page.locator('[data-testid="hero-tagline"]');
        await expect(heroTagline).toBeVisible();

        // Font size should be reasonable (min 16px)
        const taglineFontSize = await heroTagline.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
        expect(taglineFontSize).toBeGreaterThanOrEqual(16);
      }
    });

    test('no horizontal overflow at any breakpoint', async ({ page }) => {
      for (const [name, viewport] of Object.entries(VIEWPORTS)) {
        await page.setViewportSize(viewport);
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });
        expect(hasHorizontalScroll).toBe(false);
      }
    });
  });
});
