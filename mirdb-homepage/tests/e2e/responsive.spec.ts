/**
 * Responsive Design E2E Tests
 * Owner: Scenario 10 - Mobile Responsive Design
 *
 * Tests for mobile/tablet responsiveness:
 * - 375px viewport (mobile)
 * - 768px viewport (tablet)
 * - Touch targets (44px minimum)
 * - Layout adaptations
 */

import { test, expect } from '@playwright/test';

test.describe('Mobile Responsive Design - 375px viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to mobile width (iPhone SE equivalent)
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: All content visible without horizontal scrolling (except code blocks)', async ({ page }) => {
    // Check that the body doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Allow small tolerance for rounding
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 2);

    // Verify all main sections are visible
    await expect(page.getByTestId('hero-section')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.quickstart')).toBeVisible();
    await expect(page.locator('.performance')).toBeVisible();
    await expect(page.locator('.documentation')).toBeVisible();
    await expect(page.locator('.contributing')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();
  });

  test('TC2: Features display in single column layout', async ({ page }) => {
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Get computed grid style
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Single column should not have multiple column definitions
    // At 375px, the grid should be 1fr (single column)
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(c => c && c !== '0px').length;
    expect(columnCount).toBeLessThanOrEqual(1);
  });

  test('TC3: CTA button has minimum 44px height for touch accessibility', async ({ page }) => {
    // Find all CTA buttons (primary buttons)
    const ctaButtons = page.locator('[data-testid="cta-button"]');
    const buttonCount = await ctaButtons.count();

    expect(buttonCount).toBeGreaterThan(0);

    // Check each CTA button has minimum 44px height
    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      const box = await button.boundingBox();

      if (box) {
        // WCAG 2.1 recommends 44x44px minimum touch target
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  test('TC4: Navigation is accessible (hamburger menu or visible)', async ({ page }) => {
    // At mobile width, navigation should either:
    // 1. Be a hamburger menu that can be toggled
    // 2. Or be fully visible/accessible

    // Check if there's a mobile menu button
    const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"], .mobile-menu-button, .hamburger, [aria-label*="menu"]');
    const navLinks = page.locator('nav a, header a, .nav-link, .header__nav a');

    // Either there's a mobile menu button OR nav links are accessible
    const hasMobileMenu = await mobileMenuButton.count() > 0;
    const hasVisibleNavLinks = await navLinks.first().isVisible().catch(() => false);

    // At minimum, the page should have some navigation mechanism
    expect(hasMobileMenu || hasVisibleNavLinks).toBeTruthy();

    // If there's a mobile menu, it should be interactive
    if (hasMobileMenu) {
      await expect(mobileMenuButton.first()).toBeEnabled();
    }
  });

  test('TC6: Footer content stacks appropriately for mobile', async ({ page }) => {
    const footerContent = page.locator('.footer__content');
    await expect(footerContent).toBeVisible();

    // Get computed grid style
    const gridStyle = await footerContent.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    // At 375px (which is less than 480px breakpoint), footer should be single column
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(c => c && c !== '0px').length;
    expect(columnCount).toBeLessThanOrEqual(1);
  });
});

test.describe('Tablet Responsive Design - 768px viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet width
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC5: Layout adapts appropriately for tablet viewport', async ({ page }) => {
    // Verify all sections are visible
    await expect(page.getByTestId('hero-section')).toBeVisible();
    await expect(page.locator('.features')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // At 768px, features should adapt (could be 1-2 columns based on implementation)
    const featuresGrid = page.locator('.features__grid');
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    // At tablet width, grid should have appropriate column layout
    // At 768px breakpoint, could be 1 or 2 columns
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(c => c && c !== '0px').length;
    expect(columnCount).toBeGreaterThanOrEqual(1);
    expect(columnCount).toBeLessThanOrEqual(3);

    // Footer should have adapted layout (2 columns at 768px)
    const footerContent = page.locator('.footer__content');
    const footerStyle = await footerContent.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    const footerColumnCount = footerStyle.gridTemplateColumns.split(' ').filter(c => c && c !== '0px').length;
    expect(footerColumnCount).toBeGreaterThanOrEqual(1);
    expect(footerColumnCount).toBeLessThanOrEqual(3);
  });
});

test.describe('Touch Target Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('Interactive elements have adequate touch targets', async ({ page }) => {
    // Check all clickable buttons have minimum 44px height
    const buttons = page.locator('button, .btn, [role="button"]');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const isVisible = await button.isVisible().catch(() => false);

      if (isVisible) {
        const box = await button.boundingBox();
        if (box) {
          // Check minimum touch target (44x44px recommended by WCAG)
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    }
  });

  test('Links in footer have adequate spacing for touch', async ({ page }) => {
    const footerLinks = page.locator('.footer__link, .footer a');
    const linkCount = await footerLinks.count();

    // Footer links should be present and spaced
    expect(linkCount).toBeGreaterThan(0);

    // Check that footer social links have adequate size
    const socialLinks = page.locator('.footer__social-link');
    const socialCount = await socialLinks.count();

    for (let i = 0; i < socialCount; i++) {
      const link = socialLinks.nth(i);
      const box = await link.boundingBox();

      if (box) {
        // Social links should have minimum 40px size
        expect(box.height).toBeGreaterThanOrEqual(40);
        expect(box.width).toBeGreaterThanOrEqual(40);
      }
    }
  });
});

test.describe('No Horizontal Overflow', () => {
  const viewports = [
    { width: 320, height: 568, name: 'iPhone SE (small)' },
    { width: 375, height: 667, name: 'iPhone SE' },
    { width: 390, height: 844, name: 'iPhone 12/13' },
    { width: 768, height: 1024, name: 'iPad' },
  ];

  for (const viewport of viewports) {
    test(`No horizontal overflow at ${viewport.name} (${viewport.width}px)`, async ({ page }) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that no element causes horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalOverflow).toBeFalsy();
    });
  }
});
