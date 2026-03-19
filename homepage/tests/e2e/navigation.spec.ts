/**
 * E2E tests for Navigation Header.
 * Owner: Scenario 8 - Navigation Header
 *
 * Tests:
 * - Smooth scroll navigation
 * - Fixed header positioning
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('navigation header renders with MirDB logo and section links', async ({ page }) => {
    // Check header is present
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check logo/brand is present
    const logo = header.getByText('MirDB');
    await expect(logo).toBeVisible();

    // Check all navigation links are present
    await expect(page.getByRole('menuitem', { name: 'Features' })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: 'How It Works' })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: 'Status' })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: 'Quick Start' })).toBeVisible();
    await expect(page.getByRole('menuitem', { name: 'Resources' })).toBeVisible();
  });

  test('clicking Features navigation link smoothly scrolls to Features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features link
    await page.getByRole('menuitem', { name: 'Features' }).click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000);

    // Get the Features section position
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify scroll happened (page scrolled down)
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify the Features section is near the top of the viewport (accounting for header)
    const boundingBox = await featuresSection.boundingBox();
    expect(boundingBox).not.toBeNull();
    if (boundingBox) {
      // The section should be near the top (within header height + some margin for section padding)
      expect(boundingBox.y).toBeLessThan(250);
    }
  });

  test('navigation header remains fixed at top when scrolling', async ({ page }) => {
    // Get initial header position
    const header = page.locator('header');
    const initialBoundingBox = await header.boundingBox();
    expect(initialBoundingBox).not.toBeNull();
    const initialTop = initialBoundingBox!.y;

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(500);

    // Get header position after scroll
    const afterScrollBoundingBox = await header.boundingBox();
    expect(afterScrollBoundingBox).not.toBeNull();
    const afterScrollTop = afterScrollBoundingBox!.y;

    // Header should remain at the same position (fixed to top)
    expect(afterScrollTop).toBe(initialTop);
    expect(afterScrollTop).toBe(0);
  });

  test('smooth scroll works for all navigation links', async ({ page }) => {
    const links = [
      { name: 'Features', sectionId: '#features' },
      { name: 'How It Works', sectionId: '#how-it-works' },
      { name: 'Status', sectionId: '#status' },
      { name: 'Quick Start', sectionId: '#quick-start' },
      { name: 'Resources', sectionId: '#resources' },
    ];

    for (const link of links) {
      // Scroll to top first
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(300);

      // Click the navigation link
      await page.getByRole('menuitem', { name: link.name }).click();

      // Wait for smooth scroll animation
      await page.waitForTimeout(1000);

      // Verify the section is visible and near the top
      const section = page.locator(link.sectionId);
      await expect(section).toBeVisible();

      const boundingBox = await section.boundingBox();
      if (boundingBox) {
        // Section should be near the top of viewport (within header + some margin for section padding)
        expect(boundingBox.y).toBeLessThan(250);
      }
    }
  });

  test('header has correct z-index for overlay behavior', async ({ page }) => {
    const header = page.locator('header');

    // Check that header has z-50 class for proper layering
    await expect(header).toHaveClass(/z-50/);
  });

  test('header has backdrop blur effect', async ({ page }) => {
    const header = page.locator('header');

    // Check that header has backdrop-blur class
    await expect(header).toHaveClass(/backdrop-blur/);
  });
});
