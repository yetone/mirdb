/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Tests:
 * - Hero section visibility and content
 * - Logo display and animation
 * - CTA button functionality
 * - Responsive behavior
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('hero section is visible with full viewport height and correct content', async ({ page }) => {
    // Test Case 1: Hero section visibility and content
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check full viewport height (min-height: 100vh)
    const viewportHeight = page.viewportSize()?.height || 720;
    const heroBox = await hero.boundingBox();
    expect(heroBox?.height).toBeGreaterThanOrEqual(viewportHeight * 0.9);

    // Check h1 contains 'MirDB'
    const title = page.locator('#hero h1');
    await expect(title).toHaveText('MirDB');

    // Check tagline
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toHaveText('A Persistent Key-Value Store with Memcached Protocol');

    // Check animated logo is present
    const logo = page.locator('.hero__logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('src', /logo\.gif/);

    // Check two CTA buttons exist
    const ctaButtons = page.locator('.hero__cta');
    await expect(ctaButtons).toHaveCount(2);
  });

  test('primary CTA button opens GitHub in new tab with security attributes', async ({ page, context }) => {
    // Test Case 2: Primary CTA functionality
    const primaryCta = page.locator('.hero__cta-primary');

    // Verify link attributes
    await expect(primaryCta).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(primaryCta).toHaveAttribute('target', '_blank');
    await expect(primaryCta).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify the button has GitHub text
    await expect(primaryCta).toContainText('GitHub');
  });

  test('secondary CTA button links to usage section or scrolls smoothly', async ({ page }) => {
    // Test Case 3: Secondary CTA functionality
    const secondaryCta = page.locator('.hero__cta-secondary');

    // Verify it links to #usage section
    await expect(secondaryCta).toHaveAttribute('href', '#usage');

    // Click and verify scroll behavior (or at least URL hash change)
    await secondaryCta.click();

    // Wait for potential scroll animation
    await page.waitForTimeout(500);

    // Verify URL contains #usage hash
    expect(page.url()).toContain('#usage');
  });

  test('logo image has proper accessibility attributes', async ({ page }) => {
    // Test Case 4: Logo accessibility
    const logo = page.locator('.hero__logo');

    // Check descriptive alt text
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText?.length).toBeGreaterThan(10);
    expect(altText?.toLowerCase()).toContain('mirdb');

    // Check lazy loading is disabled (above fold)
    const loadingAttr = await logo.getAttribute('loading');
    expect(loadingAttr).toBe('eager');

    // Verify it's a GIF
    const src = await logo.getAttribute('src');
    expect(src).toContain('.gif');
  });

  test('hero section is responsive on mobile viewport', async ({ page }) => {
    // Test Case 5: Mobile responsiveness
    await page.setViewportSize({ width: 375, height: 667 });

    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check logo is visible and scaled
    const logo = page.locator('.hero__logo');
    await expect(logo).toBeVisible();
    const logoBox = await logo.boundingBox();
    expect(logoBox?.width).toBeLessThanOrEqual(120); // Should scale down

    // Check title is visible
    const title = page.locator('.hero__title');
    await expect(title).toBeVisible();

    // Check CTA buttons
    const ctaGroup = page.locator('.hero__cta-group');
    await expect(ctaGroup).toBeVisible();

    // Get CTA buttons positions to verify vertical stacking
    const ctaButtons = page.locator('.hero__cta');
    const count = await ctaButtons.count();
    expect(count).toBe(2);

    // Verify buttons are stacked (second button should be below first)
    const firstButton = await ctaButtons.nth(0).boundingBox();
    const secondButton = await ctaButtons.nth(1).boundingBox();

    if (firstButton && secondButton) {
      expect(secondButton.y).toBeGreaterThan(firstButton.y);
    }
  });
});
