/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Tests for:
 * - Hero section visibility above the fold
 * - Headline and tagline text content
 * - Primary CTA (Get Started) smooth scroll
 * - Secondary CTA (View on GitHub) external link
 * - Logo presence and accessibility
 * - Focus states for keyboard navigation
 * - Mobile viewport responsiveness
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section is visible above the fold with headline', async ({ page }) => {
    // Check hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check headline text
    const headline = page.locator('.hero__headline');
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText('A Persistent Key-Value Store with Memcached Protocol');

    // Verify it's above the fold (visible without scrolling)
    const viewportSize = page.viewportSize();
    const heroBox = await heroSection.boundingBox();
    expect(heroBox.y).toBeLessThan(viewportSize.height);
  });

  test('TC2: Tagline is visible with correct text', async ({ page }) => {
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toHaveText(
      'Drop-in memcached replacement with durable storage, powered by Rust and LSM tree architecture'
    );
  });

  test('TC3: Primary CTA Get Started button scrolls to getting-started section', async ({ page }) => {
    const primaryCta = page.locator('.hero__cta-primary');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toHaveText('Get Started');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the primary CTA
    await primaryCta.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000);

    // Check that the page scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify getting-started section is now in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('TC4: Secondary CTA View on GitHub opens in new tab with correct URL', async ({ page, context }) => {
    const secondaryCta = page.locator('.hero__cta-secondary');
    await expect(secondaryCta).toBeVisible();
    await expect(secondaryCta).toContainText('View on GitHub');

    // Check href attribute
    await expect(secondaryCta).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Check target and rel attributes for security
    await expect(secondaryCta).toHaveAttribute('target', '_blank');
    await expect(secondaryCta).toHaveAttribute('rel', /noopener/);

    // Test that clicking opens a new tab (via popup promise)
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      secondaryCta.click()
    ]);

    // Verify the new page URL
    expect(newPage.url()).toContain('github.com/yetone/mirdb');
  });

  test('TC5: Logo is present with appropriate alt text', async ({ page }) => {
    const logo = page.locator('.hero__logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Verify image source is set
    await expect(logo).toHaveAttribute('src', /logo\.svg/);
  });

  test('TC6: CTA buttons have visible focus states for keyboard navigation', async ({ page }) => {
    const primaryCta = page.locator('.hero__cta-primary');
    const secondaryCta = page.locator('.hero__cta-secondary');

    // Focus primary CTA using keyboard
    await primaryCta.focus();

    // Check for visible focus indicator (outline)
    const primaryOutline = await primaryCta.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth
      };
    });

    // Verify focus is visible (outline exists and is not 'none')
    expect(primaryOutline.outlineStyle).not.toBe('none');

    // Focus secondary CTA
    await secondaryCta.focus();

    const secondaryOutline = await secondaryCta.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth
      };
    });

    expect(secondaryOutline.outlineStyle).not.toBe('none');
  });

  test('TC7: Hero section is readable and CTAs are tap-friendly on 320px viewport', async ({ page }) => {
    // Set viewport to 320px width (smallest common mobile)
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Check hero content is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const headline = page.locator('.hero__headline');
    await expect(headline).toBeVisible();

    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    // Check CTAs are visible and have adequate touch target size
    const primaryCta = page.locator('.hero__cta-primary');
    const secondaryCta = page.locator('.hero__cta-secondary');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Get bounding boxes to verify touch target sizes
    const primaryBox = await primaryCta.boundingBox();
    const secondaryBox = await secondaryCta.boundingBox();

    // Minimum touch target should be 44x44px (WCAG 2.1 Level AAA recommends 44px)
    expect(primaryBox.height).toBeGreaterThanOrEqual(44);
    expect(secondaryBox.height).toBeGreaterThanOrEqual(44);

    // Verify text is not cut off (buttons should fit within viewport)
    expect(primaryBox.x).toBeGreaterThanOrEqual(0);
    expect(primaryBox.x + primaryBox.width).toBeLessThanOrEqual(320);
    expect(secondaryBox.x).toBeGreaterThanOrEqual(0);
    expect(secondaryBox.x + secondaryBox.width).toBeLessThanOrEqual(320);
  });
});
