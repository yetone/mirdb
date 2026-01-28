/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 * Also covers: Scenario 15 - Usage GIF Display
 *
 * Tests for:
 * - Product name (MirDB) visibility
 * - Tagline text content
 * - Animated logo display
 * - CTA buttons (Get Started, Read Documentation)
 * - Usage GIF display and loading
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section is visible with product name MirDB as h1 heading', async ({ page }) => {
    // Verify hero section exists and is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify h1 contains MirDB
    const heading = page.locator('h1#hero-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('MirDB');
  });

  test('TC2: Tagline text contains Persistent Key-Value Store and Memcached', async ({ page }) => {
    // Verify tagline is visible
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify tagline content
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached');
  });

  test('TC4: Primary CTA button navigates to GitHub or documentation section', async ({ page }) => {
    // Verify primary CTA button exists
    const primaryCTA = page.locator('#cta-github');
    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toHaveText('View on GitHub');

    // Verify it has proper href (GitHub link)
    const href = await primaryCTA.getAttribute('href');
    expect(href).toContain('github.com');
  });

  test('TC5: Secondary CTA for documentation is visible and functional', async ({ page }) => {
    // Verify secondary CTA button exists
    const secondaryCTA = page.locator('#cta-docs');
    await expect(secondaryCTA).toBeVisible();
    await expect(secondaryCTA).toHaveText('Read Documentation');

    // Verify it links to the documentation section
    const href = await secondaryCTA.getAttribute('href');
    expect(href).toBe('#quick-start');

    // Click and verify navigation
    await secondaryCTA.click();

    // Verify the quick-start section is now visible in viewport
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('Hero section has proper accessibility attributes', async ({ page }) => {
    // Verify hero section has aria-labelledby
    const heroSection = page.locator('#hero');
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-title');

    // Verify logo has alt text
    const logo = page.locator('.hero-logo');
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toContain('logo');
  });

  test('CTA buttons are clickable and have proper styles', async ({ page }) => {
    // Verify primary CTA has button styles
    const primaryCTA = page.locator('#cta-github');
    await expect(primaryCTA).toHaveClass(/btn-primary/);

    // Verify secondary CTA has button styles
    const secondaryCTA = page.locator('#cta-docs');
    await expect(secondaryCTA).toHaveClass(/btn-secondary/);

    // Verify buttons are focusable (accessibility)
    await primaryCTA.focus();
    await expect(primaryCTA).toBeFocused();
  });
});

/**
 * Scenario 15: Usage GIF Display E2E Tests
 * Tests for usage demonstration GIF loading and visibility
 */
test.describe('Usage GIF Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC3: Check usage GIF loads without error', async ({ page }) => {
    // Wait for the usage GIF to be present in DOM
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeAttached();

    // Check that the image loads successfully (no 404)
    const gifSrc = await usageGif.getAttribute('src');
    expect(gifSrc).toContain('usage.gif');

    // Scroll to the GIF to trigger lazy loading
    await usageGif.scrollIntoViewIfNeeded();

    // Verify the image loads by checking natural dimensions
    const loadState = await usageGif.evaluate((img) => {
      return new Promise((resolve) => {
        if (img.complete && img.naturalWidth > 0) {
          resolve({
            complete: true,
            naturalWidth: img.naturalWidth,
            naturalHeight: img.naturalHeight
          });
        } else {
          img.onload = () => resolve({
            complete: true,
            naturalWidth: img.naturalWidth,
            naturalHeight: img.naturalHeight
          });
          img.onerror = () => resolve({ complete: false, error: true });
        }
      });
    });

    expect(loadState.complete).toBe(true);
    expect(loadState.naturalWidth).toBeGreaterThan(0);
    expect(loadState.naturalHeight).toBeGreaterThan(0);
  });

  test('TC4: Verify GIF is visible in viewport', async ({ page }) => {
    // Navigate to the usage demo section
    const usageDemoSection = page.locator('#usage-demo');
    await usageDemoSection.scrollIntoViewIfNeeded();

    // Verify the section is in viewport
    await expect(usageDemoSection).toBeInViewport();

    // Verify the GIF within the section is visible
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();
    await expect(usageGif).toBeInViewport();
  });

  test('Usage demonstration section has proper heading', async ({ page }) => {
    const usageDemoSection = page.locator('#usage-demo');
    await expect(usageDemoSection).toBeVisible();

    const heading = usageDemoSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('See It In Action');
  });

  test('Usage GIF has appropriate alt text for accessibility', async ({ page }) => {
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeAttached();

    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(10);
    expect(altText.toLowerCase()).toMatch(/usage|demonstration|demo|mirdb/);
  });

  test('Usage GIF container has proper styling', async ({ page }) => {
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();

    // Check that the GIF has the usage-gif class for styling
    await expect(usageGif).toHaveClass(/usage-gif/);
  });

  test('Usage demo section is accessible via scrolling', async ({ page }) => {
    // Start at top of page
    await page.evaluate(() => window.scrollTo(0, 0));

    // Scroll down to find the usage demo section
    const usageDemoSection = page.locator('#usage-demo');

    // The section should become visible when scrolling through the page
    await usageDemoSection.scrollIntoViewIfNeeded();
    await expect(usageDemoSection).toBeInViewport();

    // The GIF should be visible within the section
    const usageGif = usageDemoSection.locator('.usage-gif');
    await expect(usageGif).toBeVisible();
  });
});
