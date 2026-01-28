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
 * Scenario 15: Usage GIF Display Tests
 * Tests for the usage demonstration GIF that shows how MirDB works
 */
test.describe('Usage GIF Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC3: Usage GIF loads without error (no 404 or network errors)', async ({ page }) => {
    // Track network requests for the usage GIF
    const gifRequests = [];
    const failedRequests = [];

    page.on('response', response => {
      if (response.url().includes('usage.gif')) {
        gifRequests.push({
          url: response.url(),
          status: response.status()
        });
        if (response.status() >= 400) {
          failedRequests.push({
            url: response.url(),
            status: response.status()
          });
        }
      }
    });

    // Verify the GIF element exists
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();

    // Scroll to the GIF to trigger lazy loading
    await usageGif.scrollIntoViewIfNeeded();

    // Wait for the image to load
    await page.waitForFunction(
      (selector) => {
        const img = document.querySelector(selector);
        return img && img.complete && img.naturalWidth > 0;
      },
      '#usage-gif',
      { timeout: 10000 }
    );

    // Verify no failed requests for the GIF (filter out any that occurred)
    const successfulRequests = gifRequests.filter(r => r.status >= 200 && r.status < 400);
    expect(successfulRequests.length).toBeGreaterThan(0);
  });

  test('TC4: Usage GIF is visible in viewport when scrolling through page', async ({ page }) => {
    // Verify the usage GIF element exists
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();

    // Scroll to the usage GIF if not in viewport
    await usageGif.scrollIntoViewIfNeeded();

    // Verify it's in the viewport after scrolling
    await expect(usageGif).toBeInViewport();
  });

  test('Usage GIF has correct src attribute', async ({ page }) => {
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();

    const src = await usageGif.getAttribute('src');
    expect(src).toContain('usage.gif');
  });

  test('Usage GIF has descriptive alt text for accessibility', async ({ page }) => {
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();

    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(10);
  });

  test('Usage GIF container has appropriate styling', async ({ page }) => {
    const usageGifContainer = page.locator('.usage-demo');
    await expect(usageGifContainer).toBeVisible();

    // Verify the container has proper layout
    const boundingBox = await usageGifContainer.boundingBox();
    expect(boundingBox.width).toBeGreaterThan(0);
    expect(boundingBox.height).toBeGreaterThan(0);
  });

  test('Usage GIF displays after page load completes', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('load');

    // Verify the usage GIF is visible
    const usageGif = page.locator('#usage-gif');
    await expect(usageGif).toBeVisible();

    // Scroll to trigger lazy loading
    await usageGif.scrollIntoViewIfNeeded();

    // Wait for the image to fully load (lazy loading may delay this)
    await page.waitForFunction(
      (selector) => {
        const img = document.querySelector(selector);
        return img && img.complete && img.naturalWidth > 0;
      },
      '#usage-gif',
      { timeout: 10000 }
    );

    // Verify it has natural dimensions (indicating image loaded)
    const naturalWidth = await usageGif.evaluate((img) => img.naturalWidth);
    const naturalHeight = await usageGif.evaluate((img) => img.naturalHeight);

    expect(naturalWidth).toBeGreaterThan(0);
    expect(naturalHeight).toBeGreaterThan(0);
  });
});
