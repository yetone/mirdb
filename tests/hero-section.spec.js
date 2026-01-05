const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name MirDB is displayed prominently with large, clear typography', async ({ page }) => {
    // Verify the hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify the product name "MirDB" is displayed in the hero h1
    const productName = page.locator('.hero h1');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify the typography is large (font-size >= 2.5rem = 40px)
    const fontSize = await productName.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(fontSize).toBeGreaterThanOrEqual(40);
  });

  test('TC2: Tagline is displayed in hero section', async ({ page }) => {
    // Verify the tagline element exists
    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toBeVisible();

    // Verify the tagline contains the expected text
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached Protocol');
  });

  test('TC3: Primary CTA button exists and is visible and clickable', async ({ page }) => {
    // Verify primary CTA button exists
    const primaryCTA = page.locator('#primary-cta');
    await expect(primaryCTA).toBeVisible();

    // Verify it's a link element
    const tagName = await primaryCTA.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('a');

    // Verify it has appropriate text (View on GitHub or Get Started)
    const buttonText = await primaryCTA.textContent();
    const hasValidText = buttonText.includes('GitHub') || buttonText.includes('Get Started');
    expect(hasValidText).toBe(true);

    // Verify it's clickable (has href attribute)
    const href = await primaryCTA.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
  });

  test('TC4: Secondary CTA button exists and is present', async ({ page }) => {
    // Verify secondary CTA button exists
    const secondaryCTA = page.locator('#secondary-cta');
    await expect(secondaryCTA).toBeVisible();

    // Verify it has appropriate text (Learn More or Documentation)
    const buttonText = await secondaryCTA.textContent();
    const hasValidText = buttonText.includes('Learn More') || buttonText.includes('Documentation');
    expect(hasValidText).toBe(true);

    // Verify it's styled as secondary (has btn-secondary class)
    await expect(secondaryCTA).toHaveClass(/btn-secondary/);
  });

  test('TC5: Primary CTA button navigates to GitHub repository', async ({ page }) => {
    // Get the primary CTA button
    const primaryCTA = page.locator('#primary-cta');

    // Verify it has the correct href pointing to GitHub
    const href = await primaryCTA.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab (has target="_blank")
    const target = await primaryCTA.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('Hero section is above the fold and prominently displayed', async ({ page }) => {
    // Verify hero section is positioned at the top of the page
    const heroSection = page.locator('.hero');
    const boundingBox = await heroSection.boundingBox();

    // Hero should start near the top (after header)
    expect(boundingBox.y).toBeLessThan(200);

    // Hero should have significant height
    expect(boundingBox.height).toBeGreaterThan(200);
  });

  test('CTA buttons container is visible within hero section', async ({ page }) => {
    // Verify the CTA buttons container exists within hero
    const ctaContainer = page.locator('.hero .cta-buttons');
    await expect(ctaContainer).toBeVisible();

    // Verify both buttons are inside the container
    const buttons = ctaContainer.locator('.btn');
    await expect(buttons).toHaveCount(2);
  });
});
