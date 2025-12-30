// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Hero section contains h1 with 'MirDB', tagline mentioning
   * 'Persistent Key-Value Store' and 'Memcached Protocol'
   */
  test('TC1: hero section contains product name and tagline', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify h1 contains 'MirDB'
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify tagline mentions 'Persistent Key-Value Store' and 'Memcached Protocol'
    const tagline = heroSection.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    const taglineText = await tagline.textContent();
    expect(taglineText).toMatch(/Persistent Key-Value Store/i);
    expect(taglineText).toMatch(/Memcached Protocol/i);
  });

  /**
   * Test Case 2: Description paragraph exists with 2-3 sentences explaining MirDB
   */
  test('TC2: hero section contains description paragraph', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify description paragraph exists
    const description = heroSection.locator('.hero-description');
    await expect(description).toBeVisible();

    // Verify description has meaningful content (at least 50 characters for 2-3 sentences)
    const descText = await description.textContent();
    expect(descText.length).toBeGreaterThan(50);

    // Description should mention key MirDB concepts
    expect(descText.toLowerCase()).toMatch(/key-value|store|persistent|memcached|rust/i);
  });

  /**
   * Test Case 3: Primary CTA button exists with 'Get Started' or 'View on GitHub'
   */
  test('TC3: primary CTA button exists with correct text and link', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find primary CTA button
    const primaryCta = heroSection.locator('.btn-primary');
    await expect(primaryCta).toBeVisible();

    // Verify button text is 'Get Started' or 'View on GitHub'
    const ctaText = await primaryCta.textContent();
    expect(ctaText).toMatch(/Get Started|View on GitHub/i);

    // Verify button has a valid href
    const href = await primaryCta.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);
  });

  /**
   * Test Case 4: Secondary CTA button exists with 'Documentation'
   */
  test('TC4: secondary CTA button exists with Documentation text', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find secondary CTA button
    const secondaryCta = heroSection.locator('.btn-secondary');
    await expect(secondaryCta).toBeVisible();

    // Verify button text contains 'Documentation'
    const ctaText = await secondaryCta.textContent();
    expect(ctaText).toMatch(/Documentation/i);

    // Verify button has a valid href
    const href = await secondaryCta.getAttribute('href');
    expect(href).toBeTruthy();
  });
});
