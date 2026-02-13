/**
 * Unit Tests for Hero Section
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Tests HTML structure, semantic elements, and content requirements
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 2: Contains h1 with MirDB project name and semantic heading structure', async ({ page }) => {
    // Check for h1 with MirDB
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify semantic heading structure - h1 should be unique and in hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const heroH1 = heroSection.locator('h1');
    await expect(heroH1).toHaveCount(1);

    // Verify h1 has proper id for accessibility
    await expect(heroH1).toHaveAttribute('id', 'hero-title');

    // Verify aria-labelledby is set correctly
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-title');
  });

  test('Test Case 3: Tagline mentions Persistent Key-Value Store and Memcached protocol', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Check for tagline content
    const tagline = heroSection.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();

    // Verify tagline contains required keywords
    expect(taglineText.toLowerCase()).toContain('persistent');
    expect(taglineText.toLowerCase()).toContain('key-value');
    expect(taglineText.toLowerCase()).toContain('memcached');
  });

  test('Test Case 6: MirDB logo is displayed and has proper alt text', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Check for logo image
    const logo = heroSection.locator('.hero__logo-image');
    await expect(logo).toBeVisible();

    // Verify logo source contains a valid logo file (GIF or SVG for performance optimization)
    const src = await logo.getAttribute('src');
    expect(src).toMatch(/logo.*\.(gif|svg)$/);

    // Verify proper alt text
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);
    expect(altText.toLowerCase()).toContain('mirdb');
  });

  test('Hero section has proper semantic structure', async ({ page }) => {
    // Verify hero is a section element
    const heroSection = page.locator('section#hero');
    await expect(heroSection).toBeVisible();

    // Verify it has hero class
    await expect(heroSection).toHaveClass(/hero/);

    // Verify CTA buttons are present
    const ctaGroup = heroSection.locator('.hero__cta-group');
    await expect(ctaGroup).toBeVisible();

    // Verify both buttons exist
    const primaryCta = heroSection.locator('.hero__cta--primary');
    const secondaryCta = heroSection.locator('.hero__cta--secondary');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();
  });

  test('Get Started button has correct href', async ({ page }) => {
    const getStartedBtn = page.locator('.hero__cta--primary');
    await expect(getStartedBtn).toContainText('Get Started');
    await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');
  });

  test('View on GitHub button has correct href and opens in new tab', async ({ page }) => {
    const githubBtn = page.locator('.hero__cta--secondary');
    await expect(githubBtn).toContainText('View on GitHub');
    await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubBtn).toHaveAttribute('target', '_blank');
    await expect(githubBtn).toHaveAttribute('rel', /noopener/);
  });
});
