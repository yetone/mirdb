// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Hero Section Display and Content E2E Tests
 * Tests REQ-1 and REQ-4 from PRD
 */

test.describe('Hero Section Display and Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:8080');
  });

  // Test Case 1: Product name 'MirDB' is visible in hero section with font size >= 32px
  test('TC1: Product name MirDB is visible with proper font size', async ({ page }) => {
    // Find the hero section
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find the product name
    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Verify font size >= 32px
    const fontSize = await productName.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(fontSize).toBeGreaterThanOrEqual(32);
  });

  // Test Case 2: Tagline contains 'Persistent Key-Value Store with Memcached Protocol'
  test('TC2: Tagline contains correct text', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find the tagline element
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store with Memcached Protocol');
  });

  // Test Case 3: Value proposition text mentions 'Rust', 'Memcached protocol', and 'LSM tree'
  test('TC3: Value proposition mentions key technologies', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find the value proposition paragraph
    const valueProposition = heroSection.locator('.value-proposition');
    await expect(valueProposition).toBeVisible();

    const text = await valueProposition.textContent();
    expect(text).toContain('Rust');
    expect(text.toLowerCase()).toContain('memcached');
    expect(text).toContain('LSM');
  });

  // Test Case 4: Button with text 'Get Started' exists and is styled as primary action
  test('TC4: Get Started button exists and is styled as primary', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find the primary CTA button
    const getStartedBtn = heroSection.locator('.cta-primary, a.btn-primary, button.btn-primary').filter({ hasText: 'Get Started' });
    await expect(getStartedBtn).toBeVisible();

    // Verify it has primary styling (background color should be distinct)
    const bgColor = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Primary button should have a non-transparent background
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(bgColor).not.toBe('transparent');
  });

  // Test Case 5: Button/link with text 'View on GitHub' exists and links to GitHub repository
  test('TC5: View on GitHub button exists and links to GitHub', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find the secondary CTA button/link
    const githubBtn = heroSection.locator('a').filter({ hasText: 'View on GitHub' });
    await expect(githubBtn).toBeVisible();

    // Verify it links to GitHub
    const href = await githubBtn.getAttribute('href');
    expect(href).toContain('github.com');
  });
});
