// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section and Product Introduction
 *
 * Test cases:
 * - Tagline displays correctly
 * - Product description is visible
 * - CTA buttons are clickable
 * - CTA buttons navigate to correct sections
 */

test.describe('Hero Section and Product Introduction', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section contains tagline with MirDB and Persistent Key-Value Store keywords', async ({ page }) => {
    // Locate the hero section
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check for tagline
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    // Verify tagline contains required keywords
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('MirDB');
    expect(taglineText?.toLowerCase()).toContain('persistent');
    expect(taglineText?.toLowerCase()).toContain('key-value store');
  });

  test('TC2: Hero section description mentions memcached compatibility and persistence', async ({ page }) => {
    // Locate the hero description
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();

    // Verify description content
    const descriptionText = await description.textContent();
    expect(descriptionText?.toLowerCase()).toContain('memcached');

    // Check for persistence/durability mentions
    const hasPersistence = descriptionText?.toLowerCase().includes('persistence') ||
                          descriptionText?.toLowerCase().includes('durability') ||
                          descriptionText?.toLowerCase().includes('disk');
    expect(hasPersistence).toBeTruthy();
  });

  test('TC3: Get Started CTA button scrolls to Quick Start section', async ({ page }) => {
    // Find and click the Get Started button
    const getStartedBtn = page.locator('.hero__cta .btn--primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Click the button
    await getStartedBtn.click();

    // Wait for scroll and verify the quickstart section is in view
    await page.waitForTimeout(500); // Allow smooth scroll to complete

    // Check that the quickstart section is now visible in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('TC4: View Code CTA button scrolls to code example section', async ({ page }) => {
    // Find and click the View Code button
    const viewCodeBtn = page.locator('.hero__cta .btn--secondary');
    await expect(viewCodeBtn).toBeVisible();
    await expect(viewCodeBtn).toHaveText('View Code');

    // Click the button
    await viewCodeBtn.click();

    // Wait for scroll and verify the code-example section is in view
    await page.waitForTimeout(500); // Allow smooth scroll to complete

    // Check that the code-example section is now visible in viewport
    const codeExampleSection = page.locator('#code-example');
    await expect(codeExampleSection).toBeInViewport();
  });

  test('CTA buttons are keyboard accessible', async ({ page }) => {
    // Tab to the Get Started button and verify focus
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab'); // Skip nav logo

    const getStartedBtn = page.locator('.hero__cta .btn--primary');
    await expect(getStartedBtn).toBeFocused();

    // Tab to View Code button
    await page.keyboard.press('Tab');
    const viewCodeBtn = page.locator('.hero__cta .btn--secondary');
    await expect(viewCodeBtn).toBeFocused();
  });

  test('Hero section has proper ARIA attributes', async ({ page }) => {
    // Check that hero section has aria-labelledby
    const heroSection = page.locator('.hero');
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-tagline');

    // Check that the tagline has the correct id
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toHaveAttribute('id', 'hero-tagline');

    // Check that CTA buttons have role="button"
    const buttons = page.locator('.hero__cta .btn');
    const count = await buttons.count();
    expect(count).toBe(2);

    for (let i = 0; i < count; i++) {
      await expect(buttons.nth(i)).toHaveAttribute('role', 'button');
    }
  });
});
