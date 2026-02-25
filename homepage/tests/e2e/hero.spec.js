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

const { test, expect } = require('@playwright/test');

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('displays tagline with MirDB and Persistent Key-Value Store keywords', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Verify tagline contains required keywords
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('MirDB');
    expect(taglineText).toContain('Persistent Key-Value Store');
  });

  test('displays product description with memcached and persistence keywords', async ({ page }) => {
    // Verify description is visible
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();

    const descriptionText = await description.textContent();
    const descriptionLower = descriptionText.toLowerCase();

    // Check for memcached compatibility mention
    expect(descriptionLower).toContain('memcached');

    // Check for persistence/durability mention
    const hasPersistence = descriptionLower.includes('persistence') || descriptionLower.includes('durability');
    expect(hasPersistence).toBe(true);
  });

  test('Get Started CTA button scrolls to quickstart section', async ({ page }) => {
    // Find and verify the Get Started button
    const getStartedBtn = page.locator('.hero__cta .btn--primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify href points to quickstart
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click and verify navigation
    await getStartedBtn.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the quickstart section is now in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('View Code CTA button scrolls to code example section', async ({ page }) => {
    // Find and verify the View Code button
    const viewCodeBtn = page.locator('.hero__cta .btn--secondary');
    await expect(viewCodeBtn).toBeVisible();
    await expect(viewCodeBtn).toHaveText('View Code');

    // Verify href points to code-example
    const href = await viewCodeBtn.getAttribute('href');
    expect(href).toBe('#code-example');

    // Click and verify navigation
    await viewCodeBtn.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the code-example section is now in view
    const codeExampleSection = page.locator('#code-example');
    await expect(codeExampleSection).toBeInViewport();
  });

  test('hero section has proper accessibility attributes', async ({ page }) => {
    // Check section has aria-labelledby
    const heroSection = page.locator('.hero');
    const ariaLabelledBy = await heroSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('hero-tagline');

    // Check heading has matching id
    const heading = page.locator('#hero-tagline');
    await expect(heading).toBeVisible();

    // Check CTA buttons have role="button"
    const buttons = page.locator('.hero__cta .btn');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBe(2);

    for (let i = 0; i < buttonCount; i++) {
      const role = await buttons.nth(i).getAttribute('role');
      expect(role).toBe('button');
    }
  });

  test('hero section is prominently visible above the fold', async ({ page }) => {
    // Hero should be visible immediately on page load without scrolling
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();
    await expect(heroSection).toBeInViewport();

    // The tagline should also be visible
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeInViewport();

    // CTA buttons should be visible
    const ctaContainer = page.locator('.hero__cta');
    await expect(ctaContainer).toBeInViewport();
  });
});
