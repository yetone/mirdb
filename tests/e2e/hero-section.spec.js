// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Hero section contains 'MirDB' text element
  test('TC1: hero section contains MirDB text element', async ({ page }) => {
    // Check that the hero section exists (header.hero or section.hero)
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check that MirDB is displayed in the h1 title
    const heroTitle = heroSection.locator('h1');
    await expect(heroTitle).toHaveText('MirDB');
    await expect(heroTitle).toBeVisible();
  });

  // Test Case 2: Tagline contains 'persistent key-value store' and 'memcached' keywords
  test('TC2: tagline contains required keywords', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check for tagline with required keywords
    const tagline = heroSection.locator('.tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText?.toLowerCase()).toContain('persistent key-value store');
    expect(taglineText?.toLowerCase()).toContain('memcached');
  });

  // Test Case 3: 'Get Started' button scrolls to quick start section
  test('TC3: Get Started button scrolls to quick start section', async ({ page }) => {
    const heroSection = page.locator('.hero');

    // Find and click the Get Started button
    const getStartedBtn = heroSection.locator('a.btn-primary, a:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();

    // Check href points to quickstart section
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toMatch(/#quick[-]?start/i);

    // Click the button
    await getStartedBtn.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the quick start section is now in view
    const quickStartSection = page.locator('#quickstart, #quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  // Test Case 4: 'View on GitHub' button navigates to GitHub repository URL
  test('TC4: View on GitHub button has correct href', async ({ page }) => {
    const heroSection = page.locator('.hero');

    const githubBtn = heroSection.locator('a.btn-secondary, a:has-text("GitHub")');
    await expect(githubBtn).toBeVisible();

    // Check that it links to the GitHub repository
    const href = await githubBtn.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Check that it opens in a new tab
    await expect(githubBtn).toHaveAttribute('target', '_blank');

    // Check for security attribute
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  // Test Case 5: Hero section has semantic HTML elements (h1, p, a/button)
  test('TC5: hero section has semantic HTML structure', async ({ page }) => {
    // Check that the hero section exists
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check for h1 element (product name)
    const h1Element = heroSection.locator('h1');
    await expect(h1Element).toBeVisible();
    await expect(h1Element).toHaveText('MirDB');

    // Check for p elements (tagline and description)
    const paragraphs = heroSection.locator('p');
    const paragraphCount = await paragraphs.count();
    expect(paragraphCount).toBeGreaterThanOrEqual(2);

    // Check for CTA anchor elements
    const ctaContainer = heroSection.locator('.cta-buttons');
    await expect(ctaContainer).toBeVisible();

    const ctaLinks = ctaContainer.locator('a');
    const ctaCount = await ctaLinks.count();
    expect(ctaCount).toBe(2);

    // Verify both CTAs are anchor elements with correct text
    const getStartedBtn = ctaContainer.locator('a', { hasText: 'Get Started' });
    const githubBtn = ctaContainer.locator('a', { hasText: 'GitHub' });
    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();
  });
});
