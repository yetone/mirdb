/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Hero section visibility on load
 * - Logo presence and loading
 * - Tagline content verification
 * - Load time under 3 seconds
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section displays MirDB logo prominently', async ({ page }) => {
    // Wait for hero section to be visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check for logo presence
    const logo = page.locator('.hero__logo');
    await expect(logo).toBeVisible();

    // Verify logo has correct alt text
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Verify logo source contains logo.gif
    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');
  });

  test('TC2: Tagline mentions Persistent Key-Value Store and Memcached Protocol', async ({ page }) => {
    // Wait for tagline to be visible
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    // Get the text content
    const taglineText = await tagline.textContent();

    // Verify key phrases are present
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached Protocol');
  });

  test('TC3: Hero content renders within 3000ms', async ({ page, baseURL }) => {
    // Start timing
    const startTime = Date.now();

    // Navigate to page and wait for hero content to be visible
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for all hero elements to be visible
    await expect(page.locator('.hero__logo')).toBeVisible({ timeout: 3000 });
    await expect(page.locator('.hero__title')).toBeVisible({ timeout: 3000 });
    await expect(page.locator('.hero__tagline')).toBeVisible({ timeout: 3000 });

    // Calculate load time
    const loadTime = Date.now() - startTime;

    // Verify load time is under 3000ms
    expect(loadTime).toBeLessThan(3000);
  });

  test('Hero section has h1 heading with MirDB title', async ({ page }) => {
    const title = page.locator('.hero__title');
    await expect(title).toBeVisible();

    // Verify it's an h1 element
    const tagName = await title.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');

    // Verify content
    await expect(title).toHaveText('MirDB');
  });

  test('Hero section has proper ARIA attributes', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Check aria-labelledby attribute
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-heading');

    // Verify the referenced heading exists
    const heading = page.locator('#hero-heading');
    await expect(heading).toBeVisible();
  });

  test('Hero CTA buttons are visible and accessible', async ({ page }) => {
    const ctaContainer = page.locator('.hero__cta');
    await expect(ctaContainer).toBeVisible();

    // Check primary button (GitHub)
    const primaryBtn = page.locator('.hero__cta .btn-primary');
    await expect(primaryBtn).toBeVisible();
    await expect(primaryBtn).toContainText('GitHub');

    // Check secondary button (Learn More)
    const secondaryBtn = page.locator('.hero__cta .btn-secondary');
    await expect(secondaryBtn).toBeVisible();
    await expect(secondaryBtn).toContainText('Learn More');
  });

  test('GitHub link has correct URL and security attributes', async ({ page }) => {
    const githubLink = page.locator('.hero__cta .btn-primary');

    // Verify href
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify security attributes
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    await expect(githubLink).toHaveAttribute('target', '_blank');
  });

  test('Hero description provides value proposition', async ({ page }) => {
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();

    const descText = await description.textContent();

    // Verify key technical terms are mentioned
    expect(descText.toLowerCase()).toMatch(/rust|lsm-tree|durability|performance|memcached/i);
  });
});
