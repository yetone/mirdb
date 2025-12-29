import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Hero Section Display
 *
 * These tests verify that the hero section displays all required elements:
 * - Product name (MirDB)
 * - Tagline with "Persistent Key-Value Store" and "Memcached Protocol"
 * - Primary CTA button ("Get Started")
 * - Secondary CTA button ("View on GitHub" or "GitHub")
 */

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name MirDB is present and visible in hero section', async ({ page }) => {
    // Check that the hero section contains the product name "MirDB"
    const heroSection = page.locator('.hero, #hero, [data-testid="hero"], header, section').first();

    // Look for MirDB text in a heading or prominent element
    const productName = page.locator('h1, .hero-title, .product-name, [data-testid="product-name"]').filter({ hasText: 'MirDB' });

    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');
  });

  test('TC2: Tagline displays Persistent Key-Value Store and Memcached Protocol', async ({ page }) => {
    // Check for tagline containing both key phrases
    const pageContent = await page.content();

    // Verify "Persistent Key-Value Store" is present
    const hasPersistentKVStore = pageContent.includes('Persistent Key-Value Store') ||
                                  pageContent.toLowerCase().includes('persistent key-value store');
    expect(hasPersistentKVStore).toBeTruthy();

    // Verify "Memcached Protocol" is present
    const hasMemcachedProtocol = pageContent.includes('Memcached Protocol') ||
                                  pageContent.toLowerCase().includes('memcached protocol');
    expect(hasMemcachedProtocol).toBeTruthy();

    // Also verify these are visible on the page
    const taglineElement = page.locator('.tagline, .subtitle, .hero-tagline, h2, p').filter({
      hasText: /Persistent|Memcached/i
    }).first();
    await expect(taglineElement).toBeVisible();
  });

  test('TC3: Get Started primary CTA button is visible with appropriate styling', async ({ page }) => {
    // Find the "Get Started" button
    const getStartedButton = page.locator('a, button').filter({ hasText: /Get Started/i });

    // Verify it exists and is visible
    await expect(getStartedButton).toBeVisible();

    // Verify it has button-like styling (either button element or styled as primary)
    const tagName = await getStartedButton.evaluate(el => el.tagName.toLowerCase());
    const classList = await getStartedButton.getAttribute('class');

    // Should be either a button or a link styled as button
    const isButton = tagName === 'button';
    const isStyledLink = tagName === 'a' && (classList?.includes('btn') || classList?.includes('button') || classList?.includes('cta'));

    expect(isButton || isStyledLink).toBeTruthy();
  });

  test('TC4: GitHub secondary CTA button is visible with link to repository', async ({ page }) => {
    // Find the GitHub button/link in the hero section (first one on the page)
    const heroSection = page.locator('.hero, header').first();
    const githubButton = heroSection.locator('a').filter({ hasText: /GitHub|View on GitHub/i }).first();

    // Verify it exists and is visible
    await expect(githubButton).toBeVisible();

    // Verify it has an href pointing to GitHub
    const href = await githubButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href?.toLowerCase()).toContain('github');
  });
});
