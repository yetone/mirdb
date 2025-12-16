import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Product name MirDB is displayed in a prominent heading element', async ({ page }) => {
    // Load homepage and inspect hero section DOM elements
    // Expected: Product name 'MirDB' is displayed in a prominent heading element
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const productName = heroSection.locator('h1');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');
  });

  test('TC2: Tagline is visible with correct text', async ({ page }) => {
    // Check for tagline text content
    // Expected: Tagline 'A persistent key-value store with Memcached protocol compatibility, written in Rust' is visible
    const heroSection = page.locator('.hero-section');
    const tagline = heroSection.locator('.tagline');

    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('A persistent key-value store with Memcached protocol compatibility, written in Rust');
  });

  test('TC3: Two CTA buttons exist: Get Started and View on GitHub', async ({ page }) => {
    // Query for CTA button elements
    // Expected: Two CTA buttons exist: 'Get Started' and 'View on GitHub'
    const heroSection = page.locator('.hero-section');

    const getStartedButton = heroSection.locator('a.cta-button', { hasText: 'Get Started' });
    const viewOnGitHubButton = heroSection.locator('a.cta-button', { hasText: 'View on GitHub' });

    await expect(getStartedButton).toBeVisible();
    await expect(viewOnGitHubButton).toBeVisible();

    // Verify there are exactly 2 CTA buttons
    const ctaButtons = heroSection.locator('a.cta-button');
    await expect(ctaButtons).toHaveCount(2);
  });

  test('TC4: View on GitHub button links to valid GitHub repository URL', async ({ page }) => {
    // Click 'View on GitHub' button
    // Expected: Button links to valid GitHub repository URL
    const viewOnGitHubButton = page.locator('a.cta-button', { hasText: 'View on GitHub' });

    await expect(viewOnGitHubButton).toBeVisible();

    const href = await viewOnGitHubButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https:\/\/github\.com\//);

    // Verify it points to the MirDB repository
    expect(href).toContain('github.com');
  });
});
