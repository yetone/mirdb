/**
 * Hero Section E2E Tests
 * Owner: Scenario 3 - Hero Section Display
 *
 * Test cases for REQ-2: Hero section above-the-fold requirement
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('hero section is fully above-the-fold on 1366x768 screen', async ({ page }) => {
    // Set viewport to typical laptop screen size
    await page.setViewportSize({ width: 1366, height: 768 });

    // Wait for hero section to be visible
    const heroSection = page.getByTestId('hero');
    await expect(heroSection).toBeVisible();

    // Get the bounding box of the hero section
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    if (boundingBox) {
      // Verify the entire hero section is visible above the fold
      // The bottom of the hero section should be less than or equal to viewport height
      const heroBottom = boundingBox.y + boundingBox.height;
      expect(heroBottom).toBeLessThanOrEqual(768);
    }

    // Verify headline is visible
    const headline = heroSection.getByRole('heading', { level: 1 });
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Persistent Key-Value Store');

    // Verify CTA buttons are visible within hero section
    const getStartedButton = heroSection.getByRole('link', { name: /get started/i });
    await expect(getStartedButton).toBeVisible();

    const githubButton = heroSection.getByRole('link', { name: /github/i });
    await expect(githubButton).toBeVisible();
  });

  test('displays headline correctly', async ({ page }) => {
    const heroSection = page.getByTestId('hero');
    const headline = heroSection.getByRole('heading', { level: 1 });
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText('Persistent Key-Value Store');
  });

  test('displays subtitle with Memcached Protocol', async ({ page }) => {
    const heroSection = page.getByTestId('hero');
    const subtitle = heroSection.getByText(/with Memcached Protocol/i);
    await expect(subtitle).toBeVisible();
  });

  test('Get Started button links to documentation', async ({ page }) => {
    const heroSection = page.getByTestId('hero');
    const getStartedButton = heroSection.getByRole('link', { name: /get started/i });
    await expect(getStartedButton).toHaveAttribute('href', '#documentation');
  });

  test('GitHub button links to repository with proper security attributes', async ({ page }) => {
    const heroSection = page.getByTestId('hero');
    const githubButton = heroSection.getByRole('link', { name: /github/i });
    await expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubButton).toHaveAttribute('target', '_blank');
    await expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
