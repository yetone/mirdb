/**
 * E2E tests for Hero section.
 * Owner: Scenario 1 - Hero Section Display
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 3: Click 'Get Started' button scrolls to Quick Start section
  test('TC3: Get Started button scrolls to Quick Start section', async ({
    page,
  }) => {
    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Click the Get Started button
    const getStartedButton = page.getByRole('link', { name: 'Get Started' });
    await expect(getStartedButton).toBeVisible();
    await getStartedButton.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000);

    // Verify the Quick Start section is now in viewport
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  // Test Case 4: Click 'View on GitHub' button opens GitHub in new tab
  test('TC4: View on GitHub button opens GitHub repository in new tab', async ({
    page,
    context,
  }) => {
    // Get the GitHub button
    const githubButton = page.getByRole('link', { name: 'View on GitHub' });
    await expect(githubButton).toBeVisible();

    // Verify button attributes for external link
    await expect(githubButton).toHaveAttribute('target', '_blank');
    await expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
    await expect(githubButton).toHaveAttribute(
      'href',
      'https://github.com/yetone/mirdb'
    );

    // Test that clicking opens a new page
    const pagePromise = context.waitForEvent('page');
    await githubButton.click();
    const newPage = await pagePromise;

    // Verify the new page URL
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    // Clean up
    await newPage.close();
  });

  // Additional E2E tests for hero section
  test('Hero section displays logo with correct alt text', async ({ page }) => {
    const logo = page.locator('#hero img[alt="MirDB"]');
    await expect(logo).toBeVisible();
  });

  test('Hero section displays project name MirDB', async ({ page }) => {
    const heading = page.locator('#hero h1');
    await expect(heading).toHaveText('MirDB');
  });

  test('Hero section displays tagline', async ({ page }) => {
    const tagline = page.getByText(
      'Persistent Key-Value Store with Memcached Compatibility'
    );
    await expect(tagline).toBeVisible();
  });

  test('Both CTA buttons are prominently displayed', async ({ page }) => {
    const getStartedButton = page.getByRole('link', { name: 'Get Started' });
    const githubButton = page.getByRole('link', { name: 'View on GitHub' });

    await expect(getStartedButton).toBeVisible();
    await expect(githubButton).toBeVisible();

    // Verify buttons are in the hero section
    const heroSection = page.locator('#hero');
    await expect(heroSection.getByRole('link', { name: 'Get Started' })).toBeVisible();
    await expect(heroSection.getByRole('link', { name: 'View on GitHub' })).toBeVisible();
  });

  test('Hero section is above the fold', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Get the hero section bounding box
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    // Verify hero starts at or near the top of the viewport
    expect(boundingBox!.y).toBeLessThanOrEqual(100);
  });

  // Accessibility tests
  test('Hero section has proper heading structure', async ({ page }) => {
    const h1 = page.locator('#hero h1');
    await expect(h1).toHaveAttribute('id', 'hero-heading');
  });

  test('Hero section has aria-labelledby attribute', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-heading');
  });
});
