import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Homepage Hero Section Display
 * Scenario: Verify that the hero section displays the product tagline,
 * brief description, and primary call-to-action buttons correctly
 */

test.describe('Homepage Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Hero section displays with headline containing 'MirDB' and 'Persistent Key-Value Store'
   * Input: Load homepage URL
   * Expected: Hero section displays with headline containing 'MirDB' and 'Persistent Key-Value Store'
   */
  test('should display hero section with MirDB headline and Persistent Key-Value Store', async ({ page }) => {
    // Verify the hero section exists
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify headline contains 'MirDB'
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('MirDB');

    // Verify headline contains 'Persistent Key-Value Store'
    await expect(headline).toContainText('Persistent Key-Value Store');
  });

  /**
   * Test Case 2: Get Started button is visible and links to getting-started section or documentation
   * Input: Check for primary CTA button
   * Expected: Get Started button is visible and links to getting-started section or documentation
   */
  test('should display Get Started button with proper link', async ({ page }) => {
    // Verify Get Started button exists and is visible
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();

    // Verify button text contains 'Get Started'
    await expect(getStartedButton).toContainText('Get Started');

    // Verify the button has a valid href (links to getting-started section or documentation)
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/(#getting-started|\/docs|\/documentation|getting-started)/i);
  });

  /**
   * Test Case 3: GitHub button/link is visible and links to MirDB repository
   * Input: Check for GitHub link
   * Expected: GitHub button/link is visible and links to MirDB repository
   */
  test('should display GitHub link that points to MirDB repository', async ({ page }) => {
    // Verify GitHub button/link exists and is visible
    const githubLink = page.locator('[data-testid="cta-github"]');
    await expect(githubLink).toBeVisible();

    // Verify it contains 'GitHub' text
    await expect(githubLink).toContainText('GitHub');

    // Verify the link points to a GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
  });

  /**
   * Test Case 4: Subheadline mentions memcached compatibility and persistence/durability
   * Input: Check subheadline content
   * Expected: Subheadline mentions memcached compatibility and persistence/durability
   */
  test('should display subheadline with memcached compatibility and persistence/durability', async ({ page }) => {
    // Verify subheadline exists and is visible
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();

    // Get the subheadline text
    const subheadlineText = await subheadline.textContent();
    expect(subheadlineText).toBeTruthy();

    // Verify it mentions memcached compatibility (case insensitive)
    expect(subheadlineText?.toLowerCase()).toMatch(/memcached/);

    // Verify it mentions persistence or durability (case insensitive)
    expect(subheadlineText?.toLowerCase()).toMatch(/(persist|durabl|disk)/);
  });
});
