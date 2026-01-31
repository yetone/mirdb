/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Hero section visibility
 * - Project name and tagline presence
 * - CTA buttons functionality
 * - Visual element presence
 *
 * Requirements: REQ-1
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Hero section contains h1 with MirDB, tagline, description, and two CTA buttons', async ({ page }) => {
    // Verify hero section exists and is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify h1 contains 'MirDB'
    const h1 = heroSection.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify tagline is present
    const tagline = heroSection.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent');
    await expect(tagline).toContainText('Memcached');

    // Verify description paragraph
    const description = heroSection.locator('.hero-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('A persistent key-value store with Memcached protocol compatibility');

    // Verify two CTA buttons exist
    const getStartedBtn = heroSection.locator('[data-testid="cta-get-started"]');
    const githubBtn = heroSection.locator('[data-testid="cta-github"]');

    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toContainText('Get Started');

    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('View on GitHub');
  });

  test('Test Case 2: Get Started button scrolls to Quick Start section', async ({ page }) => {
    // Click the Get Started button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await getStartedBtn.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the Quick Start section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Test Case 3: View on GitHub button opens GitHub repository in new tab', async ({ page, context }) => {
    // Listen for new page/tab
    const pagePromise = context.waitForEvent('page');

    // Click the GitHub button
    const githubBtn = page.locator('[data-testid="cta-github"]');
    await githubBtn.click();

    // Wait for the new page
    const newPage = await pagePromise;
    await newPage.waitForLoadState();

    // Verify the URL contains github.com and mirdb
    expect(newPage.url()).toContain('github.com');
    expect(newPage.url().toLowerCase()).toContain('mirdb');
  });

  test('Test Case 4: Visual element (logo/graphic) is present in hero section', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Check for visual element - could be logo, SVG, or image
    const heroVisual = heroSection.locator('.hero-visual');
    await expect(heroVisual).toBeVisible();

    // Verify there's either an SVG, img, or ascii art element
    const svgElement = heroVisual.locator('svg');
    const imgElement = heroVisual.locator('img');
    const asciiElement = heroVisual.locator('.hero-ascii');

    // At least one visual element should be present
    const hasSvg = await svgElement.count() > 0;
    const hasImg = await imgElement.count() > 0;
    const hasAscii = await asciiElement.count() > 0;

    expect(hasSvg || hasImg || hasAscii).toBeTruthy();

    // If SVG exists, verify it's visible
    if (hasSvg) {
      await expect(svgElement.first()).toBeVisible();
    }
  });

  test('Hero section is the first major content block visible', async ({ page }) => {
    // Navigate to page
    const heroSection = page.locator('#hero');

    // The hero should be at the top (after header)
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();

    // Hero should start near the top of the viewport (accounting for header)
    expect(boundingBox.y).toBeLessThan(200); // Should be within first 200px
  });

  test('CTA buttons have proper accessibility attributes', async ({ page }) => {
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    const githubBtn = page.locator('[data-testid="cta-github"]');

    // Get Started button should have an href
    await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');

    // GitHub button should have target blank and rel attributes for security
    await expect(githubBtn).toHaveAttribute('target', '_blank');
    await expect(githubBtn).toHaveAttribute('rel', /noopener/);
  });

  test('Hero section has proper semantic structure', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Verify it's a section element
    const tagName = await heroSection.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('section');

    // Verify it has aria-labelledby
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-title');

    // Verify the h1 has the correct id
    const h1 = heroSection.locator('h1');
    await expect(h1).toHaveAttribute('id', 'hero-title');
  });
});
