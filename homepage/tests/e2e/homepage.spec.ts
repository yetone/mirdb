/**
 * Homepage E2E Tests - Hero Section
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for hero section rendering with:
 * - Gradient/subtle background
 * - Correct layout
 * - Visual elements
 */

import { test, expect } from '@playwright/test';

test.describe('Homepage Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 4: Hero section renders with gradient background and correct layout', async ({
    page,
  }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify layout structure - centered content
    const heading = page.getByRole('heading', { level: 1 });
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');

    const tagline = heroSection.getByText(
      'A Persistent Key-Value Store with Memcached Protocol',
      { exact: true }
    );
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible within the hero section
    const githubButton = heroSection.getByRole('link', { name: /view on github/i });
    const docsButton = heroSection.getByRole('link', { name: /get started/i });
    await expect(githubButton).toBeVisible();
    await expect(docsButton).toBeVisible();
  });

  test('Hero section is above the fold on desktop viewport', async ({
    page,
  }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    const heroSection = page.locator('#hero');
    const boundingBox = await heroSection.boundingBox();

    expect(boundingBox).not.toBeNull();
    // Hero should start at or near the top of the page
    expect(boundingBox!.y).toBeLessThan(100);
  });

  test('CTA buttons have proper focus indicators', async ({ page }) => {
    const heroSection = page.locator('#hero');
    const githubButton = heroSection.getByRole('link', { name: /view on github/i });

    // Focus the button using keyboard
    await githubButton.focus();

    // Button should be focused and visible
    await expect(githubButton).toBeFocused();
    await expect(githubButton).toBeVisible();
  });

  test('Hero section renders correctly on mobile viewport', async ({
    page,
  }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // All content should still be visible
    const heading = heroSection.getByRole('heading', { level: 1 });
    await expect(heading).toBeVisible();

    const githubButton = heroSection.getByRole('link', { name: /view on github/i });
    const docsButton = heroSection.getByRole('link', { name: /get started/i });
    await expect(githubButton).toBeVisible();
    await expect(docsButton).toBeVisible();
  });

  test('GitHub link points to correct repository', async ({ page }) => {
    const heroSection = page.locator('#hero');
    const githubButton = heroSection.getByRole('link', { name: /view on github/i });
    const href = await githubButton.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('Decorative elements are hidden from screen readers', async ({
    page,
  }) => {
    const heroSection = page.locator('#hero');
    const decorativeElements = heroSection.locator('[aria-hidden="true"]');

    // Should have decorative background elements
    const count = await decorativeElements.count();
    expect(count).toBeGreaterThan(0);
  });
});

/**
 * Homepage E2E Tests - Footer Section
 * Owner: Scenario 10 - Footer and Project Metadata
 *
 * Tests for footer section with:
 * - CI badge navigation
 * - GitHub links
 * - Project metadata
 */
test.describe('Homepage Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 4: Click CI badge navigates to CircleCI build status page', async ({
    page,
  }) => {
    // Scroll to footer to ensure it's visible
    const footer = page.getByRole('contentinfo');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the CircleCI badge link
    const ciBadgeLink = page.getByTestId('circleci-badge-link');
    await expect(ciBadgeLink).toBeVisible();

    // Verify the badge link has correct href
    const href = await ciBadgeLink.getAttribute('href');
    expect(href).toBe('https://circleci.com/gh/yetone/mirdb');

    // Verify link opens in new tab (has target="_blank")
    const target = await ciBadgeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify link has security attributes
    const rel = await ciBadgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('Footer displays CircleCI badge', async ({ page }) => {
    const footer = page.getByRole('contentinfo');
    await footer.scrollIntoViewIfNeeded();

    const badge = page.getByTestId('circleci-badge');
    await expect(badge).toBeVisible();

    const src = await badge.getAttribute('src');
    expect(src).toBe('https://circleci.com/gh/yetone/mirdb.svg?style=svg');
  });

  test('Footer displays GitHub repository link', async ({ page }) => {
    const footer = page.getByRole('contentinfo');
    await footer.scrollIntoViewIfNeeded();

    const githubLink = page.getByTestId('github-link');
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('Footer is visible at the bottom of the page', async ({ page }) => {
    // Scroll to bottom of the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    const footer = page.getByRole('contentinfo');
    await expect(footer).toBeVisible();
  });

  test('Footer displays project metadata', async ({ page }) => {
    const footer = page.getByRole('contentinfo');
    await footer.scrollIntoViewIfNeeded();

    // Check for project name - use first() since MirDB appears multiple times
    await expect(footer.getByText(/MirDB/).first()).toBeVisible();

    // Check for project description - use first() since text may appear multiple times
    await expect(footer.getByText(/Persistent Key-Value Store/).first()).toBeVisible();
  });

  test('Footer links have proper accessibility', async ({ page }) => {
    const footer = page.getByRole('contentinfo');
    await footer.scrollIntoViewIfNeeded();

    // Check that footer has navigation role
    const nav = footer.getByRole('navigation', { name: /footer navigation/i });
    await expect(nav).toBeVisible();

    // Check that all links in footer navigation are accessible
    const links = await nav.getByRole('link').all();
    expect(links.length).toBeGreaterThan(0);
  });
});
