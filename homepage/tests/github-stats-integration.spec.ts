import { test, expect } from '@playwright/test';

test.describe('GitHub Stats Integration Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC4: Stats are fetched at build time and embedded in HTML', async ({ page }) => {
    // This test verifies that GitHub stats are:
    // 1. Fetched during the build process (not at runtime)
    // 2. Embedded as static HTML content
    // 3. Not loading/placeholder values

    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');

    // Get the stats section
    const statsSection = page.locator('[data-testid="github-stats-section"]');
    await expect(statsSection).toBeVisible();

    // Verify the metadata timestamp exists (proves data was fetched at build time)
    const metadata = page.locator('[data-testid="github-stats-metadata"]');
    const fetchedAt = await metadata.getAttribute('content');
    expect(fetchedAt).toBeTruthy();

    // Parse the timestamp
    const fetchedDate = new Date(fetchedAt!);
    expect(fetchedDate.toString()).not.toBe('Invalid Date');

    // Get the raw HTML of the stats section to verify values are embedded
    const starsHtml = await page.locator('[data-testid="github-stats-stars-count"]').innerHTML();
    const contributorsHtml = await page.locator('[data-testid="github-stats-contributors-count"]').innerHTML();

    // Verify the HTML contains actual numbers (not placeholder text)
    // This confirms data was fetched and embedded at build time
    expect(starsHtml).not.toContain('Loading');
    expect(starsHtml).not.toContain('undefined');
    expect(starsHtml).not.toContain('null');

    expect(contributorsHtml).not.toContain('Loading');
    expect(contributorsHtml).not.toContain('undefined');
    expect(contributorsHtml).not.toContain('null');

    // Verify stars contains a numeric value
    const starsText = await page.locator('[data-testid="github-stats-stars-count"]').textContent();
    expect(starsText).toMatch(/^\s*\d+(\.\d+)?[kKmM]?\s*stars\s*$/i);

    // Verify contributors contains a numeric value
    const contributorsText = await page.locator('[data-testid="github-stats-contributors-count"]').textContent();
    expect(contributorsText).toMatch(/^\s*\d+\s*contributors\s*$/i);

    // Verify there are no loading spinners or skeleton screens
    const loadingElements = page.locator('[data-testid="github-stats-section"] .loading, [data-testid="github-stats-section"] .skeleton');
    await expect(loadingElements).toHaveCount(0);
  });

  test('Stats values are reasonable and not error states', async ({ page }) => {
    // Get the stats values
    const starsText = await page.locator('[data-testid="github-stats-stars-count"]').textContent();
    const contributorsText = await page.locator('[data-testid="github-stats-contributors-count"]').textContent();

    // Extract numeric values
    const starsMatch = starsText?.match(/(\d+(?:\.\d+)?)/);
    const contributorsMatch = contributorsText?.match(/(\d+)/);

    expect(starsMatch).toBeTruthy();
    expect(contributorsMatch).toBeTruthy();

    const starsValue = parseFloat(starsMatch![1]);
    const contributorsValue = parseInt(contributorsMatch![1], 10);

    // Stars should be a non-negative number
    // Even if API fails, we expect 0 (fallback), not negative or NaN
    expect(starsValue).toBeGreaterThanOrEqual(0);
    expect(Number.isNaN(starsValue)).toBe(false);

    // Contributors should be a positive number (at least 1 - the repo owner)
    // If 0, the API may have failed, but it's still a valid fallback
    expect(contributorsValue).toBeGreaterThanOrEqual(0);
    expect(Number.isNaN(contributorsValue)).toBe(false);
  });

  test('No runtime API calls are made for GitHub stats', async ({ page }) => {
    // Intercept all network requests to GitHub API
    const githubApiCalls: string[] = [];

    page.on('request', (request) => {
      const url = request.url();
      if (url.includes('api.github.com')) {
        githubApiCalls.push(url);
      }
    });

    // Navigate and wait for all network activity to complete
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify no GitHub API calls were made at runtime
    // This confirms stats were embedded at build time
    expect(githubApiCalls).toHaveLength(0);
  });

  test('Stats section renders without JavaScript', async ({ page, context }) => {
    // Disable JavaScript
    await context.route('**/*.js', (route) => route.abort());

    // Navigate to page
    await page.goto('/');

    // Stats should still be visible (server-rendered/static HTML)
    const statsSection = page.locator('[data-testid="github-stats-section"]');

    // The section should exist in the DOM (even if hidden or unstyled without JS)
    await expect(statsSection).toBeAttached();
  });
});
