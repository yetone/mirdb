import { test, expect } from '@playwright/test';

test.describe('GitHub Statistics Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Repository star count is displayed', async ({ page }) => {
    // Locate GitHub stats section
    const statsSection = page.locator('[data-testid="github-stats-section"]');
    await expect(statsSection).toBeVisible();

    // Verify stars display element exists and is visible
    const starsElement = page.locator('[data-testid="github-stats-stars"]');
    await expect(starsElement).toBeVisible();

    // Verify the stars count text is displayed
    const starsCount = page.locator('[data-testid="github-stats-stars-count"]');
    await expect(starsCount).toBeVisible();

    // Verify the text contains "stars" keyword
    await expect(starsCount).toHaveText(/stars/i);
  });

  test('TC2: Number of contributors is displayed', async ({ page }) => {
    // Locate GitHub stats section
    const statsSection = page.locator('[data-testid="github-stats-section"]');
    await expect(statsSection).toBeVisible();

    // Verify contributors display element exists and is visible
    const contributorsElement = page.locator('[data-testid="github-stats-contributors"]');
    await expect(contributorsElement).toBeVisible();

    // Verify the contributors count text is displayed
    const contributorsCount = page.locator('[data-testid="github-stats-contributors-count"]');
    await expect(contributorsCount).toBeVisible();

    // Verify the text contains "contributors" keyword
    await expect(contributorsCount).toHaveText(/contributors/i);
  });

  test('TC3: Latest release version or tag is displayed', async ({ page }) => {
    // Locate GitHub stats section
    const statsSection = page.locator('[data-testid="github-stats-section"]');
    await expect(statsSection).toBeVisible();

    // Check if release element exists (may not exist if no releases)
    const releaseElement = page.locator('[data-testid="github-stats-release"]');
    const releaseExists = await releaseElement.count() > 0;

    if (releaseExists) {
      await expect(releaseElement).toBeVisible();

      // Verify the release version text is displayed
      const releaseVersion = page.locator('[data-testid="github-stats-release-version"]');
      await expect(releaseVersion).toBeVisible();

      // Verify it contains version-like text (e.g., v0.1.0, 1.0.0, etc.) or some tag name
      const versionText = await releaseVersion.textContent();
      expect(versionText).toBeTruthy();
      expect(versionText!.trim().length).toBeGreaterThan(0);
    } else {
      // If no release element, log for information (not a failure if repo has no releases)
      console.log('No latest release element found - repository may not have releases');
    }
  });

  test('TC4: Stats reflect actual repository data (fetched at build time)', async ({ page }) => {
    // Verify the GitHub stats metadata element exists (indicates data was fetched)
    const metadata = page.locator('[data-testid="github-stats-metadata"]');
    await expect(metadata).toHaveCount(1);

    // Get the fetched-at timestamp
    const fetchedAt = await metadata.getAttribute('content');
    expect(fetchedAt).toBeTruthy();

    // Verify it's a valid ISO timestamp
    const fetchedDate = new Date(fetchedAt!);
    expect(fetchedDate.getTime()).not.toBeNaN();

    // Verify the fetch timestamp is recent (within last 30 days for build-time fetch)
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    expect(fetchedDate.getTime()).toBeGreaterThan(thirtyDaysAgo.getTime());

    // Verify stars value is a number (not placeholder text like "Loading..." or "N/A")
    const starsCount = page.locator('[data-testid="github-stats-stars-count"]');
    const starsText = await starsCount.textContent();
    expect(starsText).toBeTruthy();
    // Should contain a number (possibly formatted like "1.2k")
    expect(starsText).toMatch(/\d+(\.\d+)?[kKmM]?\s*stars/i);

    // Verify contributors value is a number
    const contributorsCount = page.locator('[data-testid="github-stats-contributors-count"]');
    const contributorsText = await contributorsCount.textContent();
    expect(contributorsText).toBeTruthy();
    // Should contain a number
    expect(contributorsText).toMatch(/\d+\s*contributors/i);
  });

  test('GitHub stats section is positioned in hero section', async ({ page }) => {
    // Verify the stats section is within the hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    const statsSection = heroSection.locator('[data-testid="github-stats-section"]');

    await expect(heroSection).toBeVisible();
    await expect(statsSection).toBeVisible();
  });

  test('GitHub stats links are correctly configured', async ({ page }) => {
    // Verify stars link points to stargazers page
    const starsLink = page.locator('[data-testid="github-stats-stars"]');
    await expect(starsLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb\/stargazers/);
    await expect(starsLink).toHaveAttribute('target', '_blank');
    await expect(starsLink).toHaveAttribute('rel', /noopener/);

    // Verify contributors link points to contributors page
    const contributorsLink = page.locator('[data-testid="github-stats-contributors"]');
    await expect(contributorsLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb\/graphs\/contributors/);
    await expect(contributorsLink).toHaveAttribute('target', '_blank');
    await expect(contributorsLink).toHaveAttribute('rel', /noopener/);

    // Verify release link (if exists) points to releases page
    const releaseLink = page.locator('[data-testid="github-stats-release"]');
    const releaseExists = await releaseLink.count() > 0;
    if (releaseExists) {
      await expect(releaseLink).toHaveAttribute('href', /github\.com\/yetone\/mirdb\/releases/);
      await expect(releaseLink).toHaveAttribute('target', '_blank');
      await expect(releaseLink).toHaveAttribute('rel', /noopener/);
    }
  });

  test('GitHub stats are visually styled correctly', async ({ page }) => {
    // Verify the stats section has proper flex layout
    const statsSection = page.locator('[data-testid="github-stats-section"]');
    await expect(statsSection).toHaveClass(/flex/);

    // Verify stat items have visible icons
    const starsElement = page.locator('[data-testid="github-stats-stars"] svg');
    await expect(starsElement).toBeVisible();

    const contributorsElement = page.locator('[data-testid="github-stats-contributors"] svg');
    await expect(contributorsElement).toBeVisible();
  });
});
