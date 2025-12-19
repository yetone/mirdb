import { test, expect } from '@playwright/test';

test.describe('Source Repository Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: At least one link containing github.com is present', async ({ page }) => {
    // Test Case 1: Query for GitHub links on page
    // Expected: At least one link containing 'github.com' is present
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);
  });

  test('TC2: Hero section contains GitHub/repository link', async ({ page }) => {
    // Test Case 2: Check hero section for repository link
    // Expected: Hero section contains GitHub/repository link
    const heroSection = page.locator('section.hero, #hero, [data-testid="hero"]');
    await expect(heroSection).toBeVisible();

    // Look for GitHub link within hero section
    const githubLink = heroSection.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();

    // Verify it has appropriate text indicating it's a GitHub link
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toMatch(/github|view on github|source/i);
  });

  test('TC3: Footer contains link to source repository', async ({ page }) => {
    // Test Case 3: Check footer for repository link
    // Expected: Footer contains link to source repository
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Look for GitHub link within footer
    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink.first()).toBeVisible();

    // Verify at least one GitHub link in footer
    const count = await githubLink.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test('TC4: Repository links point to valid GitHub repository URL', async ({ page }) => {
    // Test Case 4: Verify repository links have valid href
    // Expected: Links point to valid GitHub repository URL
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Check each GitHub link has a valid URL format
    for (let i = 0; i < count; i++) {
      const href = await githubLinks.nth(i).getAttribute('href');
      expect(href).toBeTruthy();

      // Verify it's a valid GitHub URL (should contain github.com and a repo path)
      expect(href).toMatch(/^https?:\/\/(www\.)?github\.com\/[\w-]+\/[\w-]+/);
    }
  });

  test('TC5: Page indicates project is open source (license mention or badge)', async ({ page }) => {
    // Test Case 5: Verify open source status indication
    // Expected: Page indicates project is open source (license mention or badge)
    const pageContent = await page.content();
    const pageText = await page.locator('body').textContent();

    // Check for open source indicators
    const hasOpenSourceMention = /open.?source/i.test(pageText || '');
    const hasLicenseMention = /license|MIT|Apache|GPL|BSD/i.test(pageText || '');
    const hasLicenseLink = await page.locator('a[href*="license"], a[href*="LICENSE"]').count() > 0;

    // At least one of these indicators should be present
    expect(hasOpenSourceMention || hasLicenseMention || hasLicenseLink).toBeTruthy();
  });
});
