const { test, expect } = require('@playwright/test');

/**
 * Features Section Display Tests
 * Scenario: Validate the features section displays implemented and planned features correctly
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page displays async networking or Tokio as an implemented feature', async ({ page }) => {
    // Navigate to features section and check for async networking/Tokio feature
    const featuresSection = page.locator('#features, [class*="features"], .features');
    await expect(featuresSection.first()).toBeVisible();

    const pageContent = await page.textContent('body');

    // Verify async networking or Tokio appears in the page
    const hasAsyncNetworking = pageContent.toLowerCase().includes('async networking') ||
                               pageContent.toLowerCase().includes('tokio') ||
                               pageContent.toLowerCase().includes('asynchronous networking');
    expect(hasAsyncNetworking).toBe(true);

    // Verify feature is displayed as implemented (not planned)
    const implementedFeature = featuresSection.locator('[class*="feature"]:not([class*="planned"]), .feature-card, .feature-item')
      .filter({ hasText: /tokio|async/i });

    // Should have at least one matching implemented feature
    const count = await implementedFeature.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Test Case 2: Page displays memtable or skip list as an implemented feature', async ({ page }) => {
    // Navigate to features section and check for memtable/skip list feature
    const featuresSection = page.locator('#features, [class*="features"], .features');
    await expect(featuresSection.first()).toBeVisible();

    const pageContent = await page.textContent('body');

    // Verify memtable or skip list appears in the page
    const hasMemtable = pageContent.toLowerCase().includes('memtable') ||
                        pageContent.toLowerCase().includes('skip list') ||
                        pageContent.toLowerCase().includes('skiplist');
    expect(hasMemtable).toBe(true);

    // Verify feature is displayed as implemented (not planned)
    const implementedFeature = featuresSection.locator('[class*="feature"]:not([class*="planned"]), .feature-card, .feature-item')
      .filter({ hasText: /memtable|skip.?list/i });

    // Should have at least one matching implemented feature
    const count = await implementedFeature.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Test Case 3: Page displays minor compaction and major compaction as implemented features', async ({ page }) => {
    // Navigate to features section and check for compaction features
    const featuresSection = page.locator('#features, [class*="features"], .features');
    await expect(featuresSection.first()).toBeVisible();

    const pageContent = await page.textContent('body');

    // Verify minor compaction appears in the page
    const hasMinorCompaction = pageContent.toLowerCase().includes('minor compaction') ||
                               pageContent.toLowerCase().includes('minor-compaction');
    expect(hasMinorCompaction).toBe(true);

    // Verify major compaction appears in the page
    const hasMajorCompaction = pageContent.toLowerCase().includes('major compaction') ||
                               pageContent.toLowerCase().includes('major-compaction');
    expect(hasMajorCompaction).toBe(true);

    // Verify both compaction features are displayed as implemented (not planned)
    const minorCompactionFeature = featuresSection.locator('[class*="feature"]:not([class*="planned"]), .feature-card, .feature-item')
      .filter({ hasText: /minor.?compaction/i });
    const majorCompactionFeature = featuresSection.locator('[class*="feature"]:not([class*="planned"]), .feature-card, .feature-item')
      .filter({ hasText: /major.?compaction/i });

    // Should have matching implemented features for both
    const minorCount = await minorCompactionFeature.count();
    const majorCount = await majorCompactionFeature.count();
    expect(minorCount).toBeGreaterThan(0);
    expect(majorCount).toBeGreaterThan(0);
  });

  test('Test Case 4: Page displays Raft consensus as a planned/upcoming feature with visual distinction', async ({ page }) => {
    // Navigate to features section and check for Raft planned feature
    const featuresSection = page.locator('#features, [class*="features"], .features');
    await expect(featuresSection.first()).toBeVisible();

    const pageContent = await page.textContent('body');

    // Verify Raft appears in the page
    const hasRaft = pageContent.toLowerCase().includes('raft');
    expect(hasRaft).toBe(true);

    // Verify Raft is displayed with visual distinction as planned/upcoming
    // Look for planned/upcoming indicator alongside Raft
    const plannedRaftFeature = featuresSection.locator('[class*="planned"], [class*="upcoming"], [class*="coming-soon"]')
      .filter({ hasText: /raft/i });

    // Should have at least one Raft feature marked as planned
    const plannedCount = await plannedRaftFeature.count();
    expect(plannedCount).toBeGreaterThan(0);

    // Verify visual distinction exists - check that planned features have different styling
    // Either through a badge, different background, or specific class
    const plannedIndicator = featuresSection.locator('.planned-badge, .upcoming-badge, [class*="planned"], [class*="upcoming"]')
      .filter({ hasText: /raft|planned|upcoming|coming soon/i });
    const indicatorCount = await plannedIndicator.count();
    expect(indicatorCount).toBeGreaterThan(0);
  });
});
