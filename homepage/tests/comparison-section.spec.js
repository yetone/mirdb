// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('MirDB vs Memcached Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Comparison section exists showing MirDB vs memcached', async ({ page }) => {
    // Test Case 1: Query for comparison section or table
    // Expected: Comparison section exists showing MirDB vs memcached
    const comparisonSection = page.locator('#comparison, [data-testid="comparison"], .comparison');
    await expect(comparisonSection).toBeVisible();

    // Verify section has heading/title indicating comparison
    const heading = comparisonSection.locator('h2, h3').first();
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toMatch(/comparison|vs|versus|mirdb.*memcached|memcached.*mirdb/i);

    // Verify both MirDB and memcached are mentioned in the section
    const sectionText = await comparisonSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';
    expect(lowerText).toContain('mirdb');
    expect(lowerText).toMatch(/memcached/i);
  });

  test('TC2: Content clearly states MirDB persists data while memcached does not', async ({ page }) => {
    // Test Case 2: Verify persistence is mentioned as key advantage
    // Expected: Content clearly states MirDB persists data while memcached doesn't
    const comparisonSection = page.locator('#comparison, [data-testid="comparison"], .comparison');
    await expect(comparisonSection).toBeVisible();

    const sectionText = await comparisonSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';

    // Check that persistence is highlighted as a differentiator
    const hasPersistenceAdvantage = (
      (lowerText.includes('persist') && lowerText.includes('mirdb')) ||
      lowerText.includes('data persistence') ||
      lowerText.includes('persists data') ||
      lowerText.includes('disk-backed') ||
      lowerText.includes('survives restart')
    );
    expect(hasPersistenceAdvantage).toBeTruthy();

    // Check that memcached's lack of persistence is mentioned
    const mentionsMemcachedLimitation = (
      lowerText.includes('memcached') && (
        lowerText.includes('volatile') ||
        lowerText.includes('memory only') ||
        lowerText.includes('in-memory') ||
        lowerText.includes('no persistence') ||
        lowerText.includes('doesn\'t persist') ||
        lowerText.includes('does not persist') ||
        lowerText.includes('lost on restart') ||
        lowerText.includes('loses data')
      )
    );
    expect(mentionsMemcachedLimitation).toBeTruthy();
  });

  test('TC3: Content mentions MirDB avoids cold cache issues on restart', async ({ page }) => {
    // Test Case 3: Verify comparison mentions no cold cache problem
    // Expected: Content mentions MirDB avoids cold cache issues on restart
    const comparisonSection = page.locator('#comparison, [data-testid="comparison"], .comparison');
    await expect(comparisonSection).toBeVisible();

    const sectionText = await comparisonSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';

    // Check that cold cache problem is mentioned
    const mentionsColdCache = (
      lowerText.includes('cold cache') ||
      lowerText.includes('cold-cache') ||
      lowerText.includes('cache warming') ||
      lowerText.includes('warm cache') ||
      (lowerText.includes('restart') && (
        lowerText.includes('no data loss') ||
        lowerText.includes('data available') ||
        lowerText.includes('immediately available') ||
        lowerText.includes('survives')
      ))
    );
    expect(mentionsColdCache).toBeTruthy();
  });

  test('TC4: Comparison is presented in a clear format (table or side-by-side)', async ({ page }) => {
    // Additional test: Verify comparison format is easy to understand
    const comparisonSection = page.locator('#comparison, [data-testid="comparison"], .comparison');
    await expect(comparisonSection).toBeVisible();

    // Check for table or comparison grid/cards format
    const hasTable = await comparisonSection.locator('table').count() > 0;
    const hasComparisonGrid = await comparisonSection.locator('.comparison-table, .comparison-grid, .comparison-row').count() > 0;
    const hasComparisonItems = await comparisonSection.locator('.comparison-item, .comparison-feature, .feature-row').count() > 0;

    // At least one of these formats should be present
    const hasClearFormat = hasTable || hasComparisonGrid || hasComparisonItems;
    expect(hasClearFormat).toBeTruthy();
  });
});
