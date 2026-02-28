/**
 * E2E tests for MirDB Homepage
 * Shared by multiple scenarios
 *
 * Test suites:
 * - Hero section tests (Scenario 1)
 * - Features display tests (Scenario 2)
 * - Code example tests (Scenario 3)
 * - Quick start tests (Scenario 5)
 * - Performance tests (Scenario 8)
 */

const { test, expect } = require('@playwright/test');

/**
 * Scenario 1: Hero Section and Branding Tests
 * Tests REQ-1, REQ-2, and Story 1
 */
test.describe('Hero Section and Branding', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page loads successfully with HTTP 200 status', async ({ page }) => {
    // Navigate to homepage and verify successful load
    const response = await page.goto('/');
    expect(response.status()).toBe(200);
  });

  test('TC2: MirDB logo image is present with correct src and alt text', async ({ page }) => {
    // Check for logo image in hero section
    const heroLogo = page.locator('#hero .hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify src contains 'logo'
    const src = await heroLogo.getAttribute('src');
    expect(src).toContain('logo');

    // Verify alt text is present
    const altText = await heroLogo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);
  });

  test('TC3: H1 heading exists with text MirDB', async ({ page }) => {
    // Check for h1 element with MirDB text
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toHaveText('MirDB');
  });

  test('TC4: Tagline contains Persistent Key-Value Store and Memcached', async ({ page }) => {
    // Check for tagline element with required text
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached');
  });

  test('TC5: Get Started CTA button is present and clickable', async ({ page }) => {
    // Check for CTA button with Get Started text
    const ctaButton = page.locator('.hero-cta');
    await expect(ctaButton).toBeVisible();

    const ctaText = await ctaButton.textContent();
    expect(ctaText).toContain('Get Started');

    // Verify it's clickable (has href attribute)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify it links to quickstart section or GitHub
    expect(href === '#quickstart' || href.includes('github')).toBeTruthy();
  });

  test('TC6: Hero section is visible without scrolling on 1920x1080 viewport', async ({ page }) => {
    // Set viewport to 1920x1080
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Check that hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero is in viewport without scrolling
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.y).toBeGreaterThanOrEqual(0);
    expect(heroBox.y).toBeLessThan(1080); // Should be visible in viewport

    // Verify key elements are visible
    const heroLogo = page.locator('#hero .hero-logo');
    const heroTitle = page.locator('#hero .hero-title');
    const heroTagline = page.locator('#hero .hero-tagline');
    const heroCta = page.locator('#hero .hero-cta');

    await expect(heroLogo).toBeInViewport();
    await expect(heroTitle).toBeInViewport();
    await expect(heroTagline).toBeInViewport();
    await expect(heroCta).toBeInViewport();
  });
});

// ============================================
// SCENARIO 2: Features and Status Display Tests
// Tests REQ-3, REQ-4, and Story 2
// ============================================

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check for features section element
  test('features section exists with proper identifier', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });

  // Test Case 2: Check for Memcached protocol feature
  test('displays Memcached protocol feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const memcachedFeature = featuresSection.getByText(/Memcached protocol|Memcached compatible/i).first();
    await expect(memcachedFeature).toBeVisible();
  });

  // Test Case 3: Check for persistence feature
  test('displays persistence/SSTables feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const persistenceFeature = featuresSection.getByText(/persistence|SSTables/i).first();
    await expect(persistenceFeature).toBeVisible();
  });

  // Test Case 4: Check for LSM tree feature
  test('displays LSM tree feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const lsmFeature = featuresSection.getByText(/LSM tree|Log-Structured Merge/i).first();
    await expect(lsmFeature).toBeVisible();
  });

  // Test Case 5: Check for skip list feature
  test('displays skip list/memtable feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const skipListFeature = featuresSection.getByText(/skip list|memtable/i).first();
    await expect(skipListFeature).toBeVisible();
  });

  // Test Case 6: Check for compaction feature
  test('displays compaction feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    // Use .first() since there are multiple matches (title and description)
    const compactionFeature = featuresSection.getByText(/compaction/i).first();
    await expect(compactionFeature).toBeVisible();
  });

  // Test Case 10: Verify feature grid layout
  test('features are displayed in grid layout', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check that it's using CSS grid or flexbox
    const display = await featuresGrid.evaluate(el => {
      return window.getComputedStyle(el).display;
    });

    expect(['grid', 'flex']).toContain(display);
  });
});

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 7: Check for project status section
  test('status section exists with proper identifier', async ({ page }) => {
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Also check for Status heading
    const statusHeading = statusSection.getByRole('heading', { name: /Status/i });
    await expect(statusHeading).toBeVisible();
  });

  // Test Case 8: Verify implemented features have checkmarks
  test('implemented features have check indicators', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Check that Tokio is marked as implemented
    const tokioItem = statusSection.getByText(/Tokio/i);
    await expect(tokioItem).toBeVisible();

    // Check that skip list is marked as implemented
    const skipListItem = statusSection.getByText(/skip list/i);
    await expect(skipListItem).toBeVisible();

    // Check that minor compaction is marked as implemented
    const minorCompactionItem = statusSection.getByText(/minor compaction/i);
    await expect(minorCompactionItem).toBeVisible();

    // Check that major compaction is marked as implemented
    const majorCompactionItem = statusSection.getByText(/major compaction/i);
    await expect(majorCompactionItem).toBeVisible();

    // Verify checkmark indicators exist for implemented items
    const implementedChecks = statusSection.locator('.status-check.implemented');
    const checkCount = await implementedChecks.count();
    expect(checkCount).toBeGreaterThanOrEqual(4);
  });

  // Test Case 9: Verify raft is marked as planned
  test('raft is marked as planned or coming soon', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Check that Raft exists
    const raftItem = statusSection.getByText(/Raft/i).first();
    await expect(raftItem).toBeVisible();

    // Check for planned/coming soon indicator - use .first() since multiple elements may match
    const plannedIndicator = statusSection.getByText(/planned|coming soon/i).first();
    await expect(plannedIndicator).toBeVisible();
  });
});

// Placeholder for Scenario 3: Code Example Tests
test.describe.skip('Usage Examples Section', () => {
  // To be implemented by Scenario 3
});

// Placeholder for Scenario 5: Quick Start Tests
test.describe.skip('Quick Start and Documentation', () => {
  // To be implemented by Scenario 5
});

// Placeholder for Scenario 8: Performance Tests
test.describe.skip('Performance and Load Time', () => {
  // To be implemented by Scenario 8
});
