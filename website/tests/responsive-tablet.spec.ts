import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Responsive Design - Tablet
 * Scenario: Verify that the website displays correctly on tablet devices
 * NFR-1: Website must be responsive on tablet
 */

// Configure tablet viewport (iPad size - 768px width)
test.use({
  viewport: { width: 768, height: 1024 },
});

test.describe('Responsive Design - Tablet', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage at tablet viewport (768px width)
    await page.goto('/');
  });

  /**
   * Test Case 1: Load homepage at 768px width
   * Input: Load homepage at 768px width
   * Expected: Page displays with tablet-appropriate layout, no layout breaks
   */
  test('should display page with tablet-appropriate layout without layout breaks', async ({ page }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible and properly laid out
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero section width doesn't exceed viewport
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox!.width).toBeLessThanOrEqual(768);

    // Verify main content sections are visible
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify no horizontal overflow (layout breaks)
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(768);

    // Verify content is not cut off
    const htmlOverflowX = await page.evaluate(() => {
      const html = document.documentElement;
      return html.scrollWidth > html.clientWidth;
    });
    expect(htmlOverflowX).toBeFalsy();

    // Verify the hero headline is visible and readable
    const heroHeadline = page.locator('[data-testid="hero-headline"]');
    await expect(heroHeadline).toBeVisible();
    await expect(heroHeadline).toContainText('MirDB');

    // Verify CTA buttons are visible
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedButton).toBeVisible();

    const githubButton = page.locator('[data-testid="cta-github"]');
    await expect(githubButton).toBeVisible();
  });

  /**
   * Test Case 2: Check features grid on tablet
   * Input: Check features grid on tablet
   * Expected: Features display in 2-column grid or appropriate tablet layout
   */
  test('should display features in 2-column grid or appropriate tablet layout', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Verify features grid exists
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify all 4 feature cards are present
    const featureCards = page.locator('[data-testid="features-grid"] > div');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    // Get grid layout information
    const gridInfo = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const gridTemplateColumns = style.gridTemplateColumns;
      // Count the number of column values (e.g., "300px 300px" has 2 columns)
      const columns = gridTemplateColumns.split(/\s+/).filter(col => col && col !== 'none');
      return {
        display: style.display,
        columns: columns.length,
        gridTemplateColumns
      };
    });

    // Verify it's a grid display
    expect(gridInfo.display).toBe('grid');

    // For tablet (768px with sm: breakpoint at 640px), expect 2-column grid
    // Tailwind's sm:grid-cols-2 should be active at 768px
    expect(gridInfo.columns).toBeGreaterThanOrEqual(2);

    // Verify each feature card is visible and has proper dimensions
    const memcachedFeature = page.locator('[data-testid="feature-memcached"]');
    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');
    const lsmFeature = page.locator('[data-testid="feature-lsm"]');
    const performanceFeature = page.locator('[data-testid="feature-performance"]');

    await expect(memcachedFeature).toBeVisible();
    await expect(persistenceFeature).toBeVisible();
    await expect(lsmFeature).toBeVisible();
    await expect(performanceFeature).toBeVisible();

    // Verify cards don't overflow the viewport
    for (const feature of [memcachedFeature, persistenceFeature, lsmFeature, performanceFeature]) {
      const box = await feature.boundingBox();
      expect(box).not.toBeNull();
      // Card width should be less than half the viewport plus some padding
      expect(box!.width).toBeLessThan(768 - 32); // accounting for container padding
    }

    // Verify feature titles and descriptions are readable
    await expect(page.locator('[data-testid="feature-memcached-title"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-persistence-title"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-lsm-title"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-performance-title"]')).toBeVisible();
  });

  /**
   * Test Case 3: Check navigation on tablet
   * Input: Check navigation on tablet
   * Expected: Navigation is usable - either full menu or hamburger menu
   */
  test('should display usable navigation - either full menu or hamburger menu', async ({ page }) => {
    // Check for navigation elements
    // The website may have either:
    // 1. A full navigation menu visible at tablet width
    // 2. A hamburger/mobile menu that can be toggled

    // First, check if there's a dedicated nav element or header
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check for CTA buttons which serve as primary navigation
    const getStartedButton = page.locator('[data-testid="cta-get-started"]');
    const githubButton = page.locator('[data-testid="cta-github"]');

    await expect(getStartedButton).toBeVisible();
    await expect(githubButton).toBeVisible();

    // Verify buttons are clickable (not hidden behind other elements)
    const getStartedBox = await getStartedButton.boundingBox();
    expect(getStartedBox).not.toBeNull();
    expect(getStartedBox!.width).toBeGreaterThan(0);
    expect(getStartedBox!.height).toBeGreaterThan(0);

    const githubBox = await githubButton.boundingBox();
    expect(githubBox).not.toBeNull();
    expect(githubBox!.width).toBeGreaterThan(0);
    expect(githubBox!.height).toBeGreaterThan(0);

    // Verify buttons are properly spaced and not overlapping
    // At tablet width, buttons should be arranged horizontally (sm:flex-row)
    const buttonsContainer = getStartedButton.locator('..'); // Parent container
    const containerStyle = await buttonsContainer.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        display: style.display
      };
    });

    // At 768px (above sm: 640px breakpoint), buttons should be in row layout
    expect(containerStyle.display).toBe('flex');
    expect(containerStyle.flexDirection).toBe('row');

    // Verify Get Started button links to getting-started section
    const getStartedHref = await getStartedButton.getAttribute('href');
    expect(getStartedHref).toBe('#getting-started');

    // Verify GitHub button links to external repository
    const githubHref = await githubButton.getAttribute('href');
    expect(githubHref).toContain('github.com');

    // Test that clicking Get Started scrolls to the getting-started section
    await getStartedButton.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify section anchors work for navigation
    // Check that commands section can be navigated to
    await page.goto('/#commands');
    const commandsSection = page.locator('[data-testid="commands-section"]');
    await expect(commandsSection).toBeVisible();

    // Check that configuration section can be navigated to
    await page.goto('/#configuration');
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();
  });
});
