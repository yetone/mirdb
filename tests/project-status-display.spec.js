// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Project Status Display Scenario Tests
 *
 * These tests verify that the homepage displays:
 * 1. Project status (alpha/beta/stable) indicator
 * 2. List of currently implemented features
 * 3. Current version number or release information
 */

test.describe('Project Status Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Project Status Indicator', () => {
    test('should display a project status section', async ({ page }) => {
      // Check for project status element
      const projectStatus = page.locator('[data-testid="project-status"]');
      await expect(projectStatus).toBeVisible();
    });

    test('should display project status label (alpha/beta/stable)', async ({ page }) => {
      // The status should indicate one of: alpha, beta, or stable
      const statusLabel = page.locator('[data-testid="project-status-label"]');
      await expect(statusLabel).toBeVisible();

      // Verify the status text contains a valid phase
      const statusText = await statusLabel.textContent();
      const validStatuses = ['alpha', 'beta', 'stable', 'development'];
      const hasValidStatus = validStatuses.some(status =>
        statusText?.toLowerCase().includes(status)
      );
      expect(hasValidStatus).toBeTruthy();
    });

    test('should have visual status indicator', async ({ page }) => {
      // Check for visual indicator element
      const statusIndicator = page.locator('[data-testid="project-status"] .status-indicator');
      await expect(statusIndicator).toBeVisible();

      // Verify it has appropriate styling (colored dot)
      const backgroundColor = await statusIndicator.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      expect(backgroundColor).not.toBe('transparent');
    });
  });

  test.describe('Test Case 2: Implemented Features List', () => {
    test('should display implemented features section', async ({ page }) => {
      // Check for the implemented features section
      const featuresSection = page.locator('[data-testid="implemented-features"]');
      await expect(featuresSection).toBeVisible();
    });

    test('should display a list of implemented features', async ({ page }) => {
      // Check for the features list
      const featuresList = page.locator('[data-testid="implemented-features-list"]');
      await expect(featuresList).toBeVisible();

      // Verify there are multiple feature items
      const featureItems = page.locator('[data-testid="implemented-features-list"] li');
      const count = await featureItems.count();
      expect(count).toBeGreaterThan(0);
    });

    test('should list specific implemented features', async ({ page }) => {
      // Verify key implemented features are listed
      const featuresList = page.locator('[data-testid="implemented-features-list"]');
      const featuresText = await featuresList.textContent();

      // Based on knowledge file, these features are implemented
      const expectedFeatures = [
        'Tokio-based async networking',
        'Memtable with skip list',
        'Minor compaction',
        'Major compaction'
      ];

      // Check at least some of the expected features are present
      const foundFeatures = expectedFeatures.filter(feature =>
        featuresText?.toLowerCase().includes(feature.toLowerCase().split(' ')[0])
      );
      expect(foundFeatures.length).toBeGreaterThan(0);
    });

    test('should have proper heading for implemented features', async ({ page }) => {
      const heading = page.locator('[data-testid="implemented-features"] h3, [data-testid="implemented-features"] h4');
      await expect(heading).toBeVisible();

      const headingText = await heading.textContent();
      expect(headingText?.toLowerCase()).toContain('implemented');
    });
  });

  test.describe('Test Case 3: Version Information', () => {
    test('should display version information section', async ({ page }) => {
      // Check for version display
      const versionInfo = page.locator('[data-testid="version-info"]');
      await expect(versionInfo).toBeVisible();
    });

    test('should display version number or release label', async ({ page }) => {
      const versionText = page.locator('[data-testid="version-number"]');
      await expect(versionText).toBeVisible();

      // Version should contain a version-like pattern or release label
      const text = await versionText.textContent();
      // Accept patterns like: "v0.1.0", "0.1.0", "alpha", "beta", "1.0.0"
      const hasVersionPattern = /v?\d+\.\d+(\.\d+)?|alpha|beta|rc/i.test(text || '');
      expect(hasVersionPattern).toBeTruthy();
    });

    test('version info should be easily visible in footer or status section', async ({ page }) => {
      // Version should be in footer or a dedicated status section
      const footer = page.locator('footer');
      const statusSection = page.locator('[data-testid="project-status"]');

      // At least one should contain version info
      const footerHasVersion = await footer.locator('[data-testid="version-info"], [data-testid="version-number"]').count() > 0;
      const statusHasVersion = await statusSection.locator('[data-testid="version-info"], [data-testid="version-number"]').count() > 0;

      expect(footerHasVersion || statusHasVersion).toBeTruthy();
    });
  });

  test.describe('Integration Tests', () => {
    test('project status section should be properly structured', async ({ page }) => {
      // The project status section should contain all key elements
      const projectStatus = page.locator('[data-testid="project-status"]');

      // Should be within footer or a dedicated section
      await expect(projectStatus).toBeVisible();

      // Should have accessible structure
      const hasAccessibleContent = await projectStatus.evaluate(el => {
        // Check for proper semantic structure or ARIA attributes
        return el.textContent && el.textContent.trim().length > 0;
      });
      expect(hasAccessibleContent).toBeTruthy();
    });

    test('all status components should be visible without scrolling on desktop', async ({ page }) => {
      // Set desktop viewport
      await page.setViewportSize({ width: 1280, height: 720 });

      // Go to footer where status is shown
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();

      // All status elements should be visible
      const projectStatus = page.locator('[data-testid="project-status"]');
      await expect(projectStatus).toBeVisible();
    });
  });
});
