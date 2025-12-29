// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

/**
 * Project Status Display E2E Tests
 * Tests REQ-10 from PRD - Display project status indicating implemented vs. planned features
 */

test.describe('Project Status Display', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Page indicates which features are implemented vs. planned
  test('TC1: Page indicates which features are implemented vs. planned', async ({ page }) => {
    // Find the project status section
    const statusSection = page.locator('#project-status, [data-testid="project-status-section"], .project-status');
    await expect(statusSection).toBeVisible();

    // Check for heading containing 'Status' or similar
    const heading = statusSection.locator('h2').first();
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toContain('status');

    // Check that there are status indicators for features
    const statusItems = statusSection.locator('[data-testid="status-item"], .status-item, .status-list li, .feature-status');
    const itemCount = await statusItems.count();
    expect(itemCount).toBeGreaterThan(0);

    // Verify at least some items have status indicators (implemented/planned)
    const implementedBadges = statusSection.locator('.status-implemented, [data-status="implemented"], .badge-implemented');
    const plannedBadges = statusSection.locator('.status-planned, [data-status="planned"], .badge-planned');

    const implementedCount = await implementedBadges.count();
    const plannedCount = await plannedBadges.count();

    // Should have at least one status indicator
    expect(implementedCount + plannedCount).toBeGreaterThan(0);
  });

  // Test Case 2: Status indicators are visually distinct (badges, icons, or clear text)
  test('TC2: Status indicators are visually distinct (badges, icons, or clear text)', async ({ page }) => {
    // Find the project status section
    const statusSection = page.locator('#project-status, [data-testid="project-status-section"], .project-status');
    await expect(statusSection).toBeVisible();

    // Find status badge elements specifically - use class that starts with status-
    const statusBadges = statusSection.locator('.status-badge');
    const badgeCount = await statusBadges.count();
    expect(badgeCount).toBeGreaterThan(0);

    // Check first badge for visual styling
    const firstBadge = statusBadges.first();
    await expect(firstBadge).toBeVisible();

    // Verify the badge has a background color or border (indicating visual distinction)
    const styles = await firstBadge.evaluate(el => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        borderWidth: computed.borderWidth,
        borderStyle: computed.borderStyle
      };
    });

    // Background should not be fully transparent (rgba(0, 0, 0, 0))
    const hasBackgroundColor = styles.backgroundColor !== 'rgba(0, 0, 0, 0)' && styles.backgroundColor !== 'transparent';

    // Alternative: check for border styling
    const hasBorder = styles.borderWidth !== '0px' && styles.borderStyle !== 'none';

    // Badge should have either background color or border
    expect(hasBackgroundColor || hasBorder).toBeTruthy();
  });

  // Additional test: Verify implemented and planned badges have different styles
  test('TC3: Implemented and planned badges have visually different styles', async ({ page }) => {
    const statusSection = page.locator('#project-status, [data-testid="project-status-section"], .project-status');
    await expect(statusSection).toBeVisible();

    // Find implemented badges
    const implementedBadge = statusSection.locator('.status-implemented, [data-status="implemented"], .badge-implemented').first();

    // Find planned badges
    const plannedBadge = statusSection.locator('.status-planned, [data-status="planned"], .badge-planned').first();

    // Ensure both types exist
    await expect(implementedBadge).toBeVisible();
    await expect(plannedBadge).toBeVisible();

    // Get background colors for comparison
    const implementedBgColor = await implementedBadge.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    const plannedBgColor = await plannedBadge.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Colors should be different to distinguish status
    expect(implementedBgColor).not.toEqual(plannedBgColor);
  });

  // Additional test: Verify status text is clear and readable
  test('TC4: Status text is clear and includes "implemented" or "planned" terminology', async ({ page }) => {
    const statusSection = page.locator('#project-status, [data-testid="project-status-section"], .project-status');
    await expect(statusSection).toBeVisible();

    const sectionText = await statusSection.textContent();
    const lowercaseText = sectionText.toLowerCase();

    // Should contain terminology related to status
    const hasImplemented = lowercaseText.includes('implemented') || lowercaseText.includes('available') || lowercaseText.includes('complete');
    const hasPlanned = lowercaseText.includes('planned') || lowercaseText.includes('coming soon') || lowercaseText.includes('roadmap');

    expect(hasImplemented || hasPlanned).toBeTruthy();
  });

});
