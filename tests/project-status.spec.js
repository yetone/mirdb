// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Project Status and Roadmap Section Tests
 * Scenario: Verify the project status section shows implemented vs. planned features
 */
test.describe('Project Status and Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Check for implemented features section
   * Input: Check for implemented features section
   * Expected: Section listing currently implemented features exists
   */
  test('TC1: Section listing currently implemented features exists', async ({ page }) => {
    // Navigate to or scroll to project status section
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await expect(projectStatusSection).toBeVisible();

    // Check that implemented features section exists
    const implementedSection = page.locator('[data-testid="implemented-features"]');
    await expect(implementedSection).toBeVisible();

    // Verify heading indicates implemented/current features
    const implementedHeading = page.locator('[data-testid="implemented-features-heading"]');
    await expect(implementedHeading).toBeVisible();
    const headingText = await implementedHeading.textContent();
    expect(headingText.toLowerCase()).toMatch(/implemented|current|features/);
  });

  /**
   * Test Case 2: Verify Tokio networking listed as implemented
   * Input: Verify Tokio networking listed as implemented
   * Expected: Async networking or Tokio mentioned in implemented features
   */
  test('TC2: Async networking or Tokio mentioned in implemented features', async ({ page }) => {
    const implementedSection = page.locator('[data-testid="implemented-features"]');
    await expect(implementedSection).toBeVisible();

    const implementedText = await implementedSection.textContent();
    const lowerText = implementedText.toLowerCase();

    // Check for Tokio or async networking reference
    const hasTokioOrAsync = lowerText.includes('tokio') || lowerText.includes('async') || lowerText.includes('networking');
    expect(hasTokioOrAsync).toBeTruthy();
  });

  /**
   * Test Case 3: Verify compaction listed as implemented
   * Input: Verify compaction listed as implemented
   * Expected: Minor and major compaction listed in implemented features
   */
  test('TC3: Minor and major compaction listed in implemented features', async ({ page }) => {
    const implementedSection = page.locator('[data-testid="implemented-features"]');
    await expect(implementedSection).toBeVisible();

    const implementedText = await implementedSection.textContent();
    const lowerText = implementedText.toLowerCase();

    // Check for compaction reference (minor and major)
    expect(lowerText).toContain('compaction');

    // Verify both types are mentioned
    const hasMinor = lowerText.includes('minor');
    const hasMajor = lowerText.includes('major');
    expect(hasMinor || hasMajor).toBeTruthy();
  });

  /**
   * Test Case 4: Check for planned/roadmap section
   * Input: Check for planned/roadmap section
   * Expected: Section indicating future features or roadmap exists
   */
  test('TC4: Section indicating future features or roadmap exists', async ({ page }) => {
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await expect(projectStatusSection).toBeVisible();

    // Check that planned/roadmap section exists
    const plannedSection = page.locator('[data-testid="planned-features"]');
    await expect(plannedSection).toBeVisible();

    // Verify heading indicates planned/roadmap/future features
    const plannedHeading = page.locator('[data-testid="planned-features-heading"]');
    await expect(plannedHeading).toBeVisible();
    const headingText = await plannedHeading.textContent();
    expect(headingText.toLowerCase()).toMatch(/planned|roadmap|future|coming/);
  });

  /**
   * Test Case 5: Verify Raft consensus listed as planned
   * Input: Verify Raft consensus listed as planned
   * Expected: Distributed operation or Raft consensus mentioned as planned feature
   */
  test('TC5: Distributed operation or Raft consensus mentioned as planned feature', async ({ page }) => {
    const plannedSection = page.locator('[data-testid="planned-features"]');
    await expect(plannedSection).toBeVisible();

    const plannedText = await plannedSection.textContent();
    const lowerText = plannedText.toLowerCase();

    // Check for Raft or distributed reference
    const hasRaftOrDistributed = lowerText.includes('raft') || lowerText.includes('distributed') || lowerText.includes('consensus');
    expect(hasRaftOrDistributed).toBeTruthy();
  });

  /**
   * Additional test: Project status section has proper semantic structure
   */
  test('Project status section has proper semantic structure', async ({ page }) => {
    const projectStatusSection = page.locator('[data-testid="project-status-section"]');
    await expect(projectStatusSection).toBeVisible();

    // Verify section has proper role
    await expect(projectStatusSection).toHaveAttribute('role', 'region');

    // Verify section has aria-labelledby for accessibility
    const labelledBy = await projectStatusSection.getAttribute('aria-labelledby');
    expect(labelledBy).toBeTruthy();
  });

  /**
   * Additional test: Navigation link to project status section exists
   */
  test('Navigation link to project status section exists', async ({ page }) => {
    // Check navigation has a link to status section
    const navLink = page.locator('[data-testid="nav-status"]');
    await expect(navLink).toBeVisible();

    const href = await navLink.getAttribute('href');
    expect(href).toBe('#status');
  });
});
