import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Project Status Display
 *
 * These tests verify that the page displays project status including:
 * - A dedicated project status/roadmap section
 * - Implemented features (memtable, compaction, etc.)
 * - Planned features (Raft consensus)
 */

test.describe('Project Status Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section displaying project status/roadmap is present', async ({ page }) => {
    // Look for a project status section
    const statusSection = page.locator(
      'section[id*="status"], section[id*="roadmap"], section.project-status, ' +
      '[data-testid="project-status"], .status-section, .roadmap-section, ' +
      'section:has-text("Project Status"), section:has-text("Roadmap")'
    );

    // Verify the status section exists and is visible
    await expect(statusSection.first()).toBeVisible();

    // Check that the section contains meaningful content
    const sectionContent = await statusSection.first().textContent();
    expect(sectionContent?.trim().length).toBeGreaterThan(0);

    // Verify there's a heading related to status or roadmap
    const statusHeading = page.locator(
      'h2:has-text("Status"), h2:has-text("Roadmap"), h3:has-text("Status"), h3:has-text("Roadmap"), ' +
      'h2:has-text("Project Status"), h3:has-text("Project Status")'
    );
    await expect(statusHeading.first()).toBeVisible();
  });

  test('TC2: Implemented features (memtable, compaction, etc.) are indicated', async ({ page }) => {
    // Find the project status section or the page content
    const pageContent = await page.content();

    // Check for implemented features keywords
    const hasMemtable = /memtable/i.test(pageContent);
    const hasCompaction = /compaction/i.test(pageContent);
    const hasSSTable = /sstable/i.test(pageContent);
    const hasSkipList = /skip\s*list/i.test(pageContent);
    const hasWAL = /wal|write.?ahead.?log/i.test(pageContent);
    const hasNetworking = /tokio|async|networking/i.test(pageContent);

    // At least some of the implemented features should be mentioned
    const implementedFeaturesFound = [hasMemtable, hasCompaction, hasSSTable, hasSkipList, hasWAL, hasNetworking]
      .filter(Boolean).length;

    expect(implementedFeaturesFound).toBeGreaterThanOrEqual(2);

    // Look for "implemented" indicator or check mark patterns
    const statusSection = page.locator(
      'section[id*="status"], section[id*="roadmap"], section.project-status, ' +
      '.status-section, .roadmap-section, ' +
      'section:has-text("Project Status"), section:has-text("Roadmap")'
    ).first();

    await expect(statusSection).toBeVisible();

    const sectionContent = await statusSection.textContent();

    // The section should indicate implemented features
    const hasImplementedIndicator =
      /implemented|complete|done|✓|✔|available/i.test(sectionContent || '');

    expect(hasImplementedIndicator).toBeTruthy();
  });

  test('TC3: Planned features (Raft consensus) are indicated as future work', async ({ page }) => {
    // Find the project status section
    const statusSection = page.locator(
      'section[id*="status"], section[id*="roadmap"], section.project-status, ' +
      '.status-section, .roadmap-section, ' +
      'section:has-text("Project Status"), section:has-text("Roadmap")'
    ).first();

    await expect(statusSection).toBeVisible();

    const sectionContent = await statusSection.textContent();

    // Check for Raft consensus as a planned feature
    const hasRaft = /raft/i.test(sectionContent || '');
    const hasConsensus = /consensus/i.test(sectionContent || '');
    const hasDistributed = /distributed/i.test(sectionContent || '');

    // At least one of these should be present
    expect(hasRaft || hasConsensus || hasDistributed).toBeTruthy();

    // Look for "planned" or "future" indicators
    const hasPlannedIndicator =
      /planned|future|coming|roadmap|upcoming|in progress|todo|not yet/i.test(sectionContent || '');

    expect(hasPlannedIndicator).toBeTruthy();
  });
});
