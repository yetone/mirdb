// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status and Roadmap Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Page contains status, roadmap, or 'coming soon' section
  test('TC1: page contains project status/roadmap section', async ({ page }) => {
    // Check that the roadmap section exists
    const roadmapSection = page.locator('#roadmap, .roadmap, [aria-labelledby="roadmap-heading"]');
    await expect(roadmapSection).toBeVisible();

    // Check for heading that indicates status/roadmap content
    const heading = roadmapSection.locator('h2');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    const hasStatusOrRoadmap =
      headingText?.toLowerCase().includes('status') ||
      headingText?.toLowerCase().includes('roadmap');
    expect(hasStatusOrRoadmap).toBe(true);

    // Also verify there's a "coming soon" indicator in the section
    const comingSoonHeading = roadmapSection.locator('.coming-soon-heading, h3:has-text("Coming Soon")');
    await expect(comingSoonHeading).toBeVisible();
  });

  // Test Case 2: Lists implemented features like protocol support, persistence
  test('TC2: lists implemented features including protocol support and persistence', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check for implemented features section
    const implementedSection = roadmapSection.locator('.implemented-heading');
    await expect(implementedSection).toBeVisible();

    // Verify the page content includes key implemented features
    const pageContent = await page.content();
    const pageText = pageContent.toLowerCase();

    // Check for memcached protocol support mention
    expect(pageText).toContain('memcached');
    expect(pageText).toContain('protocol');

    // Check for persistence mention
    expect(pageText).toContain('persist');

    // Verify implemented items are marked with checkmarks or similar
    const implementedItems = roadmapSection.locator('.roadmap-item.implemented');
    const implementedCount = await implementedItems.count();
    expect(implementedCount).toBeGreaterThanOrEqual(2);

    // Verify specific implemented features exist
    const memcachedProtocol = roadmapSection.locator('strong:has-text("Memcached Protocol")');
    await expect(memcachedProtocol).toBeVisible();

    const persistentStorage = roadmapSection.locator('strong:has-text("Persistent Storage")');
    await expect(persistentStorage).toBeVisible();

    // Check for compaction feature as mentioned in scenario context
    const compaction = roadmapSection.locator('strong:has-text("Compaction")');
    await expect(compaction).toBeVisible();
  });

  // Test Case 3: Raft consensus mentioned as planned/roadmap feature
  test('TC3: Raft consensus mentioned as planned/roadmap feature', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Look for Raft consensus in the coming soon/planned section
    const comingSoonSection = roadmapSection.locator('.coming-soon-list');
    await expect(comingSoonSection).toBeVisible();

    // Verify Raft consensus is mentioned - using strong tag to be specific
    const raftConsensus = roadmapSection.locator('strong:has-text("Raft Consensus")');
    await expect(raftConsensus).toBeVisible();

    // Verify Raft is in the planned/coming soon section (not implemented)
    const plannedRaft = roadmapSection.locator('.roadmap-item.planned:has-text("Raft")');
    await expect(plannedRaft).toBeVisible();

    // Verify the description mentions distributed consensus or similar
    const raftDescription = plannedRaft.locator('p');
    const descText = await raftDescription.textContent();
    expect(descText?.toLowerCase()).toMatch(/consensus|distributed|availability|fault.?tolerance/);
  });

  // Additional test: Verify semantic structure of roadmap section
  test('roadmap section has proper semantic structure', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check for proper heading hierarchy
    const h2 = roadmapSection.locator('h2');
    await expect(h2).toBeVisible();

    const h3Elements = roadmapSection.locator('h3');
    const h3Count = await h3Elements.count();
    expect(h3Count).toBeGreaterThanOrEqual(2); // Implemented and Coming Soon

    // Verify lists are using proper ul/li structure
    const lists = roadmapSection.locator('ul.roadmap-list');
    const listCount = await lists.count();
    expect(listCount).toBeGreaterThanOrEqual(2);

    // Check accessibility - section should have aria-labelledby
    const ariaLabel = await roadmapSection.getAttribute('aria-labelledby');
    expect(ariaLabel).toBeTruthy();
  });
});
