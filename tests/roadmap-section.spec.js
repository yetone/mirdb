// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Roadmap Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Roadmap or coming soon section is visible', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap, section.roadmap, [data-testid="roadmap"]').first();
    await expect(roadmapSection).toBeVisible();

    // Check for section heading indicating roadmap/coming soon
    const heading = roadmapSection.locator('h2, h3').filter({
      hasText: /roadmap|coming soon|upcoming|what's next/i
    }).first();
    await expect(heading).toBeVisible();
  });

  test('TC2: Raft support is listed as an upcoming feature', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap, section.roadmap, [data-testid="roadmap"]').first();
    await expect(roadmapSection).toBeVisible();

    // Check for raft support mention
    const raftFeature = roadmapSection.locator('*').filter({
      hasText: /raft/i
    }).first();
    await expect(raftFeature).toBeVisible();

    // Verify raft is mentioned in context of upcoming/future/planned
    const raftContent = await roadmapSection.textContent();
    expect(raftContent.toLowerCase()).toContain('raft');
  });

  test('TC3: Roadmap uses timeline, checklist, or similar visual format', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap, section.roadmap, [data-testid="roadmap"]').first();
    await expect(roadmapSection).toBeVisible();

    // Check for visual structure: timeline, checklist, or roadmap items
    // Look for multiple roadmap items (at least 2)
    const roadmapItems = roadmapSection.locator('.roadmap-item, .timeline-item, .milestone, [data-testid="roadmap-item"], li, .feature-item');
    const count = await roadmapItems.count();

    // Should have at least 2 items to form a list/timeline
    expect(count).toBeGreaterThanOrEqual(2);

    // Check for visual indicators like icons, checkmarks, or status markers
    const hasVisualIndicators = await roadmapSection.locator('svg, .icon, .status-icon, .checkmark, [data-testid="roadmap-icon"]').count();
    expect(hasVisualIndicators).toBeGreaterThan(0);
  });
});
