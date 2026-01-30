/**
 * Roadmap Section E2E Tests
 * Owner: Scenario 6 - Project Roadmap Section
 *
 * Tests:
 * - Completed features display
 * - Planned features display
 * - GitHub issues link functionality
 * - Visual distinction between completed and planned
 */

const { test, expect } = require('@playwright/test');

test.describe('Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Navigate to Roadmap section - Section displays heading with categorized feature lists
  test('displays heading "Project Status" or "Roadmap" with categorized feature lists', async ({ page }) => {
    // Navigate to roadmap section
    await page.locator('#roadmap').scrollIntoViewIfNeeded();

    // Verify section exists and is visible
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Verify section title contains "Roadmap" or "Project Status"
    const sectionTitle = page.locator('#roadmap-title');
    await expect(sectionTitle).toBeVisible();
    const titleText = await sectionTitle.textContent();
    expect(titleText?.match(/Roadmap|Project Status/)).toBeTruthy();

    // Verify there are categorized feature lists (completed and planned)
    const categories = page.locator('#roadmap .roadmap__category');
    await expect(categories).toHaveCount(2);

    // Verify "Completed Features" category exists
    const completedCategory = page.locator('#roadmap .roadmap__category-title:has-text("Completed")');
    await expect(completedCategory).toBeVisible();

    // Verify "Planned Features" category exists
    const plannedCategory = page.locator('#roadmap .roadmap__category-title:has-text("Planned")');
    await expect(plannedCategory).toBeVisible();
  });

  // Test Case 2: Verify completed features list
  test('shows checkmarks for completed features: Tokio networking, Memcached protocol, Memtable with skip list, Minor compaction, Major compaction', async ({ page }) => {
    await page.locator('#roadmap').scrollIntoViewIfNeeded();

    // Get all completed feature items
    const completedItems = page.locator('#roadmap .roadmap__item--completed');
    await expect(completedItems).toHaveCount(5);

    // Verify each completed feature is present with checkmark
    const featureTexts = [
      'Tokio-based async networking',
      'Memcached protocol support',
      'Memtable with skip list',
      'Minor compaction',
      'Major compaction'
    ];

    for (const featureText of featureTexts) {
      const featureItem = page.locator('#roadmap .roadmap__item--completed', { hasText: featureText });
      await expect(featureItem).toBeVisible();

      // Verify the checkmark icon is present
      const checkmark = featureItem.locator('.roadmap__check svg');
      await expect(checkmark).toBeVisible();
    }
  });

  // Test Case 3: Verify planned features list - Shows Raft consensus as planned/in-progress
  test('shows Raft consensus as planned/in-progress feature', async ({ page }) => {
    await page.locator('#roadmap').scrollIntoViewIfNeeded();

    // Find the planned feature item containing "Raft consensus"
    const raftFeature = page.locator('#roadmap .roadmap__item--planned', { hasText: 'Raft consensus' });
    await expect(raftFeature).toBeVisible();

    // Verify it has the planned icon (not a checkmark)
    const plannedIcon = raftFeature.locator('.roadmap__planned-icon');
    await expect(plannedIcon).toBeVisible();
  });

  // Test Case 4: Click contribution/issues link - Opens GitHub issues page filtered by labels
  test('contribution links open GitHub issues page filtered by "good first issue" or "help wanted" labels', async ({ page }) => {
    await page.locator('#roadmap').scrollIntoViewIfNeeded();

    // Check for "Good First Issues" link
    const goodFirstIssuesLink = page.locator('a.roadmap__contribution-link', { hasText: 'Good First Issues' });
    await expect(goodFirstIssuesLink).toBeVisible();

    // Verify the href contains the correct GitHub issues URL with label filter
    const goodFirstIssuesHref = await goodFirstIssuesLink.getAttribute('href');
    expect(goodFirstIssuesHref).toContain('github.com/yetone/mirdb/issues');
    expect(goodFirstIssuesHref).toContain('good+first+issue');

    // Check for "Help Wanted" link
    const helpWantedLink = page.locator('a.roadmap__contribution-link', { hasText: 'Help Wanted' });
    await expect(helpWantedLink).toBeVisible();

    // Verify the href contains the correct GitHub issues URL with label filter
    const helpWantedHref = await helpWantedLink.getAttribute('href');
    expect(helpWantedHref).toContain('github.com/yetone/mirdb/issues');
    expect(helpWantedHref).toContain('help+wanted');

    // Verify links open in new tab
    await expect(goodFirstIssuesLink).toHaveAttribute('target', '_blank');
    await expect(helpWantedLink).toHaveAttribute('target', '_blank');

    // Verify links have rel="noopener noreferrer" for security
    await expect(goodFirstIssuesLink).toHaveAttribute('rel', 'noopener noreferrer');
    await expect(helpWantedLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  // Test Case 5: Verify visual distinction between completed and planned
  test('completed items have checkmarks/green indicators, planned items are visually distinct', async ({ page }) => {
    await page.locator('#roadmap').scrollIntoViewIfNeeded();

    // Check completed item styling
    const completedItem = page.locator('#roadmap .roadmap__item--completed').first();
    await expect(completedItem).toBeVisible();

    // Verify completed items have a visible checkmark with teal/green color
    const checkmark = completedItem.locator('.roadmap__check');
    await expect(checkmark).toBeVisible();

    const checkmarkBgColor = await checkmark.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Should be teal color (rgb(78, 205, 196) or similar)
    expect(checkmarkBgColor).toMatch(/rgb\(78,\s*205,\s*196\)|#4ECDC4/i);

    // Check planned item styling
    const plannedItem = page.locator('#roadmap .roadmap__item--planned').first();
    await expect(plannedItem).toBeVisible();

    // Verify planned items have a different visual indicator (border, not filled)
    const plannedIcon = plannedItem.locator('.roadmap__planned-icon');
    await expect(plannedIcon).toBeVisible();

    const plannedIconBorderColor = await plannedIcon.evaluate((el) => {
      return window.getComputedStyle(el).borderColor;
    });
    // Should be rust/orange color (rgb(222, 165, 132) or similar)
    expect(plannedIconBorderColor).toMatch(/rgb\(222,\s*165,\s*132\)|#DEA584/i);

    // Verify planned icon background is transparent (not filled like completed)
    const plannedIconBgColor = await plannedIcon.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Background should be transparent
    expect(plannedIconBgColor).toMatch(/rgba\(0,\s*0,\s*0,\s*0\)|transparent/i);
  });

  // Additional test: Verify accessibility of contribution links
  test('contribution links have accessible aria-labels', async ({ page }) => {
    await page.locator('#roadmap').scrollIntoViewIfNeeded();

    // Check "Good First Issues" link has aria-label
    const goodFirstIssuesLink = page.locator('a.roadmap__contribution-link--primary');
    await expect(goodFirstIssuesLink).toHaveAttribute('aria-label', 'View good first issues on GitHub');

    // Check "Help Wanted" link has aria-label
    const helpWantedLink = page.locator('a.roadmap__contribution-link--secondary');
    await expect(helpWantedLink).toHaveAttribute('aria-label', 'View help wanted issues on GitHub');
  });

  // Additional test: Verify responsive layout
  test('displays properly on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.locator('#roadmap').scrollIntoViewIfNeeded();

    // Verify section is visible
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Verify categories are stacked vertically on mobile
    const content = page.locator('#roadmap .roadmap__content');
    const gridStyle = await content.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should be single column on mobile
    const columns = gridStyle.split(' ').filter(c => c !== '');
    expect(columns.length).toBe(1);
  });

  // Additional test: Verify two-column layout on tablet
  test('displays two-column grid on tablet', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.locator('#roadmap').scrollIntoViewIfNeeded();

    const content = page.locator('#roadmap .roadmap__content');
    const gridStyle = await content.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 2 columns on tablet
    const columns = gridStyle.split(' ').filter(c => c !== '');
    expect(columns.length).toBe(2);
  });
});
