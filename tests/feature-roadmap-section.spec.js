// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Feature Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Completed features list shows all expected items', async ({ page }) => {
    // Scroll to the roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();

    // Verify the roadmap section is visible
    await expect(roadmapSection).toBeVisible();

    // Find the completed features list
    const completedList = page.locator('.roadmap-list.completed');
    await expect(completedList).toBeVisible();

    // Get all completed items
    const completedItems = completedList.locator('li');

    // Verify there are 4 completed items
    await expect(completedItems).toHaveCount(4);

    // Verify each expected completed feature is present
    await expect(completedList).toContainText('Tokio with memcached protocol');
    await expect(completedList).toContainText('Memtable with skiplist');
    await expect(completedList).toContainText('Minor compaction');
    await expect(completedList).toContainText('Major compaction');
  });

  test('TC2: Planned features list shows Raft consensus support', async ({ page }) => {
    // Scroll to the roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();

    // Verify the roadmap section is visible
    await expect(roadmapSection).toBeVisible();

    // Find the planned features list
    const plannedList = page.locator('.roadmap-list.planned');
    await expect(plannedList).toBeVisible();

    // Get all planned items
    const plannedItems = plannedList.locator('li');

    // Verify there is 1 planned item
    await expect(plannedItems).toHaveCount(1);

    // Verify the expected planned feature is present
    await expect(plannedList).toContainText('Raft consensus support');
  });

  test('TC3: Completed items have green visual indicator (checkmark style)', async ({ page }) => {
    // Scroll to the roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();

    // Find the completed list items
    const completedList = page.locator('.roadmap-list.completed');
    await expect(completedList).toBeVisible();

    const firstCompletedItem = completedList.locator('li').first();

    // Get the computed styles for the ::before pseudo-element
    // We verify the completed class has the green color by checking computed styles
    const bgColor = await firstCompletedItem.evaluate((el) => {
      const styles = window.getComputedStyle(el, '::before');
      return styles.backgroundColor;
    });

    // The completed items should have a green background (--color-success: #22c55e)
    // RGB conversion of #22c55e is rgb(34, 197, 94)
    expect(bgColor).toBe('rgb(34, 197, 94)');
  });

  test('TC3: Planned items have gray visual indicator (different from completed)', async ({ page }) => {
    // Scroll to the roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();

    // Find the planned list items
    const plannedList = page.locator('.roadmap-list.planned');
    await expect(plannedList).toBeVisible();

    const firstPlannedItem = plannedList.locator('li').first();

    // Get the computed styles for the ::before pseudo-element
    const bgColor = await firstPlannedItem.evaluate((el) => {
      const styles = window.getComputedStyle(el, '::before');
      return styles.backgroundColor;
    });

    // The planned items should have a gray background (--color-secondary: #64748b)
    // RGB conversion of #64748b is rgb(100, 116, 139)
    expect(bgColor).toBe('rgb(100, 116, 139)');
  });

  test('TC3: Completed and planned indicators have different colors', async ({ page }) => {
    // Scroll to the roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();

    // Get completed item indicator color
    const completedItem = page.locator('.roadmap-list.completed li').first();
    const completedBgColor = await completedItem.evaluate((el) => {
      const styles = window.getComputedStyle(el, '::before');
      return styles.backgroundColor;
    });

    // Get planned item indicator color
    const plannedItem = page.locator('.roadmap-list.planned li').first();
    const plannedBgColor = await plannedItem.evaluate((el) => {
      const styles = window.getComputedStyle(el, '::before');
      return styles.backgroundColor;
    });

    // Verify the colors are different
    expect(completedBgColor).not.toBe(plannedBgColor);

    // Verify completed is green (success color)
    expect(completedBgColor).toBe('rgb(34, 197, 94)');

    // Verify planned is gray (secondary color)
    expect(plannedBgColor).toBe('rgb(100, 116, 139)');
  });

  test('Roadmap section has proper heading structure', async ({ page }) => {
    // Scroll to the roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();

    // Verify the main heading
    const mainHeading = page.locator('#roadmap-title');
    await expect(mainHeading).toBeVisible();
    await expect(mainHeading).toHaveText('Roadmap');

    // Verify subsection headings
    const completedHeading = page.locator('.roadmap-section h3:has-text("Completed")');
    await expect(completedHeading).toBeVisible();

    const plannedHeading = page.locator('.roadmap-section h3:has-text("Planned")');
    await expect(plannedHeading).toBeVisible();
  });

  test('Roadmap section has proper accessibility attributes', async ({ page }) => {
    // Verify the roadmap section has proper aria-labelledby
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toHaveAttribute('aria-labelledby', 'roadmap-title');

    // Verify the section uses semantic HTML with lists
    const completedList = page.locator('.roadmap-list.completed');
    const completedListTag = await completedList.evaluate(el => el.tagName.toLowerCase());
    expect(completedListTag).toBe('ul');

    const plannedList = page.locator('.roadmap-list.planned');
    const plannedListTag = await plannedList.evaluate(el => el.tagName.toLowerCase());
    expect(plannedListTag).toBe('ul');
  });

  test('Roadmap section is responsive with grid layout', async ({ page }) => {
    // Scroll to the roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();

    // Verify the roadmap-content uses grid layout
    const roadmapContent = page.locator('.roadmap-content');
    const display = await roadmapContent.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(display).toBe('grid');
  });
});
