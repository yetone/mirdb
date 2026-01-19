// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Project Status and Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Check for project status section
  test('TC1: Section with feature checklist or roadmap is present', async ({ page }) => {
    // Navigate to the project status section
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Verify section has a heading
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toContain('status');

    // Verify there's status content
    const statusContent = statusSection.locator('.status-content');
    await expect(statusContent).toBeVisible();

    // Verify there are feature lists (implemented and planned)
    const implementedSection = statusSection.locator('.implemented');
    const plannedSection = statusSection.locator('.planned');

    await expect(implementedSection).toBeVisible();
    await expect(plannedSection).toBeVisible();
  });

  // Test Case 2: Verify completed features are marked
  test('TC2: Implemented features have visual checkmark or completed indicator', async ({ page }) => {
    // Navigate to the status section
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();

    // Find the implemented features list
    const implementedSection = statusSection.locator('.implemented');
    await expect(implementedSection).toBeVisible();

    // Get all list items in implemented features
    const implementedItems = implementedSection.locator('ul li');
    const count = await implementedItems.count();

    // Should have at least one implemented feature
    expect(count).toBeGreaterThan(0);

    // Each implemented feature should have a checkmark indicator
    for (let i = 0; i < count; i++) {
      const item = implementedItems.nth(i);
      const text = await item.textContent();

      // Check for checkmark symbol (✓ is &#10003;) or other completion indicators
      // The checkmark can be in various forms: ✓, ✔, ☑, [x], or text containing "done" or "complete"
      const hasCheckmark = text.includes('✓') ||
                          text.includes('✔') ||
                          text.includes('☑') ||
                          text.includes('[x]') ||
                          text.includes('\u2713') || // Unicode checkmark
                          text.includes('\u2714');   // Heavy checkmark

      expect(hasCheckmark).toBeTruthy();
    }
  });

  // Test Case 3: Verify Raft consensus is listed as planned
  test('TC3: Raft feature is listed and marked as planned/upcoming', async ({ page }) => {
    // Navigate to the status section
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();

    // Find the planned features section
    const plannedSection = statusSection.locator('.planned');
    await expect(plannedSection).toBeVisible();

    // Get all planned items
    const plannedItems = plannedSection.locator('ul li');
    const count = await plannedItems.count();

    // Should have at least one planned feature
    expect(count).toBeGreaterThan(0);

    // Find the Raft consensus feature
    let raftFound = false;
    for (let i = 0; i < count; i++) {
      const item = plannedItems.nth(i);
      const text = await item.textContent();

      if (text.toLowerCase().includes('raft')) {
        raftFound = true;

        // Verify it has a "planned" indicator (empty checkbox ☐ is &#9744;, or similar)
        // Common planned indicators: ☐, □, [ ], or lack of checkmark
        const hasPlannedIndicator = text.includes('☐') ||
                                    text.includes('□') ||
                                    text.includes('[ ]') ||
                                    text.includes('\u2610') || // Empty checkbox
                                    !text.includes('✓');       // No checkmark means planned

        expect(hasPlannedIndicator).toBeTruthy();
        break;
      }
    }

    expect(raftFound).toBeTruthy();
  });

  // Test Case 4: Verify visual distinction between completed and planned (E2E test)
  test('TC4: Clear visual difference between completed and planned features', async ({ page }) => {
    // Set viewport for consistent testing
    await page.setViewportSize({ width: 1280, height: 720 });

    // Navigate to the status section
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();

    // Get the implemented and planned sections
    const implementedSection = statusSection.locator('.implemented');
    const plannedSection = statusSection.locator('.planned');

    await expect(implementedSection).toBeVisible();
    await expect(plannedSection).toBeVisible();

    // Get text content from both sections
    const implementedText = await implementedSection.textContent();
    const plannedText = await plannedSection.textContent();

    // Verify that implemented features have checkmarks (✓)
    // and planned features have different indicators (☐ or no checkmark)
    const implementedHasCheckmarks = implementedText.includes('✓') ||
                                     implementedText.includes('✔') ||
                                     implementedText.includes('\u2713') ||
                                     implementedText.includes('\u2714');

    const plannedHasEmptyBoxes = plannedText.includes('☐') ||
                                 plannedText.includes('□') ||
                                 plannedText.includes('\u2610');

    // There should be a clear visual distinction
    expect(implementedHasCheckmarks).toBeTruthy();
    expect(plannedHasEmptyBoxes).toBeTruthy();

    // Verify they have different section headings
    const implementedHeading = implementedSection.locator('h3');
    const plannedHeading = plannedSection.locator('h3');

    await expect(implementedHeading).toBeVisible();
    await expect(plannedHeading).toBeVisible();

    const implementedHeadingText = await implementedHeading.textContent();
    const plannedHeadingText = await plannedHeading.textContent();

    // Headings should indicate the status
    expect(implementedHeadingText.toLowerCase()).toContain('implemented');
    expect(plannedHeadingText.toLowerCase()).toContain('planned');

    // Both sections should have lists
    const implementedList = implementedSection.locator('ul');
    const plannedList = plannedSection.locator('ul');

    await expect(implementedList).toBeVisible();
    await expect(plannedList).toBeVisible();
  });

  // Additional test: Scroll to status section accessibility
  test('Status section is accessible via scrolling', async ({ page }) => {
    // Set viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    // Scroll to status section
    await page.locator('#status').scrollIntoViewIfNeeded();

    // Verify status section is visible
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Verify the heading is visible
    const heading = page.locator('#status h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Project Status');
  });

  // Additional test: Verify semantic structure
  test('Status section has proper semantic HTML structure', async ({ page }) => {
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();

    // Should be a section element
    const tagName = await statusSection.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('section');

    // Should have h2 heading
    const h2 = statusSection.locator('h2');
    await expect(h2).toBeVisible();

    // Should have h3 subheadings for categories
    const h3s = statusSection.locator('h3');
    const h3Count = await h3s.count();
    expect(h3Count).toBeGreaterThanOrEqual(2);

    // Should use proper list markup
    const lists = statusSection.locator('ul');
    const listCount = await lists.count();
    expect(listCount).toBeGreaterThanOrEqual(2);
  });
});
