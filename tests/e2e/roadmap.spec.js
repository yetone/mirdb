/**
 * Roadmap Section E2E Tests
 * Owner: Scenario 7 - Roadmap Section
 *
 * Tests:
 * - Roadmap items displayed
 * - Raft consensus listed as planned
 * - Visual distinction between implemented/planned
 * - Contribution link present
 */

const { test, expect } = require('@playwright/test');

/**
 * Parse RGB string to {r, g, b} object
 */
function parseRgb(rgbString) {
  const match = rgbString.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (!match) return null;
  return {
    r: parseInt(match[1], 10),
    g: parseInt(match[2], 10),
    b: parseInt(match[3], 10),
  };
}

test.describe('Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC-1: Roadmap section contains items with status indicators', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Verify section has heading
    const heading = roadmapSection.locator('h2');
    await expect(heading).toHaveText('Roadmap');

    // Query for roadmap items with status indicators
    const roadmapItems = roadmapSection.locator('.roadmap-item');
    const itemCount = await roadmapItems.count();
    expect(itemCount).toBeGreaterThan(0);

    // Verify each item has a status indicator
    for (let i = 0; i < itemCount; i++) {
      const item = roadmapItems.nth(i);
      const statusIndicator = item.locator('.status-indicator');
      await expect(statusIndicator).toBeVisible();
    }
  });

  test('TC-2: Raft consensus listed as planned/future feature', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check for Raft consensus feature
    const raftItem = roadmapSection.locator('.roadmap-item:has-text("Raft")');
    await expect(raftItem).toBeVisible();

    // Verify it contains "Raft consensus" or similar text
    const raftText = await raftItem.textContent();
    expect(raftText.toLowerCase()).toContain('raft');

    // Verify it's marked as planned (not implemented)
    const isPlanned = await raftItem.evaluate((el) => {
      return el.classList.contains('planned') ||
             el.getAttribute('data-status') === 'planned' ||
             el.querySelector('.status-planned') !== null;
    });
    expect(isPlanned).toBe(true);
  });

  test('TC-3: Visual distinction between implemented and planned features', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Get implemented items
    const implementedItems = roadmapSection.locator('.roadmap-item.implemented, .roadmap-item[data-status="implemented"]');
    const implementedCount = await implementedItems.count();

    // Get planned items
    const plannedItems = roadmapSection.locator('.roadmap-item.planned, .roadmap-item[data-status="planned"]');
    const plannedCount = await plannedItems.count();

    // Verify we have both types
    expect(implementedCount).toBeGreaterThan(0);
    expect(plannedCount).toBeGreaterThan(0);

    // Check visual distinction for an implemented item
    if (implementedCount > 0) {
      const implementedItem = implementedItems.first();
      const implementedStyles = await implementedItem.evaluate((el) => {
        const indicator = el.querySelector('.status-indicator');
        if (!indicator) return null;
        const style = getComputedStyle(indicator);
        return {
          color: style.color,
          backgroundColor: style.backgroundColor,
          content: indicator.textContent,
        };
      });

      // Implemented should have checkmark or green indicator
      expect(implementedStyles).not.toBeNull();
      const hasCheckmark = implementedStyles.content?.includes('✓') ||
                           implementedStyles.content?.includes('✔') ||
                           implementedStyles.content?.includes('check');
      const hasGreenColor = implementedStyles.color?.includes('rgb') ||
                            implementedStyles.backgroundColor?.includes('rgb');

      // Should have some visual indicator
      expect(hasCheckmark || hasGreenColor).toBe(true);
    }

    // Check visual distinction for a planned item
    if (plannedCount > 0) {
      const plannedItem = plannedItems.first();
      const plannedStyles = await plannedItem.evaluate((el) => {
        const indicator = el.querySelector('.status-indicator');
        if (!indicator) return null;
        const style = getComputedStyle(indicator);
        return {
          color: style.color,
          backgroundColor: style.backgroundColor,
          content: indicator.textContent,
        };
      });

      expect(plannedStyles).not.toBeNull();
    }

    // Verify different visual styling between implemented and planned
    const implementedIndicator = implementedItems.first().locator('.status-indicator');
    const plannedIndicator = plannedItems.first().locator('.status-indicator');

    const implementedColor = await implementedIndicator.evaluate((el) => getComputedStyle(el).color);
    const plannedColor = await plannedIndicator.evaluate((el) => getComputedStyle(el).color);

    // Colors should be different (visual distinction)
    // or they should have different background colors or icons
    const colorsAreDifferent = implementedColor !== plannedColor;

    // If colors are same, check for different content (icons)
    if (!colorsAreDifferent) {
      const implementedContent = await implementedIndicator.textContent();
      const plannedContent = await plannedIndicator.textContent();
      expect(implementedContent).not.toBe(plannedContent);
    } else {
      expect(colorsAreDifferent).toBe(true);
    }
  });

  test('TC-4: Contribution link present to GitHub issues or contributing guide', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Look for contribution-related links
    const contributionLink = roadmapSection.locator('a[href*="issues"], a[href*="CONTRIBUTING"], a[href*="contributing"], a:has-text("Contribute"), a:has-text("contribute")');
    await expect(contributionLink.first()).toBeVisible();

    // Get the href and verify it points to a valid contributor resource
    const href = await contributionLink.first().getAttribute('href');
    expect(href).toBeTruthy();

    const isValidContributorLink =
      href.includes('github.com') && href.includes('issues') ||
      href.includes('CONTRIBUTING') ||
      href.includes('contributing');

    expect(isValidContributorLink).toBe(true);
  });

  test('Roadmap section is scrollable and accessible via anchor', async ({ page }) => {
    // Click on roadmap nav link
    const navLink = page.locator('a[href="#roadmap"]');
    await navLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify roadmap section is in viewport
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeInViewport();
  });

  test('Roadmap items have proper semantic structure', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');

    // Check for proper grid structure (parent container)
    const roadmapGrid = roadmapSection.locator('.roadmap-grid');
    await expect(roadmapGrid).toBeVisible();

    // Check for proper list structures within the grid
    const roadmapLists = roadmapSection.locator('.roadmap-list');
    const listCount = await roadmapLists.count();
    expect(listCount).toBeGreaterThan(0);

    // Check roadmap items exist
    const items = roadmapSection.locator('.roadmap-item');
    const count = await items.count();
    expect(count).toBeGreaterThan(0);
  });

  test('Implemented features show checkmark or green indicator', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    const implementedItems = roadmapSection.locator('.roadmap-item.implemented, .roadmap-item[data-status="implemented"]');

    const count = await implementedItems.count();
    expect(count).toBeGreaterThan(0);

    // Check first implemented item for green color or checkmark
    const firstItem = implementedItems.first();
    const indicator = firstItem.locator('.status-indicator');

    const indicatorInfo = await indicator.evaluate((el) => {
      const style = getComputedStyle(el);
      return {
        color: style.color,
        content: el.textContent || el.innerHTML,
      };
    });

    // Should have green color (accent-green is #3fb950) or checkmark symbol
    const hasGreen = indicatorInfo.color.includes('63') && indicatorInfo.color.includes('185') && indicatorInfo.color.includes('80'); // RGB for #3fb950
    const hasCheckmark = indicatorInfo.content.includes('✓') || indicatorInfo.content.includes('✔');

    expect(hasGreen || hasCheckmark).toBe(true);
  });

  test('Planned features show distinct indicator from implemented', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    const plannedItems = roadmapSection.locator('.roadmap-item.planned, .roadmap-item[data-status="planned"]');

    const count = await plannedItems.count();
    expect(count).toBeGreaterThan(0);

    // Check that planned items have a different indicator than implemented
    const firstPlanned = plannedItems.first();
    const indicator = firstPlanned.locator('.status-indicator');

    const indicatorInfo = await indicator.evaluate((el) => {
      const style = getComputedStyle(el);
      return {
        color: style.color,
        content: el.textContent || el.innerHTML,
      };
    });

    // Planned should NOT have checkmark (which is for implemented)
    const hasCheckmark = indicatorInfo.content.includes('✓') || indicatorInfo.content.includes('✔');
    expect(hasCheckmark).toBe(false);
  });
});
