/**
 * Project Status Section E2E Tests
 * Owner: Scenario 6 - Project Status Display
 *
 * Test cases:
 * - Project status section visible with checklist or timeline
 * - Implemented features have visual checkmark or completed indicator
 * - Planned features visually distinguished from implemented ones
 */

const { test, expect } = require('@playwright/test');

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Project status section is visible with checklist format', async ({ page }) => {
    // Locate the project status section
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Verify section has proper aria-labelledby for accessibility
    await expect(statusSection).toHaveAttribute('aria-labelledby', 'status-headline');

    // Verify headline exists
    const headline = page.locator('#status-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText(/status|roadmap/i);

    // Verify checklist or timeline format exists
    const statusList = page.locator('[data-testid="status-list"]');
    await expect(statusList).toBeVisible();

    // Verify there are status items present (at least 2)
    const statusItems = page.locator('[data-testid^="status-item"]');
    const count = await statusItems.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test('TC2: Implemented features have visual checkmark or completed indicator', async ({ page }) => {
    // Locate implemented features
    const implementedFeatures = page.locator('[data-testid="status-item-implemented"]');
    const implCount = await implementedFeatures.count();
    expect(implCount).toBeGreaterThanOrEqual(1);

    // Each implemented feature should have a checkmark indicator
    const firstImplemented = implementedFeatures.first();
    await expect(firstImplemented).toBeVisible();

    // Check for visual indicator (checkmark icon or class)
    const checkIndicator = firstImplemented.locator('.status-item__indicator--completed').first();
    await expect(checkIndicator).toBeVisible();

    // Verify the indicator has appropriate styling (green color or similar)
    const indicatorStyles = await checkIndicator.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        color: styles.color,
        backgroundColor: styles.backgroundColor
      };
    });

    // Should have some green-ish color (accent color) for completed items
    // Check that it's not just default black/white
    expect(indicatorStyles.color).not.toBe('rgb(0, 0, 0)');
  });

  test('TC3: Planned features are visually distinguished from implemented ones', async ({ page }) => {
    // Locate planned features
    const plannedFeatures = page.locator('[data-testid="status-item-planned"]');
    const plannedCount = await plannedFeatures.count();
    expect(plannedCount).toBeGreaterThanOrEqual(1);

    // Locate implemented features for comparison
    const implementedFeatures = page.locator('[data-testid="status-item-implemented"]');
    const implCount = await implementedFeatures.count();
    expect(implCount).toBeGreaterThanOrEqual(1);

    // Get visual appearance of implemented vs planned
    const implementedIndicator = implementedFeatures.first().locator('.status-item__indicator');
    const plannedIndicator = plannedFeatures.first().locator('.status-item__indicator');

    await expect(implementedIndicator).toBeVisible();
    await expect(plannedIndicator).toBeVisible();

    // Verify they have different visual treatments
    const implementedClass = await implementedIndicator.getAttribute('class');
    const plannedClass = await plannedIndicator.getAttribute('class');

    // Should have different modifier classes
    expect(implementedClass).toContain('completed');
    expect(plannedClass).toContain('planned');
  });

  test('TC4: Status matches actual MirDB implementation state', async ({ page }) => {
    // Verify implemented features match MirDB's actual state
    const implementedFeatures = page.locator('[data-testid="status-item-implemented"]');

    // Check for expected implemented features
    const expectedImplemented = [
      /memcached.*protocol/i,
      /memtable.*skip.*list/i,
      /minor.*compaction/i,
      /major.*compaction/i
    ];

    for (const pattern of expectedImplemented) {
      const matchingFeature = implementedFeatures.filter({ hasText: pattern });
      const matchCount = await matchingFeature.count();
      expect(matchCount).toBeGreaterThanOrEqual(1);
    }

    // Verify planned features
    const plannedFeatures = page.locator('[data-testid="status-item-planned"]');

    // Raft consensus should be marked as planned
    const raftFeature = plannedFeatures.filter({ hasText: /raft|consensus|distributed/i });
    await expect(raftFeature).toHaveCount(1);
  });

  test('Status section has proper heading hierarchy', async ({ page }) => {
    const headline = page.locator('#status-headline');
    const tagName = await headline.evaluate((el) => el.tagName.toLowerCase());
    expect(['h2', 'h3']).toContain(tagName);
  });

  test('Status items are keyboard accessible', async ({ page }) => {
    // Tab through status items to verify accessibility
    const statusSection = page.locator('#status');
    await statusSection.scrollIntoViewIfNeeded();

    // Verify the section can receive focus
    const focusableElements = page.locator('#status a, #status button, #status [tabindex="0"]');
    const count = await focusableElements.count();

    // If there are focusable elements, ensure they work
    if (count > 0) {
      await focusableElements.first().focus();
      await expect(focusableElements.first()).toBeFocused();
    }
  });
});
