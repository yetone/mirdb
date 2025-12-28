// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Value Propositions Section
 * Scenario: Verify the three key value propositions (persistence, memcached compatibility, Rust)
 * are clearly displayed
 */

test.describe('Value Propositions Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Persistent Storage value proposition is displayed with description
   * Input: Inspect value propositions section
   * Expected: Section displays 'Persistent Storage' value proposition with description
   */
  test('TC1: Persistent Storage value proposition is displayed with description', async ({ page }) => {
    // Verify the value propositions section exists
    const valuePropsSection = page.locator('[data-testid="value-propositions-section"]');
    await expect(valuePropsSection).toBeVisible();

    // Verify the Persistent Storage value proposition card exists
    const persistentStorageProp = page.locator('[data-testid="value-prop-persistent-storage"]');
    await expect(persistentStorageProp).toBeVisible();

    // Verify the title
    const title = page.locator('[data-testid="value-prop-persistent-storage-title"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Persistent Storage');

    // Verify the description exists and contains relevant content
    const description = page.locator('[data-testid="value-prop-persistent-storage-description"]');
    await expect(description).toBeVisible();

    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toContain('persists');
    expect(descriptionText.toLowerCase()).toContain('disk');
  });

  /**
   * Test Case 2: Memcached Compatible value proposition is displayed with description
   * Input: Inspect value propositions section
   * Expected: Section displays 'Memcached Compatible' value proposition with description
   */
  test('TC2: Memcached Compatible value proposition is displayed with description', async ({ page }) => {
    // Verify the value propositions section exists
    const valuePropsSection = page.locator('[data-testid="value-propositions-section"]');
    await expect(valuePropsSection).toBeVisible();

    // Verify the Memcached Compatible value proposition card exists
    const memcachedProp = page.locator('[data-testid="value-prop-memcached-compatible"]');
    await expect(memcachedProp).toBeVisible();

    // Verify the title
    const title = page.locator('[data-testid="value-prop-memcached-compatible-title"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Memcached Compatible');

    // Verify the description exists and contains relevant content
    const description = page.locator('[data-testid="value-prop-memcached-compatible-description"]');
    await expect(description).toBeVisible();

    const descriptionText = await description.textContent();
    expect(descriptionText.toLowerCase()).toContain('memcached');
    expect(descriptionText.toLowerCase()).toContain('protocol');
  });

  /**
   * Test Case 3: Written in Rust value proposition is displayed with description
   * Input: Inspect value propositions section
   * Expected: Section displays 'Written in Rust' value proposition with description
   */
  test('TC3: Written in Rust value proposition is displayed with description', async ({ page }) => {
    // Verify the value propositions section exists
    const valuePropsSection = page.locator('[data-testid="value-propositions-section"]');
    await expect(valuePropsSection).toBeVisible();

    // Verify the Written in Rust value proposition card exists
    const rustProp = page.locator('[data-testid="value-prop-rust"]');
    await expect(rustProp).toBeVisible();

    // Verify the title
    const title = page.locator('[data-testid="value-prop-rust-title"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Written in Rust');

    // Verify the description exists and contains relevant content
    const description = page.locator('[data-testid="value-prop-rust-description"]');
    await expect(description).toBeVisible();

    const descriptionText = await description.textContent();
    expect(descriptionText).toContain('Rust');
    expect(descriptionText.toLowerCase()).toContain('performance');
  });

  /**
   * Test Case 4: Value propositions are displayed in a 3-column layout on desktop
   * Input: Check value proposition layout
   * Expected: Value propositions are displayed in a 3-column layout on desktop
   */
  test('TC4: Value propositions are displayed in a 3-column layout on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1200, height: 800 });

    // Get the value propositions grid
    const grid = page.locator('[data-testid="value-props-grid"]');
    await expect(grid).toBeVisible();

    // Verify there are exactly 3 value proposition cards
    const valueProps = page.locator('.value-prop-card');
    await expect(valueProps).toHaveCount(3);

    // Verify the grid is using CSS grid with 3 columns
    const gridStyles = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });

    // Check it's using grid layout
    expect(gridStyles.display).toBe('grid');

    // Check it has 3 columns (gridTemplateColumns will show the actual column widths)
    // When set to repeat(3, 1fr), the computed value shows the actual widths
    const columnCount = gridStyles.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(3);
  });
});
