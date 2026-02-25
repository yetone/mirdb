/**
 * What is MirDB Section E2E Tests
 * Owner: Scenario 2 - What is MirDB Section
 *
 * Test cases:
 * - Section contains heading with "What is MirDB" text
 * - Content mentions "SSTable" or "Sorted String Table"
 * - Content mentions "LSM tree" or "Log-Structured Merge-tree"
 */

const { test, expect } = require('@playwright/test');

test.describe('What is MirDB Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
  });

  test('Section contains heading with "What is MirDB" text', async ({ page }) => {
    // Test case 1: Verify section heading
    const heading = page.locator('#what-is h2, #what-is-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('What is MirDB');
  });

  test('Content mentions SSTable or Sorted String Table for data persistence', async ({ page }) => {
    // Test case 2: Check for SSTable explanation
    const section = page.locator('#what-is');
    await expect(section).toBeVisible();

    // Check for SSTable or Sorted String Table text
    const sectionText = await section.textContent();
    const hasSSTable = sectionText.includes('SSTable') || sectionText.includes('Sorted String Table');
    expect(hasSSTable).toBeTruthy();
  });

  test('Content mentions LSM tree or Log-Structured Merge-tree architecture', async ({ page }) => {
    // Test case 3: Check for LSM tree explanation
    const section = page.locator('#what-is');
    await expect(section).toBeVisible();

    // Check for LSM tree or Log-Structured Merge-tree text
    const sectionText = await section.textContent();
    const hasLSMTree = sectionText.includes('LSM tree') || sectionText.includes('Log-Structured Merge-tree') || sectionText.includes('LSM Tree');
    expect(hasLSMTree).toBeTruthy();
  });

  test('Section is navigable via anchor link', async ({ page }) => {
    // Additional test: verify the section has proper id for navigation
    const section = page.locator('#what-is');
    await expect(section).toHaveAttribute('id', 'what-is');
  });

  test('Section has accessible title with proper aria-labelledby', async ({ page }) => {
    // Additional test: verify accessibility
    const section = page.locator('#what-is');
    await expect(section).toHaveAttribute('aria-labelledby', 'what-is-title');

    const title = page.locator('#what-is-title');
    await expect(title).toBeVisible();
  });
});
