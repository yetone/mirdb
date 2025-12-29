import { test, expect } from '@playwright/test';

test.describe('Technical Specifications Section (REQ-7)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page contains section with technical details about MirDB', async ({ page }) => {
    // Test Case 1: Check for technical specifications content
    // Expected: Page contains section with technical details about MirDB

    const techSpecsSection = page.locator('[data-testid="technical-specs-section"]');
    await expect(techSpecsSection).toBeVisible();

    // Verify it has a heading indicating technical specifications
    const heading = techSpecsSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/Technical|Specifications|Architecture/i);

    // Verify there's content about MirDB technical details
    await expect(techSpecsSection).toContainText(/MirDB|LSM|memcached|protocol/i);
  });

  test('technical section mentions 7-level LSM tree', async ({ page }) => {
    // Test Case 2: Verify 7-level LSM tree mention
    // Expected: Technical section mentions '7-level' or 'seven level' LSM tree

    const techSpecsSection = page.locator('[data-testid="technical-specs-section"]');
    await expect(techSpecsSection).toBeVisible();

    // Check for 7-level or seven level LSM tree mention
    const content = await techSpecsSection.textContent();
    expect(content).not.toBeNull();

    const has7Level = content!.toLowerCase().includes('7-level') ||
                      content!.toLowerCase().includes('7 level') ||
                      content!.toLowerCase().includes('seven level') ||
                      content!.toLowerCase().includes('seven-level');

    expect(has7Level).toBe(true);

    // Also verify LSM tree is mentioned
    expect(content!.toLowerCase()).toContain('lsm');
  });

  test('page lists all supported commands', async ({ page }) => {
    // Test Case 3: Verify supported commands list
    // Expected: Page lists supported commands: SET, GET, GETS, ADD, REPLACE, APPEND, PREPEND, DELETE, INFO

    const techSpecsSection = page.locator('[data-testid="technical-specs-section"]');
    await expect(techSpecsSection).toBeVisible();

    const content = await techSpecsSection.textContent();
    expect(content).not.toBeNull();

    // Verify all supported commands are listed
    const requiredCommands = ['SET', 'GET', 'GETS', 'ADD', 'REPLACE', 'APPEND', 'PREPEND', 'DELETE', 'INFO'];

    for (const command of requiredCommands) {
      expect(content!.toUpperCase()).toContain(command);
    }
  });

  test('technical section has proper structure with multiple specs', async ({ page }) => {
    // Additional test for semantic structure of technical specs
    const techSpecsSection = page.locator('[data-testid="technical-specs-section"]');
    await expect(techSpecsSection).toBeVisible();

    // Should be a section element
    const sectionElement = page.locator('section[data-testid="technical-specs-section"]');
    await expect(sectionElement).toBeVisible();

    // Should contain multiple spec items or details
    const specItems = techSpecsSection.locator('[data-testid^="spec-"]');
    const count = await specItems.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test('technical section mentions Snappy compression', async ({ page }) => {
    // Test for compression information as mentioned in PRD technical highlights
    const techSpecsSection = page.locator('[data-testid="technical-specs-section"]');
    await expect(techSpecsSection).toBeVisible();

    const content = await techSpecsSection.textContent();
    expect(content).not.toBeNull();

    // Verify Snappy compression is mentioned
    expect(content!.toLowerCase()).toContain('snappy');
  });
});
