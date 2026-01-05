// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Project Status Display Tests (REQ-9)
 *
 * This test suite verifies that project status and current capabilities are displayed
 * as specified in the product requirements document.
 *
 * Technical stack information that should be visible:
 * - Rust programming language
 * - Tokio async runtime
 * - Snappy compression
 * - CRC32 checksums
 */

test.describe('Project Status Display (REQ-9)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Rust Language Mention
   * Verifies that the page mentions MirDB is written in Rust.
   */
  test('TC1: Page mentions that MirDB is written in Rust', async ({ page }) => {
    // Search for Rust mention anywhere on the page
    const pageContent = await page.textContent('body');

    // Verify Rust is mentioned on the page
    expect(pageContent).toMatch(/Rust/i);

    // Also verify there's a project status section with Rust information
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Verify Rust is mentioned in the status section
    const statusContent = await statusSection.textContent();
    expect(statusContent).toMatch(/Rust/i);
  });

  /**
   * Test Case 2: Tokio Runtime Mention
   * Verifies that the page mentions async networking with Tokio.
   */
  test('TC2: Page mentions async networking with Tokio', async ({ page }) => {
    // Search for Tokio mention in the project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    const statusContent = await statusSection.textContent();

    // Verify Tokio is mentioned
    expect(statusContent).toMatch(/Tokio/i);

    // Verify async networking context is provided
    expect(statusContent).toMatch(/async/i);
  });

  /**
   * Test Case 3: Compression/Storage Details
   * Verifies that the page mentions Snappy compression or CRC32 checksums.
   */
  test('TC3: Page mentions Snappy compression or CRC32 checksums', async ({ page }) => {
    // Search for compression/storage details in the project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    const statusContent = await statusSection.textContent();

    // Verify either Snappy compression or CRC32 checksums are mentioned
    const hasSnappy = /Snappy/i.test(statusContent);
    const hasCRC32 = /CRC32/i.test(statusContent);

    expect(hasSnappy || hasCRC32).toBe(true);
  });

  /**
   * Additional test: Project status section is properly structured
   */
  test('Project status section has proper heading and structure', async ({ page }) => {
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Verify there's a heading for the section
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();

    // Verify the section has a tech stack grid
    const techGrid = page.locator('[data-testid="tech-stack-grid"]');
    await expect(techGrid).toBeVisible();
  });

  /**
   * Additional test: All tech stack items are displayed
   */
  test('Tech stack displays all key technologies', async ({ page }) => {
    const techGrid = page.locator('[data-testid="tech-stack-grid"]');
    await expect(techGrid).toBeVisible();

    // Verify individual tech stack items are present
    const rustItem = page.locator('[data-testid="tech-rust"]');
    const tokioItem = page.locator('[data-testid="tech-tokio"]');
    const snappyItem = page.locator('[data-testid="tech-snappy"]');
    const crc32Item = page.locator('[data-testid="tech-crc32"]');

    await expect(rustItem).toBeVisible();
    await expect(tokioItem).toBeVisible();
    await expect(snappyItem).toBeVisible();
    await expect(crc32Item).toBeVisible();
  });
});
