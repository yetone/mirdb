/**
 * Architecture Section E2E Tests
 * Owner: Scenario 8 - Architecture Overview Section
 *
 * Tests:
 * - Architecture section presence with heading
 * - LSM tree architecture diagram visibility
 * - Architecture component cards presence
 */
import { test, expect } from '@playwright/test';

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('index.html');
  });

  test('TC1: Architecture section with heading exists', async ({ page }) => {
    // Check for architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for Architecture heading
    const heading = architectureSection.locator('h2');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toContain('architecture');
  });

  test('TC5: Architecture diagram or visualization exists', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for architecture diagram (could be pre/code block, SVG, or image)
    // Our implementation uses a pre element with ASCII art
    const diagram = architectureSection.locator('.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Verify it has the role="img" attribute for accessibility
    await expect(diagram).toHaveAttribute('role', 'img');

    // Verify it contains the ASCII art data flow diagram
    const diagramText = await diagram.textContent();
    expect(diagramText).toContain('WRITE REQUEST');
    expect(diagramText).toContain('MEMTABLE');
    expect(diagramText).toContain('SSTABLE');
    expect(diagramText).toContain('WAL');
  });

  test('Architecture section has LSM tree component explanation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    const lsmComponent = architectureSection.locator('[data-component="lsm-tree"]');

    await expect(lsmComponent).toBeVisible();

    // Check for LSM Tree heading
    const heading = lsmComponent.locator('h3');
    await expect(heading).toContainText('LSM Tree');
  });

  test('Architecture section has memtable component explanation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    const memtableComponent = architectureSection.locator('[data-component="memtable"]');

    await expect(memtableComponent).toBeVisible();

    // Check for Memtable heading
    const heading = memtableComponent.locator('h3');
    await expect(heading).toContainText('Memtable');
  });

  test('Architecture section has SSTable component explanation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    const sstableComponent = architectureSection.locator('[data-component="sstable"]');

    await expect(sstableComponent).toBeVisible();

    // Check for SSTable heading
    const heading = sstableComponent.locator('h3');
    await expect(heading).toContainText('SSTable');
  });

  test('Architecture section has WAL component explanation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    const walComponent = architectureSection.locator('[data-component="wal"]');

    await expect(walComponent).toBeVisible();

    // Check for WAL heading
    const heading = walComponent.locator('h3');
    await expect(heading).toContainText('Write-Ahead Log');
  });

  test('Architecture diagram is accessible with aria-label', async ({ page }) => {
    const diagram = page.locator('.architecture-diagram');
    await expect(diagram).toHaveAttribute('aria-label', /architecture.*diagram/i);
  });
});
