/**
 * End-to-end tests for LSM Tree Visualization.
 * Covers REQ-5 (LSM tree visualization).
 */

import { test, expect } from '@playwright/test';

test.describe('LSM Tree Visualization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('LSM tree section is visible', async ({ page }) => {
    const section = page.getByTestId('lsm-tree-section');
    await expect(section).toBeVisible();
  });

  test('renders section title', async ({ page }) => {
    const title = page.getByTestId('lsm-tree-title');
    await expect(title).toBeVisible();
    await expect(title).toHaveTextContent('LSM Tree Visualization');
  });

  test('renders SVG visualization', async ({ page }) => {
    const svg = page.getByTestId('lsm-tree-svg');
    await expect(svg).toBeVisible();
  });

  test('renders memtable block with key count', async ({ page }) => {
    const svg = page.getByTestId('lsm-tree-svg');
    await expect(svg).toHaveText(/Memtable/);
    await expect(svg).toHaveText(/50 keys/);
  });

  test('renders SSTable levels with file counts', async ({ page }) => {
    const svg = page.getByTestId('lsm-tree-svg');
    await expect(svg).toHaveText(/Level 0/);
    await expect(svg).toHaveText(/3 files/);
    await expect(svg).toHaveText(/Level 1/);
    await expect(svg).toHaveText(/2 files/);
    await expect(svg).toHaveText(/Level 2/);
    await expect(svg).toHaveText(/1 file/);
  });

  test('shows tooltip on hovering SSTable level block', async ({ page }) => {
    const level0 = page.getByTestId('sstable-level-0');
    await level0.hover();

    const tooltip = page.getByTestId('lsm-tooltip');
    await expect(tooltip).toBeVisible();
    await expect(tooltip).toHaveTextContent('Level 0');
    await expect(tooltip).toHaveTextContent('Files: 3');
    await expect(tooltip).toHaveTextContent('Total size');
    await expect(tooltip).toHaveTextContent('Compaction');
  });

  test('shows tooltip on hovering memtable block', async ({ page }) => {
    const memtable = page.getByTestId('memtable-block');
    await memtable.hover();

    const tooltip = page.getByTestId('lsm-tooltip');
    await expect(tooltip).toBeVisible();
    await expect(tooltip).toHaveTextContent('Memtable');
    await expect(tooltip).toHaveTextContent('Keys: 50');
    await expect(tooltip).toHaveTextContent('Location: RAM');
  });

  test('hides tooltip on mouse leave', async ({ page }) => {
    const level0 = page.getByTestId('sstable-level-0');
    await level0.hover();
    await expect(page.getByTestId('lsm-tooltip')).toBeVisible();

    await page.mouse.move(0, 0);
    await expect(page.getByTestId('lsm-tooltip')).not.toBeVisible();
  });

  test('SVG has accessibility attributes', async ({ page }) => {
    const svg = page.getByTestId('lsm-tree-svg');
    await expect(svg).toHaveAttribute('role', 'img');
    await expect(svg).toHaveAttribute('aria-label', expect.stringContaining('LSM tree'));
  });

  test('section has ARIA label for accessibility', async ({ page }) => {
    const section = page.getByTestId('lsm-tree-section');
    await expect(section).toHaveAttribute('aria-label', 'LSM Tree Visualization');
  });
});
