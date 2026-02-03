/**
 * E2E tests for Architecture section.
 * Owner: Scenario 4 - Architecture Overview Section
 *
 * Tests:
 * - Architecture section loads with diagram and explanation
 * - Documentation link navigates to architecture documentation
 */

import { test, expect } from '@playwright/test';

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/mirdb/');
  });

  test('architecture section renders with diagram and LSM-tree explanation', async ({ page }) => {
    // Scroll to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check section is visible
    await expect(architectureSection).toBeVisible();

    // Check heading is present
    const heading = architectureSection.locator('h2');
    await expect(heading).toContainText('Architecture Overview');

    // Check diagram image is present and has alt text
    const diagramImg = architectureSection.locator('.architecture-diagram-img');
    await expect(diagramImg).toBeVisible();
    await expect(diagramImg).toHaveAttribute('alt', /Client.*Memcached.*Memtable.*SSTable/i);

    // Check LSM-tree explanation sections
    await expect(architectureSection.locator('h3')).toContainText(['Data Flow', 'LSM-tree Architecture']);

    // Check memtable description (exact match for "Memtable (Active)")
    await expect(architectureSection.locator('h4').filter({ hasText: 'Memtable (Active)' })).toBeVisible();

    // Check immutable memtable description
    await expect(architectureSection.locator('h4').filter({ hasText: 'Immutable Memtable' })).toBeVisible();

    // Check SSTable description (exact match for "SSTable (Sorted String Table)")
    await expect(architectureSection.locator('h4').filter({ hasText: 'SSTable (Sorted String Table)' })).toBeVisible();
  });

  test('clicking documentation link navigates to detailed architecture documentation', async ({ page, context }) => {
    // Navigate to the page and find architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Find the architecture documentation link
    const docLink = architectureSection.locator('.architecture-doc-link');
    await expect(docLink).toBeVisible();
    await expect(docLink).toContainText('View detailed architecture documentation');

    // Verify link attributes
    await expect(docLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#architecture');
    await expect(docLink).toHaveAttribute('target', '_blank');
    await expect(docLink).toHaveAttribute('rel', /noopener.*noreferrer|noreferrer.*noopener/);

    // Set up listener for new page (popup/new tab)
    const pagePromise = context.waitForEvent('page');

    // Click the documentation link
    await docLink.click();

    // Wait for the new tab to open
    const newPage = await pagePromise;

    // Verify the new tab URL is the GitHub architecture documentation
    expect(newPage.url()).toContain('github.com/yetone/mirdb');
  });

  test('architecture diagram SVG loads correctly', async ({ page }) => {
    // Navigate and scroll to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check the diagram image source
    const diagramImg = architectureSection.locator('.architecture-diagram-img');
    await expect(diagramImg).toHaveAttribute('src', '/architecture-diagram.svg');

    // Verify the image is visible and has dimensions
    await expect(diagramImg).toBeVisible();

    // Wait for image to load and verify it rendered with actual dimensions
    await page.waitForLoadState('networkidle');
    const boundingBox = await diagramImg.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox!.width).toBeGreaterThan(0);
    expect(boundingBox!.height).toBeGreaterThan(0);
  });

  test('architecture section has proper accessibility attributes', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check diagram has proper alt text for accessibility
    const diagramImg = architectureSection.locator('.architecture-diagram-img');
    const altText = await diagramImg.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText?.length).toBeGreaterThan(50); // Alt text should be descriptive

    // Verify heading hierarchy
    const h2 = architectureSection.locator('h2');
    const h3Elements = architectureSection.locator('h3');
    const h4Elements = architectureSection.locator('h4');

    await expect(h2).toHaveCount(1);
    await expect(h3Elements).toHaveCount(2);
    await expect(h4Elements).toHaveCount(3);
  });

  test('architecture section displays correctly on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    await expect(architectureSection).toBeVisible();

    // Check diagram is visible on mobile
    const diagramImg = architectureSection.locator('.architecture-diagram-img');
    await expect(diagramImg).toBeVisible();

    // Check documentation link is visible
    const docLink = architectureSection.locator('.architecture-doc-link');
    await expect(docLink).toBeVisible();
  });
});
