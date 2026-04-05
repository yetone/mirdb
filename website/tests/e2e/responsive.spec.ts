/**
 * Responsive Design E2E Tests - Tablet
 * Owner: Scenario 8 - Responsive Design - Tablet
 *
 * Tests tablet viewport (768px - 1023px) responsive behavior
 */
import { test, expect } from '@playwright/test';

// Tablet viewport dimensions (iPad / medium screen)
const TABLET_VIEWPORT = { width: 768, height: 1024 };
const TABLET_LANDSCAPE = { width: 1024, height: 768 };

test.describe('Responsive Design - Tablet', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport before each test
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: Page renders correctly at 768px width with appropriate tablet layout', async ({ page }) => {
    // Verify page loads successfully at tablet width
    await expect(page).toHaveTitle(/MirDB/);

    // Hero section should be visible and properly rendered
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Hero content should be centered and fit within viewport
    const heroContent = page.locator('.hero__content');
    await expect(heroContent).toBeVisible();
    const heroBox = await heroContent.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Features section should be visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Quickstart section should be visible
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Comparison section should be visible
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Specs section should be visible
    const specsSection = page.locator('#specs');
    await expect(specsSection).toBeVisible();

    // Footer should be visible
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // No horizontal scrollbar should appear (content fits viewport)
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // Allow 1px tolerance
  });

  test('TC2: Feature cards display in 2-column layout on tablet', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = featuresSection.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.features__card');
    await expect(featureCards).toHaveCount(3);

    // Get bounding boxes of first two cards
    const card1 = featureCards.nth(0);
    const card2 = featureCards.nth(1);
    const card3 = featureCards.nth(2);

    const box1 = await card1.boundingBox();
    const box2 = await card2.boundingBox();
    const box3 = await card3.boundingBox();

    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();
    expect(box3).not.toBeNull();

    // On tablet (2-column layout):
    // - Cards 1 and 2 should be on the same row (same Y position)
    // - Card 3 should be on a separate row (different Y position)
    const rowTolerance = 10; // Allow some tolerance for alignment

    // First two cards should be in the same row (similar Y position)
    expect(Math.abs(box1!.y - box2!.y)).toBeLessThan(rowTolerance);

    // First two cards should be side by side (different X positions)
    expect(box2!.x).toBeGreaterThan(box1!.x);

    // Third card should be in a new row (Y position greater than first row)
    expect(box3!.y).toBeGreaterThan(box1!.y + box1!.height / 2);

    // All cards should fit within viewport width
    expect(box1!.x + box1!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    expect(box2!.x + box2!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
    expect(box3!.x + box3!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  test('TC3: Navigation remains accessible and usable on tablet', async ({ page }) => {
    // Navigation header should be visible
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Navigation should have proper structure
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check that navigation has navigation role for accessibility
    await expect(nav).toHaveAttribute('role', 'navigation');

    // Navigation links should be accessible (either visible or via menu)
    // On tablet, navigation might be inline or have a hamburger menu
    // Check if nav links are visible or hamburger menu exists

    // Verify navigation takes up appropriate width
    const navBox = await nav.boundingBox();
    expect(navBox).not.toBeNull();
    // Nav should span the full width or most of it
    expect(navBox!.width).toBeGreaterThan(TABLET_VIEWPORT.width * 0.5);

    // Hero CTA buttons should still be accessible and visible
    const heroSection = page.locator('#hero');
    const quickStartBtn = heroSection.locator('a.btn:has-text("Quick Start")');
    const githubBtn = heroSection.locator('a.btn:has-text("GitHub")');

    await expect(quickStartBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();

    // Verify buttons are clickable
    await expect(quickStartBtn).toBeEnabled();
    await expect(githubBtn).toBeEnabled();

    // Verify CTA buttons fit within viewport and are not truncated
    const quickStartBox = await quickStartBtn.boundingBox();
    const githubBox = await githubBtn.boundingBox();

    expect(quickStartBox).not.toBeNull();
    expect(githubBox).not.toBeNull();
    expect(quickStartBox!.x).toBeGreaterThanOrEqual(0);
    expect(githubBox!.x + githubBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  test('TC4: Specs section adapts to tablet with 2-column grid', async ({ page }) => {
    const specsSection = page.locator('#specs');
    await expect(specsSection).toBeVisible();

    // Get specs grid
    const specsGrid = specsSection.locator('.specs__grid');
    await expect(specsGrid).toBeVisible();

    // Get spec cards
    const specCards = specsSection.locator('.specs__card');
    await expect(specCards).toHaveCount(2);

    // Get bounding boxes
    const card1 = specCards.nth(0);
    const card2 = specCards.nth(1);

    const box1 = await card1.boundingBox();
    const box2 = await card2.boundingBox();

    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();

    // On tablet, specs should remain in 2-column layout
    // Both cards should be on the same row
    const rowTolerance = 10;
    expect(Math.abs(box1!.y - box2!.y)).toBeLessThan(rowTolerance);

    // Cards should be side by side
    expect(box2!.x).toBeGreaterThan(box1!.x);
  });

  test('TC5: Comparison table is readable and fits on tablet', async ({ page }) => {
    const comparisonSection = page.locator('#comparison');
    await expect(comparisonSection).toBeVisible();

    // Table wrapper should be visible
    const tableWrapper = comparisonSection.locator('.comparison__table-wrapper');
    await expect(tableWrapper).toBeVisible();

    // Table should be visible
    const table = comparisonSection.locator('.comparison__table');
    await expect(table).toBeVisible();

    // Headers should be visible
    const headers = comparisonSection.locator('.comparison__header');
    const headerCount = await headers.count();
    expect(headerCount).toBeGreaterThanOrEqual(3); // Feature, MirDB, Memcached

    // Feature rows should be visible
    const featureRows = comparisonSection.locator('tbody tr');
    const rowCount = await featureRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(1);

    // Table wrapper should handle overflow appropriately
    const wrapperBox = await tableWrapper.boundingBox();
    expect(wrapperBox).not.toBeNull();
    expect(wrapperBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });

  test('TC6: Tablet landscape orientation renders correctly', async ({ page }) => {
    // Set tablet landscape viewport
    await page.setViewportSize(TABLET_LANDSCAPE);
    await page.goto('/');

    // Hero section should be visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Features should still render properly
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // At 1024px (edge of tablet/desktop), features might be 2 or 3 columns
    // Just verify they render without overflow
    const featuresGrid = page.locator('.features__grid');
    const gridBox = await featuresGrid.boundingBox();
    expect(gridBox).not.toBeNull();
    expect(gridBox!.width).toBeLessThanOrEqual(TABLET_LANDSCAPE.width);

    // No horizontal scroll
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);
  });

  test('TC7: Footer displays correctly on tablet', async ({ page }) => {
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Footer links should be visible
    const githubLink = footer.locator('[data-testid="github-link"]');
    const communityLink = footer.locator('[data-testid="community-link"]');
    const licenseLink = footer.locator('[data-testid="license-link"]');

    await expect(githubLink).toBeVisible();
    await expect(communityLink).toBeVisible();
    await expect(licenseLink).toBeVisible();

    // Footer should fit within viewport
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Footer content should be accessible
    const footerContainer = footer.locator('.footer__container');
    await expect(footerContainer).toBeVisible();
  });

  test('TC8: Code blocks in quickstart section are readable on tablet', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Code blocks should be visible
    const codeBlocks = quickstartSection.locator('.quickstart__code');
    const blockCount = await codeBlocks.count();
    expect(blockCount).toBeGreaterThanOrEqual(1);

    // First code block should fit within viewport or have scroll
    const firstBlock = codeBlocks.first();
    await expect(firstBlock).toBeVisible();

    const blockBox = await firstBlock.boundingBox();
    expect(blockBox).not.toBeNull();
    // Block should not overflow viewport width significantly
    expect(blockBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Copy buttons should still be accessible
    const copyButtons = quickstartSection.locator('.quickstart__copy-btn');
    const btnCount = await copyButtons.count();
    expect(btnCount).toBeGreaterThanOrEqual(1);
    await expect(copyButtons.first()).toBeVisible();
  });
});
