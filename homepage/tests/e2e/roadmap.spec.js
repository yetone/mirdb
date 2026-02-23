// @ts-check
import { test, expect } from '@playwright/test';

/**
 * Roadmap Section E2E Tests
 * Owner: Scenario 4 - Roadmap Section
 *
 * End-to-end tests for roadmap section:
 * - Section is present
 * - Raft consensus listed
 * - Coming soon labels visible
 * - Visual distinction from features
 */

test.describe('Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Roadmap section is present and visible', async ({ page }) => {
    // Check Roadmap section exists
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check section has proper heading
    const heading = page.locator('#roadmap-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Roadmap');
  });

  test('TC2: Raft consensus is listed as a planned feature', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');

    // Check for Raft consensus in the roadmap
    const raftItem = roadmapSection.locator('.roadmap__item', {
      has: page.locator('.roadmap__item-title:has-text("Raft Consensus")')
    });
    await expect(raftItem).toBeVisible();

    // Verify the title text
    const raftTitle = raftItem.locator('.roadmap__item-title');
    await expect(raftTitle).toContainText('Raft Consensus');
  });

  test('TC3: Planned features are clearly labeled as coming soon', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    const roadmapItems = roadmapSection.locator('.roadmap__item');

    // Should have at least one item with "Coming Soon" badge
    const comingSoonBadges = roadmapSection.locator('.roadmap__badge');
    const badgeCount = await comingSoonBadges.count();
    expect(badgeCount).toBeGreaterThan(0);

    // Each roadmap item should have a coming soon badge
    const itemCount = await roadmapItems.count();
    expect(itemCount).toBeGreaterThan(0);

    for (let i = 0; i < itemCount; i++) {
      const badge = roadmapItems.nth(i).locator('.roadmap__badge');
      await expect(badge).toBeVisible();
      await expect(badge).toContainText(/coming soon/i);
    }
  });

  test('TC4: Roadmap items are visually distinct from implemented features', async ({ page }) => {
    // Get styling of roadmap items vs features items
    const roadmapItem = page.locator('.roadmap__item').first();
    const featureItem = page.locator('.features__item').first();

    await expect(roadmapItem).toBeVisible();
    await expect(featureItem).toBeVisible();

    // Check that roadmap items have the warning color left border (coming soon indicator)
    const roadmapBorderColor = await roadmapItem.evaluate((el) => {
      return window.getComputedStyle(el).borderLeftColor;
    });

    // Roadmap items should have the warning color (--color-warning: #f39c12) as border
    // RGB value for #f39c12 is rgb(243, 156, 18)
    expect(roadmapBorderColor).toContain('243');
    expect(roadmapBorderColor).toContain('156');
    expect(roadmapBorderColor).toContain('18');

    // Check that roadmap items have a Coming Soon badge which features don't have
    const roadmapBadge = roadmapItem.locator('.roadmap__badge');
    await expect(roadmapBadge).toBeVisible();

    // Feature items should NOT have a "Coming Soon" badge
    const featureBadge = featureItem.locator('.roadmap__badge');
    await expect(featureBadge).not.toBeVisible();
  });

  test('Additional Protocol Support is listed as a planned feature', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');

    // Check for Additional Protocol Support
    const protocolItem = roadmapSection.locator('.roadmap__item', {
      has: page.locator('.roadmap__item-title:has-text("Additional Protocol Support")')
    });
    await expect(protocolItem).toBeVisible();
  });

  test('Performance Benchmarks is listed as a planned feature', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');

    // Check for Performance Benchmarks
    const benchmarkItem = roadmapSection.locator('.roadmap__item', {
      has: page.locator('.roadmap__item-title:has-text("Performance Benchmarks")')
    });
    await expect(benchmarkItem).toBeVisible();
  });

  test('All three planned features are displayed', async ({ page }) => {
    const roadmapSection = page.locator('#roadmap');
    const roadmapItems = roadmapSection.locator('.roadmap__item');

    // Should have exactly 3 planned features
    await expect(roadmapItems).toHaveCount(3);
  });

  test('Roadmap section follows Features section', async ({ page }) => {
    // Get bounding boxes to verify order
    const featuresSection = page.locator('#features');
    const roadmapSection = page.locator('#roadmap');

    const featuresBox = await featuresSection.boundingBox();
    const roadmapBox = await roadmapSection.boundingBox();

    expect(featuresBox).not.toBeNull();
    expect(roadmapBox).not.toBeNull();

    // Roadmap should be below Features (higher Y value)
    expect(roadmapBox.y).toBeGreaterThan(featuresBox.y);
  });

  test('Roadmap section has accessible structure', async ({ page }) => {
    const roadmapSection = page.locator('section#roadmap');

    // Check for aria-labelledby
    await expect(roadmapSection).toHaveAttribute('aria-labelledby', 'roadmap-title');

    // Check the heading is an h2
    const heading = page.locator('#roadmap-title');
    const tagName = await heading.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('h2');
  });

  test('Roadmap badges have proper styling for visibility', async ({ page }) => {
    const badge = page.locator('.roadmap__badge').first();
    await expect(badge).toBeVisible();

    // Check badge background color matches warning color
    const backgroundColor = await badge.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should have warning color background (--color-warning: #f39c12)
    expect(backgroundColor).toContain('243');
    expect(backgroundColor).toContain('156');
    expect(backgroundColor).toContain('18');

    // Check badge is readable with dark text
    const textColor = await badge.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Should have dark text color (--color-text-dark: #333333 -> rgb(51, 51, 51))
    expect(textColor).toMatch(/rgb\(5[01], 5[01], 5[01]\)/);
  });

  test('Roadmap can be navigated to via nav link', async ({ page }) => {
    // Find the Roadmap link in navigation
    const navLink = page.locator('nav a[href="#roadmap"]');
    await expect(navLink).toBeVisible();

    // Click the nav link
    await navLink.click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify roadmap section is in view
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeInViewport();
  });
});
