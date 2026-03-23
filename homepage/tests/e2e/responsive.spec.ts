/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 6, 7, 8 - Responsive Design
 *
 * Test cases:
 * - Desktop viewport (1920x1080, 1280x720)
 * - Tablet viewport (768px)
 * - Mobile viewport (375px)
 * - Navigation adaptation
 * - Content stacking
 */

import { test, expect } from '@playwright/test';
import { navigateToHomepage, selectors, viewports } from './test-utils';

test.describe('Responsive Design - Desktop', () => {
  test('TC1: Page renders without horizontal scrolling at 1920x1080 viewport, all content visible and properly aligned', async ({ page }) => {
    // Set large desktop viewport
    await page.setViewportSize(viewports.desktopLarge);
    await navigateToHomepage(page);

    // Verify no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify all major sections are visible
    const heroSection = page.locator(selectors.hero.section);
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator(selectors.features.section);
    await expect(featuresSection).toBeAttached();

    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeAttached();

    const quickstartSection = page.locator(selectors.quickstart.section);
    await expect(quickstartSection).toBeAttached();

    // Verify content is properly aligned (centered container)
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();

    // Container should be centered (left margin roughly equals right margin)
    const viewportWidth = 1920;
    const containerWidth = containerBox!.width;
    const leftMargin = containerBox!.x;
    const rightMargin = viewportWidth - (containerBox!.x + containerBox!.width);

    // Allow some tolerance for centering (within 50px)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);

    // Verify hero title is visible and readable
    const heroTitle = page.locator(selectors.hero.title);
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');
  });

  test('TC2: Page renders without horizontal scrolling at 1280x720 viewport, navigation fully visible', async ({ page }) => {
    // Set standard desktop viewport
    await page.setViewportSize(viewports.desktop);
    await navigateToHomepage(page);

    // Verify no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify navigation header is visible
    const header = page.locator(selectors.navigation.header);
    await expect(header).toBeVisible();

    // Verify navigation links are visible
    const navLinks = page.locator(selectors.navigation.links);
    await expect(navLinks).toBeVisible();

    // Verify individual navigation links are present and visible
    const navLinkItems = page.locator(selectors.navigation.link);
    const linkCount = await navLinkItems.count();
    expect(linkCount).toBeGreaterThanOrEqual(3); // At least Features, Usage, Quick Start

    for (let i = 0; i < linkCount; i++) {
      await expect(navLinkItems.nth(i)).toBeVisible();
    }

    // Verify navigation logo is visible
    const logo = page.locator(selectors.navigation.logo);
    await expect(logo).toBeVisible();
    await expect(logo).toHaveText('MirDB');
  });

  test('TC3: Content is centered and has appropriate max-width for readability', async ({ page }) => {
    // Set large desktop viewport to test max-width constraint
    await page.setViewportSize(viewports.desktopLarge);
    await navigateToHomepage(page);

    // Get the container max-width
    const container = page.locator('.container').first();
    const maxWidth = await container.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });

    // Verify max-width is set (not 'none')
    expect(maxWidth).not.toBe('none');

    // Parse max-width and verify it's reasonable for readability (typically 1200-1400px)
    const maxWidthValue = parseFloat(maxWidth);
    expect(maxWidthValue).toBeGreaterThanOrEqual(1000); // At least 1000px
    expect(maxWidthValue).toBeLessThanOrEqual(1600); // At most 1600px

    // Verify hero content has its own max-width for optimal reading
    const heroContent = page.locator('.hero-content');
    const heroMaxWidth = await heroContent.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(heroMaxWidth).not.toBe('none');

    // Verify section description has max-width for readability
    const sectionDescription = page.locator('.section-description').first();
    await expect(sectionDescription).toBeAttached();
    const descMaxWidth = await sectionDescription.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(descMaxWidth).not.toBe('none');

    // Verify centering via checking that left and right margins are equal (computed margins resolve 'auto' to pixels)
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();

    const viewportWidth = 1920;
    const leftMargin = containerBox!.x;
    const rightMargin = viewportWidth - (containerBox!.x + containerBox!.width);

    // Container should be centered (left and right margins roughly equal, within 50px tolerance)
    expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);

    // Container should have meaningful margins on both sides (not edge-to-edge)
    expect(leftMargin).toBeGreaterThan(20);
    expect(rightMargin).toBeGreaterThan(20);
  });

  test('TC4: Full horizontal navigation menu is displayed (no hamburger menu) on desktop', async ({ page }) => {
    // Test both desktop viewports
    const desktopViewports = [viewports.desktop, viewports.desktopLarge];

    for (const viewport of desktopViewports) {
      await page.setViewportSize(viewport);
      await navigateToHomepage(page);

      // Verify hamburger/toggle button is NOT visible
      const navToggle = page.locator(selectors.navigation.toggle);
      await expect(navToggle).not.toBeVisible();

      // Verify nav-links is visible (horizontal menu)
      const navLinks = page.locator(selectors.navigation.links);
      await expect(navLinks).toBeVisible();

      // Verify navigation links are displayed horizontally (flex row)
      const display = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('flex');

      const flexDirection = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('row');

      // Verify all navigation links are visible and accessible
      const linkTexts = ['Features', 'Usage', 'Quick Start', 'GitHub'];
      for (const text of linkTexts) {
        const link = page.locator(selectors.navigation.link, { hasText: text });
        await expect(link).toBeVisible();
      }

      // Verify navigation links are positioned horizontally
      const firstLink = page.locator(selectors.navigation.link).first();
      const lastLink = page.locator(selectors.navigation.link).last();

      const firstBox = await firstLink.boundingBox();
      const lastBox = await lastLink.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(lastBox).not.toBeNull();

      // Links should be on the same horizontal line (similar Y position)
      expect(Math.abs(firstBox!.y - lastBox!.y)).toBeLessThan(10);

      // Last link should be to the right of first link
      expect(lastBox!.x).toBeGreaterThan(firstBox!.x);
    }
  });

  test('All sections are accessible without horizontal scrolling on desktop', async ({ page }) => {
    await page.setViewportSize(viewports.desktopLarge);
    await navigateToHomepage(page);

    // Verify all sections can be scrolled to and are visible
    const sections = ['hero', 'features', 'usage', 'quickstart'];

    for (const sectionId of sections) {
      const section = page.locator(`#${sectionId}`);
      await section.scrollIntoViewIfNeeded();
      await expect(section).toBeVisible();

      // Verify no horizontal scroll at each section
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    }
  });

  test('Features grid displays properly on desktop', async ({ page }) => {
    await page.setViewportSize(viewports.desktopLarge);
    await navigateToHomepage(page);

    // Scroll to features section
    const featuresSection = page.locator(selectors.features.section);
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify features grid exists
    const featuresGrid = page.locator(selectors.features.grid);
    await expect(featuresGrid).toBeVisible();

    // Verify features use grid or flex for layout
    const display = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(['grid', 'flex']).toContain(display);

    // Verify feature cards are displayed
    const featureCards = page.locator(selectors.features.card);
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4); // Should have at least 4 features

    // On desktop, feature cards should be arranged in multiple columns
    const firstCard = featureCards.first();
    const secondCard = featureCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    // On large desktop, cards should be side by side (different X positions)
    expect(secondBox!.x).toBeGreaterThan(firstBox!.x);
  });
});
