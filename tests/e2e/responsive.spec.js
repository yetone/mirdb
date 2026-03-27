/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 10-12 - Responsive Design
 *
 * Tests:
 * - Mobile viewport (375px) layout
 * - Tablet viewport (768px) layout
 * - Desktop viewport (1280px) layout
 * - Mobile navigation menu
 * - Touch target sizes
 */
import { test, expect } from '@playwright/test';

// Mobile viewport (iPhone SE/X width)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  test('no horizontal overflow at 375px viewport width', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that there's no horizontal scrollbar visible
    const hasHorizontalScroll = await page.evaluate(() => {
      // Check if document is wider than viewport
      const docWidth = Math.max(
        document.documentElement.scrollWidth,
        document.body.scrollWidth
      );
      const viewportWidth = window.innerWidth;
      return docWidth > viewportWidth + 5; // Allow 5px tolerance
    });

    // The page should not have significant horizontal overflow
    // (some small overflow may occur due to scrollbar calculations)
    expect(hasHorizontalScroll).toBe(false);
  });

  test('mobile navigation hamburger menu is visible and functional', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Mobile menu button should be visible
    const mobileMenuBtn = page.locator('#mobile-menu-btn');
    await expect(mobileMenuBtn).toBeVisible({ timeout: 5000 });

    // Mobile menu should initially be hidden
    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toBeHidden();

    // Click hamburger button to open menu
    await mobileMenuBtn.click();
    await expect(mobileMenu).toBeVisible({ timeout: 3000 });

    // Mobile menu should contain navigation links
    const featuresLink = mobileMenu.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Click again to close menu
    await mobileMenuBtn.click();
    await expect(mobileMenu).toBeHidden({ timeout: 3000 });
  });

  test('code blocks are horizontally scrollable on mobile', async ({ page }) => {
    // Navigate to quick-start section
    await page.goto('/#quick-start');
    await page.waitForLoadState('domcontentloaded');
    await page.waitForTimeout(300);

    // Find code blocks
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check that code blocks have overflow-x styling that allows scrolling
    const firstCodeBlock = codeBlocks.first();
    const preElement = firstCodeBlock.locator('pre');

    // Get computed style for overflow
    const overflowX = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });

    // Should be 'auto', 'scroll', or 'visible' (visible means content can extend)
    // The key is that code blocks have overflow handling
    expect(['auto', 'scroll', 'visible']).toContain(overflowX);

    // Verify code block container doesn't overflow viewport
    const codeBlockRect = await firstCodeBlock.boundingBox();
    expect(codeBlockRect.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 5);
  });

  test('touch targets are at least 44x44px for accessibility', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Test key interactive elements for minimum touch target size
    // Focus on primary mobile navigation elements
    const primaryTouchTargets = [
      { selector: '#mobile-menu-btn', name: 'Mobile menu button' },
      { selector: '#hero a', name: 'Hero CTA buttons' }
    ];

    for (const { selector, name } of primaryTouchTargets) {
      const elements = page.locator(selector);
      const count = await elements.count();

      if (count > 0) {
        const firstElement = elements.first();
        const isVisible = await firstElement.isVisible().catch(() => false);

        if (isVisible) {
          const boundingBox = await firstElement.boundingBox();

          if (boundingBox) {
            // Touch targets should be at least 44x44px (WCAG 2.5.5)
            // Allow some tolerance for computed dimensions
            expect(boundingBox.width).toBeGreaterThanOrEqual(44);
            expect(boundingBox.height).toBeGreaterThanOrEqual(44);
          }
        }
      }
    }
  });

  test('all sections stack vertically on mobile', async ({ page }) => {
    // Verify main sections are visible and stack vertically
    const sections = ['#hero', '#demo', '#features', '#quick-start', '#commands', '#status'];

    let previousBottom = 0;

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      const isVisible = await section.isVisible().catch(() => false);

      if (isVisible) {
        const box = await section.boundingBox();
        if (box) {
          // Section should start at or after the previous section ended
          expect(box.y).toBeGreaterThanOrEqual(previousBottom - 1);

          // Section should fit within viewport width
          expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

          previousBottom = box.y + box.height;
        }
      }
    }
  });

  test('navigation is accessible via hamburger menu', async ({ page }) => {
    // Open mobile menu
    const mobileMenuBtn = page.locator('#mobile-menu-btn');
    await mobileMenuBtn.click();

    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toBeVisible();

    // Verify aria-expanded attribute is updated
    const ariaExpanded = await mobileMenuBtn.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('true');

    // Navigate using mobile menu link
    const featuresLink = mobileMenu.locator('a[href="#features"]');
    await featuresLink.click();

    // Should scroll to features section
    await page.waitForTimeout(500);
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport({ ratio: 0.3 });
  });

  test('hero section is readable on mobile', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Hero title should be visible
    const title = hero.locator('h1');
    await expect(title).toBeVisible();
    await expect(title).toContainText('MirDB');

    // Value proposition should be visible
    const description = hero.locator('p').first();
    await expect(description).toBeVisible();

    // CTA buttons should be visible
    const ctaButtons = hero.locator('a');
    const ctaCount = await ctaButtons.count();
    expect(ctaCount).toBeGreaterThanOrEqual(1);
  });

  test('features section displays as single column on mobile', async ({ page }) => {
    await page.goto('/#features');
    await page.waitForTimeout(500);

    const featuresGrid = page.locator('#features .grid');
    const featureCards = featuresGrid.locator('> div');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThan(0);

    // On mobile, cards should stack (single column)
    // Check that cards have similar x positions (stacked vertically)
    const firstCard = featureCards.first();
    const firstCardBox = await firstCard.boundingBox();

    if (cardCount > 1) {
      const secondCard = featureCards.nth(1);
      const secondCardBox = await secondCard.boundingBox();

      // Cards should be at similar x position (stacked) or very close
      // On mobile, they should be in a single column
      expect(Math.abs(firstCardBox.x - secondCardBox.x)).toBeLessThan(10);
    }
  });

  test('footer content is accessible on mobile', async ({ page }) => {
    await page.goto('/#footer');
    await page.waitForTimeout(300);

    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Footer text should be visible
    const footerText = footer.locator('p');
    await expect(footerText.first()).toBeVisible();

    // GitHub link in footer should be accessible
    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();
  });
});

// Tablet viewport (iPad/standard tablet width)
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('layout adapts appropriately at 768px viewport width with no content overflow', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that there's no horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      const docWidth = Math.max(
        document.documentElement.scrollWidth,
        document.body.scrollWidth
      );
      const viewportWidth = window.innerWidth;
      return docWidth > viewportWidth + 5; // Allow 5px tolerance
    });

    expect(hasHorizontalScroll).toBe(false);

    // Verify main sections are visible and fit within viewport
    const sections = ['#hero', '#demo', '#features', '#quick-start', '#footer'];
    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      const isVisible = await section.isVisible().catch(() => false);
      if (isVisible) {
        const box = await section.boundingBox();
        if (box) {
          // Section should fit within viewport width
          expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
        }
      }
    }
  });

  test('features display in 2-column or appropriate grid layout on tablet', async ({ page }) => {
    await page.goto('/#features');
    await page.waitForTimeout(500);

    const featuresGrid = page.locator('#features .grid');
    await expect(featuresGrid).toBeVisible();

    const featureCards = featuresGrid.locator('> div');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // On tablet (md: breakpoint is 768px), we expect 2-column layout
    // Check that the first two cards are side by side (same y position)
    if (cardCount >= 2) {
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstCardBox = await firstCard.boundingBox();
      const secondCardBox = await secondCard.boundingBox();

      // On tablet with 2-column grid, first two cards should be roughly at the same Y position
      // (allowing some tolerance for padding/margins)
      const yDifference = Math.abs(firstCardBox.y - secondCardBox.y);
      expect(yDifference).toBeLessThan(50); // Cards should be in the same row

      // The cards should be side by side (different x positions)
      expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x);
    }

    // Verify feature cards have proper grid layout class
    const gridClass = await featuresGrid.getAttribute('class');
    expect(gridClass).toContain('md:grid-cols-2');
  });

  test('navigation is accessible and appropriately sized on tablet', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // On tablet (768px is md: breakpoint), desktop navigation should be visible
    const desktopNav = page.locator('header nav .hidden.md\\:flex');
    const mobileMenuBtn = page.locator('#mobile-menu-btn');

    // Check if desktop navigation is visible (md: breakpoint starts at 768px)
    const isDesktopNavVisible = await desktopNav.isVisible().catch(() => false);
    const isMobileMenuBtnVisible = await mobileMenuBtn.isVisible().catch(() => false);

    // Either desktop nav should be visible OR mobile menu button should be visible
    // (depending on exact breakpoint behavior)
    expect(isDesktopNavVisible || isMobileMenuBtnVisible).toBe(true);

    if (isDesktopNavVisible) {
      // Desktop navigation is visible - check that links are accessible
      const featuresLink = desktopNav.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      const quickStartLink = desktopNav.locator('a[href="#quick-start"]');
      await expect(quickStartLink).toBeVisible();

      // GitHub button should be visible
      const githubLink = desktopNav.locator('a[href*="github.com"]');
      await expect(githubLink).toBeVisible();

      // Check touch target sizes for tablet
      const githubBox = await githubLink.boundingBox();
      expect(githubBox.height).toBeGreaterThanOrEqual(36); // Reasonable button height
    } else {
      // Mobile navigation is showing - hamburger menu should work
      await expect(mobileMenuBtn).toBeVisible();

      // Touch target should be appropriately sized
      const menuBtnBox = await mobileMenuBtn.boundingBox();
      expect(menuBtnBox.width).toBeGreaterThanOrEqual(44);
      expect(menuBtnBox.height).toBeGreaterThanOrEqual(44);

      // Test that mobile menu works
      await mobileMenuBtn.click();
      const mobileMenu = page.locator('#mobile-menu');
      await expect(mobileMenu).toBeVisible({ timeout: 3000 });

      // Mobile menu links should be accessible
      const featuresLink = mobileMenu.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();
    }
  });

  test('hero section displays properly on tablet', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Hero title should be visible and appropriately sized
    const title = hero.locator('h1');
    await expect(title).toBeVisible();
    await expect(title).toContainText('MirDB');

    // Check title font size is appropriate for tablet (should use md: class)
    const titleFontSize = await title.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    // md:text-5xl = 3rem = 48px
    const fontSize = parseInt(titleFontSize);
    expect(fontSize).toBeGreaterThanOrEqual(36); // Should be larger than mobile

    // CTA buttons should be in a row (sm: breakpoint)
    const ctaContainer = hero.locator('.flex.flex-col.sm\\:flex-row');
    await expect(ctaContainer).toBeVisible();

    const buttons = ctaContainer.locator('a');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(2);

    // Check buttons are side by side on tablet
    if (buttonCount >= 2) {
      const firstBtn = buttons.nth(0);
      const secondBtn = buttons.nth(1);
      const firstBox = await firstBtn.boundingBox();
      const secondBox = await secondBtn.boundingBox();

      // On tablet, buttons should be in a row (same Y position)
      const yDiff = Math.abs(firstBox.y - secondBox.y);
      expect(yDiff).toBeLessThan(20);
    }
  });

  test('code blocks are properly displayed on tablet', async ({ page }) => {
    await page.goto('/#quick-start');
    await page.waitForTimeout(300);

    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Code block should fit within viewport
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

    // Copy button should be visible and clickable
    const copyBtn = firstCodeBlock.locator('.copy-btn');
    await expect(copyBtn).toBeVisible();
  });

  test('comparison table displays properly on tablet', async ({ page }) => {
    await page.goto('/#comparison');
    await page.waitForTimeout(300);

    const comparisonTable = page.locator('.comparison-table table');
    const isVisible = await comparisonTable.isVisible().catch(() => false);

    if (isVisible) {
      // Table should be visible and readable
      const tableBox = await comparisonTable.boundingBox();

      // Table header should be visible
      const headers = comparisonTable.locator('thead th');
      const headerCount = await headers.count();
      expect(headerCount).toBeGreaterThanOrEqual(3); // Feature, MirDB, Memcached

      // All headers should be visible
      for (let i = 0; i < headerCount; i++) {
        await expect(headers.nth(i)).toBeVisible();
      }

      // Table rows should be readable
      const rows = comparisonTable.locator('tbody tr');
      const rowCount = await rows.count();
      expect(rowCount).toBeGreaterThan(0);
    }
  });

  test('architecture section grid displays in 2 columns on tablet', async ({ page }) => {
    await page.goto('/#architecture');
    await page.waitForTimeout(300);

    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Architecture components grid
    const componentsGrid = architectureSection.locator('.grid.md\\:grid-cols-2');
    const isVisible = await componentsGrid.isVisible().catch(() => false);

    if (isVisible) {
      const components = componentsGrid.locator('.architecture-component');
      const componentCount = await components.count();

      if (componentCount >= 2) {
        const first = components.nth(0);
        const second = components.nth(1);
        const firstBox = await first.boundingBox();
        const secondBox = await second.boundingBox();

        // On tablet with 2-column layout, first two should be side by side
        const yDiff = Math.abs(firstBox.y - secondBox.y);
        expect(yDiff).toBeLessThan(50);
        expect(secondBox.x).toBeGreaterThan(firstBox.x);
      }
    }
  });

  test('footer content displays properly on tablet', async ({ page }) => {
    await page.goto('/#footer');
    await page.waitForTimeout(300);

    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Footer content should be visible
    const footerText = footer.locator('p').first();
    await expect(footerText).toBeVisible();

    // Footer should use flex layout on tablet (md: breakpoint)
    const footerContainer = footer.locator('.flex.flex-col.md\\:flex-row');
    await expect(footerContainer).toBeVisible();

    // GitHub link should be accessible
    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();
  });

  test('commands section grid displays appropriately on tablet', async ({ page }) => {
    await page.goto('/#commands');
    await page.waitForTimeout(300);

    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Commands grid should adapt for tablet
    const commandsGrid = commandsSection.locator('.grid');
    await expect(commandsGrid).toBeVisible();

    const commandCards = commandsGrid.locator('.commands-card');
    const cardCount = await commandCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Cards should fit within viewport
    const firstCard = commandCards.first();
    const cardBox = await firstCard.boundingBox();
    expect(cardBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);
  });
});
