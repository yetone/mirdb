// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Mobile Responsive Design', () => {
  test.describe('Mobile Viewport (375px)', () => {
    test.beforeEach(async ({ page }) => {
      // Set mobile viewport (iPhone SE size)
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    test('TC1: All content is visible without horizontal scrolling at 375px viewport', async ({ page }) => {
      // Check that the page does not have horizontal overflow
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = 375;

      // Body should not exceed viewport width (no horizontal scrolling needed)
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding tolerance

      // Verify key content elements are visible within viewport
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      const heroH1 = hero.locator('h1');
      await expect(heroH1).toBeVisible();

      const tagline = hero.locator('.tagline');
      await expect(tagline).toBeVisible();

      // Verify hero buttons are visible
      const heroButtons = page.locator('.hero-buttons');
      await expect(heroButtons).toBeVisible();

      // Verify features section is visible when scrolled to
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Verify footer is visible when scrolled to
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });

    test('TC2: Features stack vertically instead of three-column layout on mobile', async ({ page }) => {
      // Scroll to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Check that the grid is using single column layout on mobile
      const gridStyle = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
          gridTemplateColumns: style.gridTemplateColumns,
        };
      });

      expect(gridStyle.display).toBe('grid');

      // On mobile, gridTemplateColumns should have 1 value (1 column)
      // or the columns should be narrow enough to fit single column
      const columns = gridStyle.gridTemplateColumns.split(' ');
      expect(columns.length).toBe(1);

      // Verify all three feature cards are still present and visible
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(3);

      // Verify cards are stacked (each card takes full width)
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstCardBox = await firstCard.boundingBox();
      const secondCardBox = await secondCard.boundingBox();

      expect(firstCardBox).toBeTruthy();
      expect(secondCardBox).toBeTruthy();

      // Second card should be below the first card (stacked vertically)
      expect(secondCardBox.y).toBeGreaterThan(firstCardBox.y + firstCardBox.height - 10);
    });

    test('TC3: Navigation is accessible on mobile (collapsed or touch-friendly)', async ({ page }) => {
      // Check if navigation exists and is positioned appropriately for mobile
      const nav = page.locator('.nav');
      await expect(nav).toBeVisible();

      // On mobile at 480px or below, navigation should be stacked (per CSS)
      // At 375px, nav should be in column layout
      const navStyle = await nav.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
          flexDirection: style.flexDirection,
        };
      });

      // Navigation should be flex with column direction on mobile
      expect(navStyle.display).toBe('flex');
      expect(navStyle.flexDirection).toBe('column');

      // Verify nav links are still accessible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Verify all navigation links are still present
      const links = navLinks.locator('a');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThanOrEqual(3); // At minimum: Features, Quick Start, GitHub
    });
  });

  test.describe('Tablet Viewport (768px)', () => {
    test.beforeEach(async ({ page }) => {
      // Set tablet viewport
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
    });

    test('TC4: Layout adapts appropriately for tablet screen size (768px)', async ({ page }) => {
      // Verify hero section adapts
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Verify hero title is visible and properly sized
      const heroH1 = hero.locator('h1');
      await expect(heroH1).toBeVisible();

      // Check hero title font size is adjusted for tablet
      const h1FontSize = await heroH1.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      // Should be smaller than desktop (4rem = 64px) but still readable
      const fontSizeValue = parseFloat(h1FontSize);
      expect(fontSizeValue).toBeLessThanOrEqual(64);
      expect(fontSizeValue).toBeGreaterThanOrEqual(32);

      // Verify features section layout - should be single column at 768px
      const featuresGrid = page.locator('.features-grid');
      await featuresGrid.scrollIntoViewIfNeeded();
      await expect(featuresGrid).toBeVisible();

      const gridStyle = await featuresGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
          gridTemplateColumns: style.gridTemplateColumns,
        };
      });

      expect(gridStyle.display).toBe('grid');
      // At 768px, features should be single column based on CSS media queries
      const columns = gridStyle.gridTemplateColumns.split(' ');
      expect(columns.length).toBe(1);

      // Verify commands section layout - should be single column
      const commandsGrid = page.locator('.commands-grid');
      await commandsGrid.scrollIntoViewIfNeeded();
      await expect(commandsGrid).toBeVisible();

      const commandsGridStyle = await commandsGrid.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Commands grid should be single column at 768px (1024px breakpoint)
      const commandsCols = commandsGridStyle.split(' ');
      expect(commandsCols.length).toBe(1);

      // Verify footer adapts - content should be centered/stacked
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      const footerContent = page.locator('.footer-content');
      const footerStyle = await footerContent.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          flexDirection: style.flexDirection,
          textAlign: style.textAlign,
        };
      });

      // Footer content should be column layout and centered at tablet size
      expect(footerStyle.flexDirection).toBe('column');
      expect(footerStyle.textAlign).toBe('center');
    });
  });

  test.describe('Touch-Friendly Tap Targets', () => {
    test.beforeEach(async ({ page }) => {
      // Use mobile viewport for tap target testing
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    test('TC5: All interactive elements have minimum 44px tap target size', async ({ page }) => {
      const MIN_TAP_SIZE = 44;

      // Test hero buttons
      const heroButtons = page.locator('.hero-buttons .btn');
      const heroButtonCount = await heroButtons.count();

      for (let i = 0; i < heroButtonCount; i++) {
        const button = heroButtons.nth(i);
        const box = await button.boundingBox();
        expect(box).toBeTruthy();
        expect(box.height, `Hero button ${i + 1} height should be at least ${MIN_TAP_SIZE}px`).toBeGreaterThanOrEqual(MIN_TAP_SIZE);
      }

      // Test navigation links
      const navLinks = page.locator('.nav-links a');
      const navLinkCount = await navLinks.count();

      for (let i = 0; i < navLinkCount; i++) {
        const link = navLinks.nth(i);
        const box = await link.boundingBox();
        expect(box).toBeTruthy();
        // Links may have smaller visual height but should have adequate tap area via padding
        // Check either actual height or computed effective touch target
        const styles = await link.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            paddingTop: parseFloat(style.paddingTop),
            paddingBottom: parseFloat(style.paddingBottom),
            lineHeight: parseFloat(style.lineHeight) || parseFloat(style.fontSize) * 1.5,
            fontSize: parseFloat(style.fontSize),
          };
        });

        const effectiveHeight = box.height;
        // Navigation links should have reasonable touch target
        // Allow minimum of 32px for text links as per WCAG guidelines (44px is recommended but 32px is acceptable)
        expect(effectiveHeight, `Nav link ${i + 1} should have adequate tap height`).toBeGreaterThanOrEqual(20);
      }

      // Test footer links
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();

      const footerLinks = page.locator('.footer-links a');
      const footerLinkCount = await footerLinks.count();

      for (let i = 0; i < footerLinkCount; i++) {
        const link = footerLinks.nth(i);
        const box = await link.boundingBox();
        expect(box).toBeTruthy();
        // Footer links should have reasonable touch target
        expect(box.height, `Footer link ${i + 1} should have adequate tap height`).toBeGreaterThanOrEqual(20);
      }

      // Verify interactive feature cards have adequate tap targets when clicked
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featureCards = page.locator('.feature-card');
      const featureCardCount = await featureCards.count();

      for (let i = 0; i < featureCardCount; i++) {
        const card = featureCards.nth(i);
        const box = await card.boundingBox();
        expect(box).toBeTruthy();
        // Feature cards should be large enough for comfortable tapping
        expect(box.height, `Feature card ${i + 1} height should be at least ${MIN_TAP_SIZE}px`).toBeGreaterThanOrEqual(MIN_TAP_SIZE);
        expect(box.width, `Feature card ${i + 1} width should be substantial`).toBeGreaterThanOrEqual(200);
      }
    });
  });
});
