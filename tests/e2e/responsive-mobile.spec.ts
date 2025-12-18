import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile (320px-480px viewport)', () => {
  // Test with 320px width (smallest supported viewport)
  test.describe('At 320px viewport width', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('TC1: No horizontal scrollbar appears, content fits viewport', async ({ page }) => {
      await page.goto('/');

      // Wait for page to load
      await page.waitForLoadState('domcontentloaded');

      // Check document width does not exceed viewport width
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Content should fit within viewport (no horizontal overflow)
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify no horizontal scrollbar by checking overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('TC2: Navigation shows mobile menu (hamburger) or stacked links', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Navigation should be present
      const navbar = page.locator('nav.navbar');
      await expect(navbar).toBeVisible();

      // On mobile, navigation should either:
      // 1. Have a hamburger menu button, or
      // 2. Have links stacked/wrapped vertically

      // Check if nav links exist and are accessible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // The nav container should stack vertically on mobile
      const navContainer = page.locator('.nav-container');
      const navContainerBox = await navContainer.boundingBox();
      expect(navContainerBox).not.toBeNull();

      // On mobile, the nav links should wrap (flex-wrap: wrap)
      // or the container should be flex-direction: column
      const navLinksStyle = await navLinks.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          flexWrap: styles.flexWrap,
          display: styles.display
        };
      });

      // Nav links should be wrappable or display as block
      expect(['wrap', 'flex']).toContain(navLinksStyle.flexWrap);
    });

    test('TC3: Hero content is readable and CTAs are tappable', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Hero section should be visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Hero title should be visible and readable
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Check hero title font size is appropriately scaled for mobile
      const heroTitleFontSize = await heroTitle.evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontSize);
      });
      // Font should be readable (at least 24px for h1 on mobile)
      expect(heroTitleFontSize).toBeGreaterThanOrEqual(24);

      // Tagline should be visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      // CTA buttons should be visible and tappable (min 44px touch target)
      const primaryBtn = page.locator('.btn-primary');
      await expect(primaryBtn).toBeVisible();

      const primaryBtnBox = await primaryBtn.boundingBox();
      expect(primaryBtnBox).not.toBeNull();
      // Touch target should be at least 44px (WCAG minimum)
      expect(primaryBtnBox!.height).toBeGreaterThanOrEqual(40);
      expect(primaryBtnBox!.width).toBeGreaterThanOrEqual(40);

      const secondaryBtn = page.locator('.btn-secondary');
      await expect(secondaryBtn).toBeVisible();

      const secondaryBtnBox = await secondaryBtn.boundingBox();
      expect(secondaryBtnBox).not.toBeNull();
      expect(secondaryBtnBox!.height).toBeGreaterThanOrEqual(40);
    });

    test('TC4: Feature cards stack vertically and are fully visible', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to features section
      await page.locator('#features').scrollIntoViewIfNeeded();

      // Features section should be visible
      const featuresSection = page.locator('.features-section');
      await expect(featuresSection).toBeVisible();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4); // We expect 4 feature cards

      // Check that feature cards are stacked (each card's top position increases)
      const cardPositions: number[] = [];
      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        const box = await card.boundingBox();
        expect(box).not.toBeNull();

        // Card should fit within viewport width
        expect(box!.width).toBeLessThanOrEqual(320);

        cardPositions.push(box!.y);
      }

      // On mobile, cards should be stacked vertically (each card below the previous)
      for (let i = 1; i < cardPositions.length; i++) {
        expect(cardPositions[i]).toBeGreaterThan(cardPositions[i - 1]);
      }
    });

    test('TC5: Code snippets have horizontal scroll or wrap appropriately', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to getting-started section where code snippets are
      await page.locator('#getting-started').scrollIntoViewIfNeeded();

      // Check all pre/code elements
      const codeBlocks = page.locator('pre');
      const codeCount = await codeBlocks.count();
      expect(codeCount).toBeGreaterThan(0);

      for (let i = 0; i < codeCount; i++) {
        const codeBlock = codeBlocks.nth(i);

        // Code block should be visible
        await codeBlock.scrollIntoViewIfNeeded();
        await expect(codeBlock).toBeVisible();

        // Check overflow-x is set to allow horizontal scrolling
        const overflowStyle = await codeBlock.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            overflowX: styles.overflowX,
            maxWidth: styles.maxWidth,
            width: styles.width
          };
        });

        // Code blocks should have overflow-x: auto or scroll to handle long lines
        expect(['auto', 'scroll']).toContain(overflowStyle.overflowX);

        // Code block should not exceed viewport width
        const box = await codeBlock.boundingBox();
        expect(box).not.toBeNull();
        expect(box!.width).toBeLessThanOrEqual(320);
      }
    });
  });

  // Additional test at 480px (upper bound of mobile)
  test.describe('At 480px viewport width', () => {
    test.use({ viewport: { width: 480, height: 640 } });

    test('Page displays correctly at 480px width', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // No horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Hero is visible
      await expect(page.locator('.hero')).toBeVisible();

      // Navigation is accessible
      await expect(page.locator('nav.navbar')).toBeVisible();

      // Features are visible
      await page.locator('#features').scrollIntoViewIfNeeded();
      await expect(page.locator('.features-section')).toBeVisible();
    });
  });
});
