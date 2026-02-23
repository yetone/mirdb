/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 7, 8, 9 - Responsive Design
 *
 * End-to-end tests for responsive design:
 * - Mobile viewport tests (320px, 375px, 414px)
 * - Tablet viewport tests (768px, 1024px)
 * - Desktop viewport tests (1024px, 1440px, 1920px)
 * - No horizontal scroll at any size
 * - Touch target sizes on mobile
 * - Content max-width on large screens
 */

import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile (320px-767px)', () => {
  test.describe('320px viewport (minimum mobile)', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('TC1: Page loads without horizontal scrollbar on body at 320px', async ({ page }) => {
      // Check that body doesn't have horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify overflow-x is hidden or auto, not causing visible scrollbar
      const bodyOverflowX = await page.evaluate(() => {
        return window.getComputedStyle(document.body).overflowX;
      });
      // Body should not have visible horizontal scrollbar
      expect(['hidden', 'auto', 'visible']).toContain(bodyOverflowX);

      // Double check - scroll width should not exceed client width
      const scrollInfo = await page.evaluate(() => {
        return {
          scrollWidth: document.documentElement.scrollWidth,
          clientWidth: document.documentElement.clientWidth
        };
      });
      expect(scrollInfo.scrollWidth).toBeLessThanOrEqual(scrollInfo.clientWidth);
    });

    test('TC2: Hero content stacks vertically and remains readable at 320px', async ({ page }) => {
      // Verify hero section exists
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Verify hero content is visible
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();

      const heroHeadline = page.locator('.hero-headline');
      await expect(heroHeadline).toBeVisible();

      const heroTagline = page.locator('.hero-tagline');
      await expect(heroTagline).toBeVisible();

      const heroCta = page.locator('.hero-cta');
      await expect(heroCta).toBeVisible();

      // Check that headline font size is readable (at least 16px equivalent)
      const headlineFontSize = await heroHeadline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(headlineFontSize).toBeGreaterThanOrEqual(16);

      // Verify hero layout is column (stacked vertically)
      const heroFlexDirection = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(heroFlexDirection).toBe('column');

      // Verify all hero content fits within viewport width
      const heroBox = await heroSection.boundingBox();
      expect(heroBox.width).toBeLessThanOrEqual(320);
    });

    test('TC3: Code blocks have horizontal scroll or proper wrapping at 320px', async ({ page }) => {
      // Scroll to quickstart section
      const quickstartSection = page.locator('#quickstart');
      await quickstartSection.scrollIntoViewIfNeeded();

      // Get all code blocks
      const codeBlocks = page.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Check each code block
      for (let i = 0; i < codeBlockCount; i++) {
        const codeBlock = codeBlocks.nth(i);
        await codeBlock.scrollIntoViewIfNeeded();
        await expect(codeBlock).toBeVisible();

        // Verify code block doesn't overflow viewport
        const codeBlockBox = await codeBlock.boundingBox();
        expect(codeBlockBox.width).toBeLessThanOrEqual(320);

        // Check that pre element has overflow-x set for scrolling
        const preElement = codeBlock.locator('pre');
        const overflowX = await preElement.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });
        expect(['auto', 'scroll']).toContain(overflowX);
      }
    });

    test('TC4: Get Started button has minimum 44px touch target size', async ({ page }) => {
      const ctaButton = page.locator('.hero-cta');
      await expect(ctaButton).toBeVisible();

      const boundingBox = await ctaButton.boundingBox();

      // Verify minimum 44x44px touch target (WCAG 2.5.5)
      expect(boundingBox.width).toBeGreaterThanOrEqual(44);
      expect(boundingBox.height).toBeGreaterThanOrEqual(44);
    });

    test('All interactive elements have proper touch targets at 320px', async ({ page }) => {
      // Check copy buttons in code blocks
      const quickstartSection = page.locator('#quickstart');
      await quickstartSection.scrollIntoViewIfNeeded();

      const copyButtons = page.locator('.code-block__copy');
      const buttonCount = await copyButtons.count();

      for (let i = 0; i < buttonCount; i++) {
        const button = copyButtons.nth(i);
        await button.scrollIntoViewIfNeeded();
        const box = await button.boundingBox();
        // Touch target should be at least 44x44 pixels
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('Navigation is accessible on mobile at 320px', async ({ page }) => {
      // Check that navigation exists
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Check that nav links exist
      const navLinks = page.locator('.nav__links a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // On mobile, navigation should fit within viewport and be accessible
      const navBox = await nav.boundingBox();
      expect(navBox.width).toBeLessThanOrEqual(320);

      // Check that links are focusable (keyboard accessible)
      const firstLink = navLinks.first();
      if (await firstLink.isVisible()) {
        await firstLink.focus();
        await expect(firstLink).toBeFocused();
      }
    });

    test('Sections display correctly without overflow at 320px', async ({ page }) => {
      const sections = ['#hero', '#quickstart', '#features', '#roadmap', '#configuration'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();

        // Verify section doesn't overflow viewport width
        const sectionBox = await section.boundingBox();
        expect(sectionBox.width).toBeLessThanOrEqual(320);
      }
    });
  });

  test.describe('375px viewport (iPhone)', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('TC5: Page displays correctly on typical iPhone width (375px)', async ({ page }) => {
      // Check no horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify all major sections are visible and fit
      const sections = ['#hero', '#quickstart', '#features', '#roadmap', '#configuration'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();

        const sectionBox = await section.boundingBox();
        expect(sectionBox.width).toBeLessThanOrEqual(375);
      }

      // Check hero content is readable
      const heroHeadline = page.locator('.hero-headline');
      await expect(heroHeadline).toBeVisible();
      const headlineFontSize = await heroHeadline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(headlineFontSize).toBeGreaterThanOrEqual(16);
    });

    test('Footer is accessible and displays correctly at 375px', async ({ page }) => {
      const footer = page.locator('[data-testid="footer"]');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Footer should fit within viewport
      const footerBox = await footer.boundingBox();
      expect(footerBox.width).toBeLessThanOrEqual(375);

      // Footer links should be visible
      const githubLink = page.locator('[data-testid="github-link"]');
      await expect(githubLink).toBeVisible();
    });
  });

  test.describe('414px viewport (iPhone Plus)', () => {
    test.use({ viewport: { width: 414, height: 736 } });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('TC6: Page displays correctly on larger phone width (414px)', async ({ page }) => {
      // Check no horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);

      // Verify hero section displays correctly
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      const heroBox = await heroSection.boundingBox();
      expect(heroBox.width).toBeLessThanOrEqual(414);

      // Verify all sections are visible
      const sections = ['#hero', '#quickstart', '#features', '#roadmap', '#configuration'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();

        const sectionBox = await section.boundingBox();
        expect(sectionBox.width).toBeLessThanOrEqual(414);
      }

      // Verify footer
      const footer = page.locator('[data-testid="footer"]');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });

    test('Features list displays correctly at 414px', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Check feature items are visible
      const featureItems = page.locator('.features__item');
      const itemCount = await featureItems.count();
      expect(itemCount).toBeGreaterThan(0);

      // Each feature item should fit within viewport
      for (let i = 0; i < itemCount; i++) {
        const item = featureItems.nth(i);
        await item.scrollIntoViewIfNeeded();
        await expect(item).toBeVisible();

        const itemBox = await item.boundingBox();
        expect(itemBox.width).toBeLessThanOrEqual(414);
      }
    });

    test('Roadmap section displays correctly at 414px', async ({ page }) => {
      const roadmapSection = page.locator('#roadmap');
      await roadmapSection.scrollIntoViewIfNeeded();

      // Check roadmap items are visible
      const roadmapItems = page.locator('.roadmap__item');
      const itemCount = await roadmapItems.count();
      expect(itemCount).toBeGreaterThan(0);

      // Each roadmap item should fit within viewport
      for (let i = 0; i < itemCount; i++) {
        const item = roadmapItems.nth(i);
        await item.scrollIntoViewIfNeeded();
        await expect(item).toBeVisible();

        const itemBox = await item.boundingBox();
        expect(itemBox.width).toBeLessThanOrEqual(414);
      }

      // Coming soon badges should be visible
      const badges = page.locator('.roadmap__badge');
      const badgeCount = await badges.count();
      expect(badgeCount).toBeGreaterThan(0);
    });
  });

  test.describe('Mobile responsive general tests', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('Typography is readable on mobile', async ({ page }) => {
      // Check main text sizes
      const body = page.locator('body');
      const bodyFontSize = await body.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Body font should be at least 16px for readability
      expect(bodyFontSize).toBeGreaterThanOrEqual(16);

      // Check paragraph line height
      const paragraph = page.locator('p').first();
      if (await paragraph.count() > 0) {
        const lineHeight = await paragraph.evaluate((el) => {
          const style = window.getComputedStyle(el);
          const lineHeightValue = parseFloat(style.lineHeight);
          const fontSize = parseFloat(style.fontSize);
          return lineHeightValue / fontSize;
        });
        // Line height should be at least 1.4 for readability
        expect(lineHeight).toBeGreaterThanOrEqual(1.4);
      }
    });

    test('Images scale appropriately on mobile', async ({ page }) => {
      // Check hero logo scales
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();

      const logoBox = await heroLogo.boundingBox();
      // Logo should fit within viewport
      expect(logoBox.width).toBeLessThanOrEqual(320);

      // Check CircleCI badge in footer
      const footer = page.locator('[data-testid="footer"]');
      await footer.scrollIntoViewIfNeeded();

      const badgeImg = page.locator('[data-testid="circleci-img"]');
      if (await badgeImg.isVisible()) {
        const badgeBox = await badgeImg.boundingBox();
        expect(badgeBox.width).toBeLessThanOrEqual(320);
      }
    });

    test('Content has adequate padding on mobile', async ({ page }) => {
      // Main content should have horizontal padding
      const main = page.locator('main');
      const mainPadding = await main.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          left: parseFloat(style.paddingLeft),
          right: parseFloat(style.paddingRight)
        };
      });

      // Should have some padding for readability
      expect(mainPadding.left + mainPadding.right).toBeGreaterThan(0);
    });

    test('Tech badges wrap appropriately on mobile', async ({ page }) => {
      const techStack = page.locator('.hero-tech-stack');
      await expect(techStack).toBeVisible();

      // Check that flex-wrap is enabled
      const flexWrap = await techStack.evaluate((el) => {
        return window.getComputedStyle(el).flexWrap;
      });
      expect(flexWrap).toBe('wrap');

      // Verify badges are visible
      const badges = page.locator('.tech-badge');
      const badgeCount = await badges.count();
      expect(badgeCount).toBeGreaterThan(0);

      // Each badge should fit within viewport
      for (let i = 0; i < badgeCount; i++) {
        const badge = badges.nth(i);
        const badgeBox = await badge.boundingBox();
        expect(badgeBox.width).toBeLessThanOrEqual(320);
      }
    });
  });
});
