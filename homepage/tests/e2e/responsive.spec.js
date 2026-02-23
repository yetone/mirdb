// @ts-check
import { test, expect } from '@playwright/test';

/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 7, 8, 9 - Responsive Design
 *
 * End-to-end tests for responsive design:
 * - Mobile viewport tests (320px, 375px, 414px) - Scenario 7
 * - Tablet viewport tests (768px, 1024px) - Scenario 8
 * - Desktop viewport tests (1024px, 1440px, 1920px) - Scenario 9
 * - No horizontal scroll at any size
 * - Touch target sizes on mobile
 * - Content max-width on large screens
 */

// ============================================================================
// MOBILE VIEWPORT TESTS (Scenario 7 - 320px to 767px)
// ============================================================================

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

// ============================================================================
// TABLET VIEWPORT TESTS (Scenario 8 - 768px to 1023px)
// ============================================================================

test.describe('Tablet Responsive Design (768px - 1023px)', () => {
  test.describe('768px viewport - tablet breakpoint start', () => {
    test.use({ viewport: { width: 768, height: 1024 } });

    test('page displays with tablet-appropriate layout at 768px', async ({ page }) => {
      await page.goto('/');

      // Page should load successfully
      await expect(page).toHaveTitle(/MirDB/);

      // Hero section should be visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Hero headline should be readable
      const headline = page.locator('.hero-headline');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('MirDB');

      // No horizontal scroll at tablet size
      const body = page.locator('body');
      const bodyBox = await body.boundingBox();
      const viewportWidth = 768;
      expect(bodyBox.width).toBeLessThanOrEqual(viewportWidth);
    });

    test('all sections are visible and properly laid out at 768px', async ({ page }) => {
      await page.goto('/');

      // Check all main sections are visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#roadmap')).toBeVisible();
      await expect(page.locator('#configuration')).toBeVisible();
      await expect(page.locator('.footer')).toBeVisible();
    });

    test('navigation is accessible at tablet size', async ({ page }) => {
      await page.goto('/');

      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Navigation links should be visible
      const navLinks = page.locator('.nav__links a');
      const linksCount = await navLinks.count();
      expect(linksCount).toBeGreaterThan(0);

      // Each link should be visible
      for (let i = 0; i < linksCount; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    });

    test('features section displays in 2-column grid at tablet size', async ({ page }) => {
      await page.goto('/');

      const featuresList = page.locator('.features__list');
      await expect(featuresList).toBeVisible();

      // Get computed styles to verify grid layout
      const gridStyles = await featuresList.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns
        };
      });

      // Should be a grid display
      expect(gridStyles.display).toBe('grid');

      // Should have 2 columns (the value will contain column widths)
      const columns = gridStyles.gridTemplateColumns.split(' ').filter(col => col.trim() !== '');
      expect(columns.length).toBe(2);
    });

    test('text remains readable at tablet size', async ({ page }) => {
      await page.goto('/');

      // Check hero tagline is visible and readable
      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();
      const taglineBox = await tagline.boundingBox();
      expect(taglineBox.width).toBeGreaterThan(200);

      // Check feature descriptions are readable
      const featureDesc = page.locator('.features__item-description').first();
      await expect(featureDesc).toBeVisible();
      const featureDescBox = await featureDesc.boundingBox();
      expect(featureDescBox.width).toBeGreaterThan(150);
    });

    test('code blocks are readable and do not overflow at 768px', async ({ page }) => {
      await page.goto('/');

      // Navigate to quickstart section
      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      // Check first code block doesn't overflow viewport
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();
      const codeBlockBox = await firstCodeBlock.boundingBox();
      expect(codeBlockBox.width).toBeLessThanOrEqual(768);
    });

    test('CTA button has appropriate touch target size', async ({ page }) => {
      await page.goto('/');

      const ctaButton = page.locator('.hero-cta');
      await expect(ctaButton).toBeVisible();

      const ctaBox = await ctaButton.boundingBox();
      // Touch target should be at least 44px (WCAG recommendation)
      expect(ctaBox.height).toBeGreaterThanOrEqual(44);
      expect(ctaBox.width).toBeGreaterThanOrEqual(44);
    });
  });

  test.describe('900px viewport - mid tablet', () => {
    test.use({ viewport: { width: 900, height: 1024 } });

    test('page displays correctly at mid-tablet size (900px)', async ({ page }) => {
      await page.goto('/');

      // Page should load successfully
      await expect(page).toHaveTitle(/MirDB/);

      // Hero section should be properly sized
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // No horizontal scroll
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(scrollWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('features grid displays properly at 900px', async ({ page }) => {
      await page.goto('/');

      const featureItems = page.locator('.features__item');
      const count = await featureItems.count();
      expect(count).toBeGreaterThan(0);

      // All feature items should be visible
      for (let i = 0; i < count; i++) {
        await expect(featureItems.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('1023px viewport - tablet/desktop boundary', () => {
    test.use({ viewport: { width: 1023, height: 768 } });

    test('page displays correctly at tablet/desktop boundary (1023px)', async ({ page }) => {
      await page.goto('/');

      // Page should load successfully
      await expect(page).toHaveTitle(/MirDB/);

      // All main sections should be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#roadmap')).toBeVisible();
      await expect(page.locator('#configuration')).toBeVisible();
    });

    test('layout is appropriate for upper tablet boundary', async ({ page }) => {
      await page.goto('/');

      // Features should use appropriate layout for tablet
      const featuresList = page.locator('.features__list');
      await expect(featuresList).toBeVisible();

      const gridStyles = await featuresList.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns
        };
      });

      expect(gridStyles.display).toBe('grid');
      // At 1023px boundary, auto-fit can produce 2-3 columns depending on min-width
      // This is acceptable responsive behavior for tablet layout
      const columns = gridStyles.gridTemplateColumns.split(' ').filter(col => col.trim() !== '');
      expect(columns.length).toBeGreaterThanOrEqual(2);
      expect(columns.length).toBeLessThanOrEqual(3);
    });

    test('no horizontal overflow at 1023px', async ({ page }) => {
      await page.goto('/');

      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(scrollWidth).toBeLessThanOrEqual(viewportWidth);
    });
  });

  test.describe('1024px viewport - boundary test', () => {
    test.use({ viewport: { width: 1024, height: 768 } });

    test('page displays correctly at desktop breakpoint (1024px)', async ({ page }) => {
      await page.goto('/');

      // Page should load successfully
      await expect(page).toHaveTitle(/MirDB/);

      // Hero section should be visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // All content sections should be present
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
    });

    test('no horizontal overflow at 1024px boundary', async ({ page }) => {
      await page.goto('/');

      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(scrollWidth).toBeLessThanOrEqual(viewportWidth);
    });
  });

  test.describe('tablet orientation changes', () => {
    test('landscape tablet (1024x768) displays correctly', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      await expect(page).toHaveTitle(/MirDB/);
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
    });

    test('portrait tablet (768x1024) displays correctly', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      await expect(page).toHaveTitle(/MirDB/);
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
    });
  });
});
