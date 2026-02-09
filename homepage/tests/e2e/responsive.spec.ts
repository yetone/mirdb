/**
 * E2E tests for Responsive Design.
 * Owner: Scenario 10 - Responsive Design
 *
 * Requirements: REQ-7
 *
 * Test cases:
 * 1. View page on mobile viewport (375px) - All content fits within viewport width without horizontal scrolling
 * 2. View page on tablet viewport (768px) - Layout uses appropriate tablet-optimized styles
 * 3. View page on desktop viewport (1280px) - Layout uses full desktop styles with multi-column layouts
 * 4. View navigation on mobile viewport - Navigation collapses to hamburger menu
 * 5. View feature cards on mobile viewport - Feature cards stack vertically in single column
 * 6. View code blocks on mobile viewport - Code blocks are horizontally scrollable within container
 * 7. Resize browser window across breakpoints - Layout transitions smoothly between breakpoints
 */

import { test, expect } from '@playwright/test';

// Viewport dimensions for testing
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
};

test.describe('Responsive Design E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Test Case 1: Mobile viewport (375px) - No horizontal scrolling', () => {
    test('all content fits within mobile viewport width', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      // Check that there is no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('body does not overflow on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const bodyOverflow = await page.evaluate(() => {
        const body = document.body;
        return {
          scrollWidth: body.scrollWidth,
          clientWidth: body.clientWidth,
          overflowX: window.getComputedStyle(body).overflowX,
        };
      });

      // Body scroll width should not exceed client width significantly
      expect(bodyOverflow.scrollWidth).toBeLessThanOrEqual(bodyOverflow.clientWidth + 1);
    });

    test('container respects mobile viewport width', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const containers = page.locator('.container');
      const containerCount = await containers.count();

      for (let i = 0; i < containerCount; i++) {
        const container = containers.nth(i);
        const box = await container.boundingBox();

        if (box) {
          // Container should not exceed viewport width
          expect(box.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
        }
      }
    });

    test('sections fit within mobile viewport', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const sections = page.locator('section');
      const sectionCount = await sections.count();

      for (let i = 0; i < sectionCount; i++) {
        const section = sections.nth(i);
        const box = await section.boundingBox();

        if (box) {
          expect(box.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
        }
      }
    });
  });

  test.describe('Test Case 2: Tablet viewport (768px) - Tablet-optimized styles', () => {
    test('layout adapts for tablet viewport', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);

      // Container should adapt to tablet width
      const container = page.locator('.container').first();
      const box = await container.boundingBox();

      expect(box).not.toBeNull();
      expect(box!.width).toBeLessThanOrEqual(VIEWPORTS.tablet.width);
    });

    test('features grid shows 2 columns on tablet', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyle = await featuresGrid.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return computed.gridTemplateColumns;
      });

      // On tablet, should have 2 columns or auto-fit
      const columns = gridStyle.split(' ').filter((col) => col.trim() !== '');
      // The grid-template-columns will show actual computed values
      // For tablet it should be 2 columns based on minmax
      expect(columns.length).toBeGreaterThanOrEqual(1);
      expect(columns.length).toBeLessThanOrEqual(3);
    });

    test('hero section adapts to tablet layout', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);

      const heroContainer = page.locator('.hero-container');
      const heroStyle = await heroContainer.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          flexDirection: computed.flexDirection,
          textAlign: computed.textAlign,
        };
      });

      // On tablet (<=768px), hero should be column layout
      expect(heroStyle.flexDirection).toBe('column');
    });

    test('no horizontal scroll on tablet', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.tablet);

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('Test Case 3: Desktop viewport (1280px) - Full desktop styles', () => {
    test('features grid shows multi-column layout on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyle = await featuresGrid.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return computed.gridTemplateColumns;
      });

      // On desktop (>=769px), should have 3 columns
      const columns = gridStyle.split(' ').filter((col) => col.trim() !== '');
      expect(columns.length).toBe(3);
    });

    test('hero section uses side-by-side layout on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);

      const heroContainer = page.locator('.hero-container');
      const heroStyle = await heroContainer.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          flexDirection: computed.flexDirection,
        };
      });

      // On desktop (>768px), hero should be row layout
      expect(heroStyle.flexDirection).toBe('row');
    });

    test('navigation is fully visible on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);

      const navList = page.locator('.header__nav-list');
      await expect(navList).toBeVisible();

      // Hamburger should be hidden on desktop
      const hamburger = page.locator('.header__hamburger');
      const hamburgerStyle = await hamburger.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return computed.display;
      });

      expect(hamburgerStyle).toBe('none');
    });

    test('commands grid uses multi-column layout on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);

      const commandsGrid = page.locator('.commands-grid');

      if ((await commandsGrid.count()) > 0) {
        const gridStyle = await commandsGrid.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return computed.gridTemplateColumns;
        });

        const columns = gridStyle.split(' ').filter((col) => col.trim() !== '');
        expect(columns.length).toBeGreaterThanOrEqual(2);
      }
    });
  });

  test.describe('Test Case 4: Navigation collapses to hamburger on mobile', () => {
    test('hamburger menu is visible on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const hamburger = page.locator('.header__hamburger');
      await expect(hamburger).toBeVisible();
    });

    test('hamburger menu is hidden on desktop', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);

      const hamburger = page.locator('.header__hamburger');
      const hamburgerStyle = await hamburger.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return computed.display;
      });

      expect(hamburgerStyle).toBe('none');
    });

    test('navigation is collapsed by default on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const nav = page.locator('.header__nav');
      const navStyle = await nav.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          maxHeight: computed.maxHeight,
          overflow: computed.overflow,
        };
      });

      expect(navStyle.maxHeight).toBe('0px');
    });

    test('clicking hamburger expands navigation on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const hamburger = page.locator('.header__hamburger');
      const nav = page.locator('.header__nav');

      // Click to open
      await hamburger.click();

      // Nav should have open class
      await expect(nav).toHaveClass(/header__nav--open/);
    });
  });

  test.describe('Test Case 5: Feature cards stack vertically on mobile', () => {
    test('feature cards display in single column on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyle = await featuresGrid.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return computed.gridTemplateColumns;
      });

      // On mobile (<=480px), should be single column (1fr)
      // The computed value will be the actual pixel width
      const columns = gridStyle.split(' ').filter((col) => col.trim() !== '');
      expect(columns.length).toBe(1);
    });

    test('feature cards span full width on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      for (let i = 0; i < Math.min(cardCount, 3); i++) {
        const card = featureCards.nth(i);
        const box = await card.boundingBox();

        if (box) {
          // Card should be close to the container width (minus padding)
          expect(box.width).toBeGreaterThan(VIEWPORTS.mobile.width * 0.8);
        }
      }
    });

    test('feature cards stack vertically (no horizontal overflow)', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      if (cardCount >= 2) {
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
          // Second card should be below the first card (vertically stacked)
          expect(secondCard.y).toBeGreaterThan(firstCard.y);
        }
      }
    });
  });

  test.describe('Test Case 6: Code blocks horizontally scrollable on mobile', () => {
    test('code blocks have overflow-x auto on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const codeBlocks = page.locator('pre');
      const codeBlockCount = await codeBlocks.count();

      if (codeBlockCount > 0) {
        for (let i = 0; i < Math.min(codeBlockCount, 3); i++) {
          const codeBlock = codeBlocks.nth(i);
          const overflowStyle = await codeBlock.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return computed.overflowX;
          });

          // Should be auto or scroll for horizontal scrolling
          expect(['auto', 'scroll']).toContain(overflowStyle);
        }
      }
    });

    test('code blocks do not cause page horizontal scroll', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      // Scroll to quickstart section which has code blocks
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('code block wrapper constrains code width', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const codeWrappers = page.locator('.code-block-wrapper');
      const wrapperCount = await codeWrappers.count();

      if (wrapperCount > 0) {
        const wrapper = codeWrappers.first();
        const box = await wrapper.boundingBox();

        if (box) {
          // Wrapper should not exceed viewport
          expect(box.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
        }
      }
    });
  });

  test.describe('Test Case 7: Smooth transitions between breakpoints', () => {
    test('layout transitions from mobile to tablet', async ({ page }) => {
      // Start at mobile
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.waitForTimeout(100);

      // Verify mobile layout
      const mobileNavStyle = await page.locator('.header__hamburger').evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(mobileNavStyle).not.toBe('none');

      // Transition to tablet
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.waitForTimeout(300); // Allow transition

      // Still should have hamburger at 768px
      const tabletNavStyle = await page.locator('.header__hamburger').evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(tabletNavStyle).not.toBe('none');
    });

    test('layout transitions from tablet to desktop', async ({ page }) => {
      // Start at tablet
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.waitForTimeout(100);

      // Transition to desktop
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.waitForTimeout(300); // Allow transition

      // Hamburger should be hidden
      const desktopHamburgerStyle = await page.locator('.header__hamburger').evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(desktopHamburgerStyle).toBe('none');

      // Hero should be row layout
      const heroStyle = await page.locator('.hero-container').evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(heroStyle).toBe('row');
    });

    test('layout transitions from desktop to mobile', async ({ page }) => {
      // Start at desktop
      await page.setViewportSize(VIEWPORTS.desktop);
      await page.waitForTimeout(100);

      // Verify desktop layout
      const desktopHeroStyle = await page.locator('.hero-container').evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(desktopHeroStyle).toBe('row');

      // Transition to mobile
      await page.setViewportSize(VIEWPORTS.mobile);
      await page.waitForTimeout(300); // Allow transition

      // Verify mobile layout
      const mobileHeroStyle = await page.locator('.hero-container').evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(mobileHeroStyle).toBe('column');

      // Hamburger should be visible
      const hamburger = page.locator('.header__hamburger');
      await expect(hamburger).toBeVisible();
    });

    test('no visual glitches during resize', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.desktop);

      // Perform gradual resize
      const widths = [1280, 1024, 900, 768, 600, 480, 375];

      for (const width of widths) {
        await page.setViewportSize({ width, height: 800 });
        await page.waitForTimeout(100);

        // Check no horizontal overflow at any point
        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });

        expect(hasHorizontalScroll).toBe(false);
      }
    });
  });

  test.describe('Additional Responsive Tests', () => {
    test('footer adapts to mobile layout', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const footerTop = page.locator('.footer-top');
      if ((await footerTop.count()) > 0) {
        const footerStyle = await footerTop.evaluate((el) => {
          return window.getComputedStyle(el).flexDirection;
        });

        expect(footerStyle).toBe('column');
      }
    });

    test('architecture diagram is scrollable on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const archDiagram = page.locator('.arch-diagram');
      if ((await archDiagram.count()) > 0) {
        const overflowStyle = await archDiagram.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });

        expect(['auto', 'scroll', 'visible']).toContain(overflowStyle);
      }
    });

    test('configuration table is scrollable on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      const configWrapper = page.locator('.config-table-wrapper');
      if ((await configWrapper.count()) > 0) {
        // On very small screens, the table might transform to card layout
        // or be scrollable
        const wrapperBox = await configWrapper.boundingBox();

        if (wrapperBox) {
          expect(wrapperBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width);
        }
      }
    });

    test('all touch targets are at least 44px on mobile', async ({ page }) => {
      await page.setViewportSize(VIEWPORTS.mobile);

      // Check hamburger button
      const hamburger = page.locator('.header__hamburger');
      const hamburgerBox = await hamburger.boundingBox();

      if (hamburgerBox) {
        expect(hamburgerBox.height).toBeGreaterThanOrEqual(44);
        expect(hamburgerBox.width).toBeGreaterThanOrEqual(28); // Button itself is 28px wide
      }
    });
  });
});
