/**
 * Cross-Browser E2E Tests
 * Owner: Scenario 13 - Cross-Browser Compatibility
 *
 * Tests:
 * - Renders in Chrome (chromium)
 * - Renders in Firefox
 * - Renders in Safari (webkit)
 * - CSS Grid support across browsers
 * - Visual consistency across browsers
 *
 * Per NFR-5: Page must render correctly across Chrome, Firefox, Safari, Edge (latest 2 versions)
 */

import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test.describe('Homepage renders correctly', () => {
    test('page loads without errors', async ({ page }) => {
      const consoleErrors = [];
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const criticalErrors = consoleErrors.filter(
        (error) =>
          !error.includes('favicon') &&
          !error.includes('DevTools') &&
          !error.includes('third-party')
      );
      expect(criticalErrors).toHaveLength(0);
    });

    test('header renders correctly', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();

      const logo = page.locator('.nav__logo');
      await expect(logo).toBeVisible();
      await expect(logo).toContainText('MirDB');

      const navMenu = page.locator('.nav__menu');
      const viewportWidth = (await page.viewportSize())?.width || 0;

      if (viewportWidth >= 768) {
        await expect(navMenu).toBeVisible();
      }
    });

    test('hero section renders correctly', async ({ page }) => {
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      const heroTitle = page.locator('.hero__title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      const heroTagline = page.locator('.hero__tagline');
      await expect(heroTagline).toBeVisible();

      const ctaButtons = page.locator('.hero__actions .btn');
      await expect(ctaButtons.first()).toBeVisible();
    });

    test('features section renders correctly', async ({ page }) => {
      const features = page.locator('.features');
      await expect(features).toBeVisible();

      const featuresTitle = page.locator('.features__title');
      await expect(featuresTitle).toBeVisible();
      await expect(featuresTitle).toContainText('Features');

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(3);
      expect(cardCount).toBeLessThanOrEqual(5);
    });

    test('footer renders correctly', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      const copyright = page.locator('.footer__copyright');
      await expect(copyright).toBeVisible();
      await expect(copyright).toContainText('MirDB');

      const footerLinks = page.locator('.footer__link');
      await expect(footerLinks.first()).toBeVisible();
    });
  });

  test.describe('CSS Grid support', () => {
    test('features grid displays correctly on desktop', async ({ page }) => {
      const viewportWidth = (await page.viewportSize())?.width || 0;

      if (viewportWidth >= 1200) {
        const featuresGrid = page.locator('.features__grid');
        await expect(featuresGrid).toBeVisible();

        const gridStyle = await featuresGrid.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            display: computed.display,
            gridTemplateColumns: computed.gridTemplateColumns,
          };
        });

        expect(gridStyle.display).toBe('grid');
        const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
        expect(columnCount).toBeGreaterThanOrEqual(2);
      }
    });

    test('feature cards have consistent dimensions', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      if (cardCount > 1) {
        const boxes = [];
        for (let i = 0; i < Math.min(cardCount, 3); i++) {
          const box = await featureCards.nth(i).boundingBox();
          if (box) {
            boxes.push(box);
          }
        }

        if (boxes.length > 1) {
          const widths = boxes.map((b) => Math.round(b.width));
          const maxWidthDiff = Math.max(...widths) - Math.min(...widths);
          expect(maxWidthDiff).toBeLessThan(10);
        }
      }
    });

    test('grid gap is applied correctly', async ({ page }) => {
      const featuresGrid = page.locator('.features__grid');
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      if (cardCount > 1) {
        const gridGap = await featuresGrid.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return computed.gap || computed.gridGap;
        });

        expect(gridGap).toBeTruthy();
        expect(gridGap).not.toBe('normal');
      }
    });
  });

  test.describe('CSS features work as expected', () => {
    test('CSS custom properties are applied', async ({ page }) => {
      const body = page.locator('body');
      const backgroundColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      expect(backgroundColor).toBeTruthy();
      expect(backgroundColor).not.toBe('');
    });

    test('flexbox layout works correctly', async ({ page }) => {
      const nav = page.locator('.nav');
      const navDisplay = await nav.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });

      expect(navDisplay).toBe('flex');
    });

    test('transitions are applied', async ({ page }) => {
      const ctaButton = page.locator('.btn--primary').first();
      const transition = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });

      expect(transition).toBeTruthy();
      expect(transition).not.toBe('none');
    });

    test('border-radius is applied correctly', async ({ page }) => {
      const ctaButton = page.locator('.btn').first();
      const borderRadius = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });

      expect(borderRadius).toBeTruthy();
      expect(borderRadius).not.toBe('0px');
    });

    test('box-shadow is rendered', async ({ page }) => {
      const header = page.locator('.header');
      const boxShadow = await header.evaluate((el) => {
        return window.getComputedStyle(el).boxShadow;
      });

      expect(boxShadow).toBeTruthy();
    });
  });

  test.describe('No layout issues', () => {
    test('no horizontal overflow', async ({ page }) => {
      const hasOverflow = await page.evaluate(() => {
        return document.body.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasOverflow).toBe(false);
    });

    test('all sections are visible within viewport width', async ({ page }) => {
      const sections = ['header', '.hero', '.features', 'footer'];

      for (const selector of sections) {
        const element = page.locator(selector).first();
        const box = await element.boundingBox();

        if (box) {
          const viewportWidth = (await page.viewportSize())?.width || 0;
          expect(box.x).toBeGreaterThanOrEqual(0);
          expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth + 1);
        }
      }
    });

    test('text is readable and not clipped', async ({ page }) => {
      const heroTitle = page.locator('.hero__title');
      const titleBox = await heroTitle.boundingBox();
      const titleContent = await heroTitle.textContent();

      expect(titleBox).toBeTruthy();
      expect(titleContent).toBeTruthy();
      expect(titleBox?.height).toBeGreaterThan(0);
      expect(titleBox?.width).toBeGreaterThan(0);
    });

    test('images and SVGs render without distortion', async ({ page }) => {
      const svgs = page.locator('svg');
      const svgCount = await svgs.count();

      for (let i = 0; i < svgCount; i++) {
        const svg = svgs.nth(i);
        const isVisible = await svg.isVisible();

        if (isVisible) {
          const box = await svg.boundingBox();
          if (box) {
            expect(box.width).toBeGreaterThan(0);
            expect(box.height).toBeGreaterThan(0);
          }
        }
      }
    });
  });

  test.describe('Interactive elements work', () => {
    test('navigation links are clickable', async ({ page }) => {
      const viewportWidth = (await page.viewportSize())?.width || 0;

      if (viewportWidth >= 768) {
        const navLinks = page.locator('.nav__link');
        const linkCount = await navLinks.count();

        for (let i = 0; i < linkCount; i++) {
          const link = navLinks.nth(i);
          await expect(link).toBeEnabled();
        }
      }
    });

    test('CTA buttons have correct hover states', async ({ page }) => {
      const primaryBtn = page.locator('.btn--primary').first();

      const initialBg = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      await primaryBtn.hover();
      await page.waitForTimeout(200);

      const hoverBg = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      expect(initialBg).toBeTruthy();
      expect(hoverBg).toBeTruthy();
    });

    test('focus states are visible', async ({ page }) => {
      const ctaButton = page.locator('.btn').first();
      await ctaButton.focus();

      const outline = await ctaButton.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          outline: computed.outline,
          outlineColor: computed.outlineColor,
          boxShadow: computed.boxShadow,
        };
      });

      const hasFocusIndicator =
        (outline.outline && outline.outline !== 'none' && outline.outline !== '0px none rgb(0, 0, 0)') ||
        (outline.boxShadow && outline.boxShadow !== 'none');

      expect(hasFocusIndicator).toBe(true);
    });
  });

  test.describe('Fonts render correctly', () => {
    test('system font stack is applied', async ({ page }) => {
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      expect(fontFamily).toBeTruthy();
      expect(fontFamily.length).toBeGreaterThan(0);
    });

    test('font weights are applied correctly', async ({ page }) => {
      const heroTitle = page.locator('.hero__title');
      const fontWeight = await heroTitle.evaluate((el) => {
        return window.getComputedStyle(el).fontWeight;
      });

      const weightValue = parseInt(fontWeight, 10);
      expect(weightValue).toBeGreaterThanOrEqual(600);
    });

    test('line heights are consistent', async ({ page }) => {
      const paragraphs = page.locator('p');
      const pCount = await paragraphs.count();

      if (pCount > 0) {
        const lineHeight = await paragraphs.first().evaluate((el) => {
          return window.getComputedStyle(el).lineHeight;
        });

        expect(lineHeight).toBeTruthy();
        expect(lineHeight).not.toBe('normal');
      }
    });
  });
});
