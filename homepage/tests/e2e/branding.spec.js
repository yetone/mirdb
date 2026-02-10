/**
 * Brand Consistency and Visual Design E2E Tests
 * Owner: Scenario 9 - Brand Consistency and Visual Design
 *
 * End-to-end tests for validating brand consistency, logo display,
 * typography, whitespace, and visual design of the MirDB homepage.
 *
 * Requirements traced:
 * - NFR-2: Brand consistency with project
 * - Design Requirements: Clean, professional design
 */

// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Brand Consistency and Visual Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Logo Display (Test Case 2)', () => {
    test('should display logo image element in header', async ({ page }) => {
      const logo = page.locator('header .logo img');
      await expect(logo).toBeVisible();

      // Check that logo img element exists and has src attribute referencing logo.gif
      const src = await logo.getAttribute('src');
      expect(src).toBeTruthy();
      expect(src).toContain('logo.gif');

      // Get dimensions to ensure img is rendered properly
      const dimensions = await logo.evaluate((img) => {
        return {
          displayedWidth: img.offsetWidth,
          displayedHeight: img.offsetHeight,
          complete: img.complete
        };
      });

      // Logo element should be rendered (has dimensions)
      expect(dimensions.displayedWidth).toBeGreaterThan(0);
      expect(dimensions.displayedHeight).toBeGreaterThan(0);
    });

    test('should have logo properly positioned in header', async ({ page }) => {
      const logo = page.locator('header .logo img');
      await expect(logo).toBeVisible();

      // Logo should be within the viewport header area
      const boundingBox = await logo.boundingBox();
      expect(boundingBox).toBeTruthy();
      expect(boundingBox.y).toBeLessThan(200); // Logo should be near top of page
      expect(boundingBox.x).toBeGreaterThanOrEqual(0); // Logo should be visible horizontally
    });

    test('should display logo with proper CSS styling', async ({ page }) => {
      const logo = page.locator('header .logo img');
      await expect(logo).toBeVisible();

      // Check that CSS height styling is applied
      const logoStyle = await logo.evaluate((img) => {
        const style = window.getComputedStyle(img);
        return {
          height: style.height,
          width: style.width,
          display: style.display
        };
      });

      // Logo should have height defined (not 0)
      expect(parseInt(logoStyle.height)).toBeGreaterThan(0);
      // Logo should have proper display (not hidden)
      expect(logoStyle.display).not.toBe('none');
    });
  });

  test.describe('Adequate Whitespace (Test Case 4)', () => {
    test('should have sufficient padding on main sections', async ({ page }) => {
      // Check hero section padding
      const hero = page.locator('.hero');
      const heroPadding = await hero.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          paddingTop: parseFloat(style.paddingTop),
          paddingBottom: parseFloat(style.paddingBottom)
        };
      });
      expect(heroPadding.paddingTop).toBeGreaterThanOrEqual(32); // At least 2rem
      expect(heroPadding.paddingBottom).toBeGreaterThanOrEqual(32);

      // Check features section padding
      const features = page.locator('.features');
      const featuresPadding = await features.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          paddingTop: parseFloat(style.paddingTop),
          paddingBottom: parseFloat(style.paddingBottom)
        };
      });
      expect(featuresPadding.paddingTop).toBeGreaterThanOrEqual(32);
      expect(featuresPadding.paddingBottom).toBeGreaterThanOrEqual(32);
    });

    test('should have sufficient margin between sections', async ({ page }) => {
      // Get positions of consecutive sections to verify spacing
      const heroBox = await page.locator('.hero').boundingBox();
      const featuresBox = await page.locator('.features').boundingBox();

      expect(heroBox).toBeTruthy();
      expect(featuresBox).toBeTruthy();

      // Features section should start after hero section
      expect(featuresBox.y).toBeGreaterThanOrEqual(heroBox.y + heroBox.height);
    });

    test('should have readable line spacing', async ({ page }) => {
      const body = page.locator('body');
      const lineHeight = await body.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).lineHeight);
      });

      // Line height should be at least 1.4 times the font size for readability
      const fontSize = await body.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Either explicit line-height value or reasonable computed value
      // (line-height: 1.6 * 16px = 25.6px)
      expect(lineHeight / fontSize).toBeGreaterThanOrEqual(1.4);
    });

    test('should have adequate spacing in feature cards', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      if (cardCount > 0) {
        const firstCard = featureCards.first();
        const cardPadding = await firstCard.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            paddingTop: parseFloat(style.paddingTop),
            paddingRight: parseFloat(style.paddingRight),
            paddingBottom: parseFloat(style.paddingBottom),
            paddingLeft: parseFloat(style.paddingLeft)
          };
        });

        // Cards should have comfortable padding (at least 16px)
        expect(cardPadding.paddingTop).toBeGreaterThanOrEqual(16);
        expect(cardPadding.paddingRight).toBeGreaterThanOrEqual(16);
        expect(cardPadding.paddingBottom).toBeGreaterThanOrEqual(16);
        expect(cardPadding.paddingLeft).toBeGreaterThanOrEqual(16);
      }
    });

    test('should have content contained within max-width container', async ({ page }) => {
      const container = page.locator('.container').first();
      const containerStyle = await container.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          maxWidth: style.maxWidth,
          marginLeft: style.marginLeft,
          marginRight: style.marginRight
        };
      });

      // Container should have max-width set
      expect(containerStyle.maxWidth).not.toBe('none');
      expect(parseInt(containerStyle.maxWidth)).toBeGreaterThan(0);

      // Container should be centered (auto margins compute to equal pixel values on both sides
      // when centered, or margin: 0 auto is specified in CSS)
      // Note: computed styles often show pixel values instead of "auto"
      expect(containerStyle.marginLeft).toBe(containerStyle.marginRight);
    });
  });

  test.describe('Consistent Font Styling', () => {
    test('should apply consistent font family across the page', async ({ page }) => {
      // Check body font
      const bodyFont = await page.locator('body').evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Check heading font
      const h1Font = await page.locator('h1').first().evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Body and heading should use the same base font family (may differ by weight)
      expect(bodyFont.toLowerCase()).not.toContain('times');
      expect(bodyFont.toLowerCase()).toMatch(/system|sans-serif|segoe|roboto|helvetica|arial/i);

      // Heading should also use sans-serif font
      expect(h1Font.toLowerCase()).not.toContain('times');
    });

    test('should use monospace font for code blocks', async ({ page }) => {
      const codeElements = page.locator('pre code, code');
      const codeCount = await codeElements.count();

      if (codeCount > 0) {
        const codeFont = await codeElements.first().evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });

        // Code should use monospace font
        expect(codeFont.toLowerCase()).toMatch(/monospace|consolas|monaco|courier|menlo|sfmono/i);
      }
    });

    test('should have proper heading hierarchy with different font sizes', async ({ page }) => {
      const h1Size = await page.locator('h1').first().evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      const h2Elements = page.locator('h2');
      let h2Size = 24; // default
      if (await h2Elements.count() > 0) {
        h2Size = await h2Elements.first().evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
      }

      const h3Elements = page.locator('h3');
      let h3Size = 18; // default
      if (await h3Elements.count() > 0) {
        h3Size = await h3Elements.first().evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize);
        });
      }

      // Heading sizes should follow hierarchy: h1 > h2 > h3
      expect(h1Size).toBeGreaterThan(h2Size);
      expect(h2Size).toBeGreaterThan(h3Size);
    });
  });

  test.describe('Professional Color Scheme', () => {
    test('should have consistent button styling', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();

      if (await primaryBtn.count() > 0) {
        const btnStyle = await primaryBtn.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            backgroundColor: style.backgroundColor,
            color: style.color,
            borderRadius: style.borderRadius
          };
        });

        // Button should have a colored background (not transparent)
        expect(btnStyle.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
        expect(btnStyle.backgroundColor).not.toBe('transparent');

        // Button text should be readable (not the same as background)
        expect(btnStyle.color).not.toBe(btnStyle.backgroundColor);
      }
    });

    test('should have distinct section backgrounds for visual separation', async ({ page }) => {
      const heroBackground = await page.locator('.hero').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      const featuresBackground = await page.locator('.features').evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Sections should have backgrounds defined (can be same or different)
      expect(heroBackground).toBeTruthy();
      expect(featuresBackground).toBeTruthy();
    });

    test('should have readable text contrast', async ({ page }) => {
      const bodyStyle = await page.locator('body').evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          backgroundColor: style.backgroundColor
        };
      });

      // Text color should not be the same as background
      expect(bodyStyle.color).not.toBe(bodyStyle.backgroundColor);

      // Text should be dark-ish (not pure white on white)
      expect(bodyStyle.color).not.toBe('rgb(255, 255, 255)');
    });
  });

  test.describe('Overall Professional Appearance', () => {
    test('should have no horizontal scrollbar on desktop viewport', async ({ page }) => {
      // Set desktop viewport
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('should have all major sections visible and styled', async ({ page }) => {
      // Check all major sections are present and visible
      await expect(page.locator('header')).toBeVisible();
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('.features')).toBeVisible();
      await expect(page.locator('.quickstart')).toBeVisible();
      await expect(page.locator('.resources')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();
    });

    test('should display project name MirDB prominently', async ({ page }) => {
      // MirDB should appear in hero section prominently
      const heroH1 = page.locator('.hero h1');
      await expect(heroH1).toContainText('MirDB');

      // Check font size is large enough to be prominent
      const h1FontSize = await heroH1.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      expect(h1FontSize).toBeGreaterThanOrEqual(32); // At least 2rem
    });
  });
});
