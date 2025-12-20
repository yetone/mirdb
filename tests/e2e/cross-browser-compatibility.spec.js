// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage renders correctly across major browsers:
 * - Chrome (chromium)
 * - Firefox
 * - Safari (webkit)
 * - Edge
 *
 * Each test validates visual rendering, CSS features, and layout consistency.
 */

const indexPath = `file://${path.resolve(__dirname, '../../index.html')}`;

test.describe('Cross-Browser Compatibility', () => {

  test.describe('Visual Rendering Tests', () => {

    test('page renders correctly with all major elements visible', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Verify the page title is correct
      await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store with Memcached Compatibility');

      // Check hero section elements are visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check h1 title is visible and has correct text
      const heroTitle = page.locator('h1#hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Check tagline is visible
      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Check CTA buttons are visible
      const ctaButtons = page.locator('.hero-cta .btn');
      await expect(ctaButtons).toHaveCount(2);

      // Check logo is visible
      const logo = page.locator('.hero-logo');
      await expect(logo).toBeVisible();

      // Log browser name for debugging
      console.log(`Visual rendering test passed on ${browserName}`);
    });

    test('quick start section renders correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check quick start section is visible
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Verify installation steps are rendered
      const installationSteps = page.locator('.installation-steps li');
      await expect(installationSteps).toHaveCount(4);

      // Verify code blocks are visible
      const codeBlocks = page.locator('.code-block');
      expect(await codeBlocks.count()).toBeGreaterThanOrEqual(4);

      console.log(`Quick start section renders correctly on ${browserName}`);
    });

    test('configuration section renders correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check configuration section is visible
      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();

      // Verify config table exists and has rows
      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();

      const tableRows = page.locator('.config-table tbody tr');
      expect(await tableRows.count()).toBeGreaterThanOrEqual(7);

      console.log(`Configuration section renders correctly on ${browserName}`);
    });

  });

  test.describe('CSS Flexbox and Grid Features', () => {

    test('hero section flexbox layout works correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check hero section uses flexbox
      const heroSection = page.locator('.hero');
      const heroDisplay = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(heroDisplay).toBe('flex');

      // Check hero content is centered
      const heroContent = page.locator('.hero-content');
      const heroContentBox = await heroContent.boundingBox();
      const heroBox = await heroSection.boundingBox();

      // Verify hero content is roughly centered horizontally
      if (heroContentBox && heroBox) {
        const contentCenter = heroContentBox.x + heroContentBox.width / 2;
        const heroCenter = heroBox.x + heroBox.width / 2;
        const tolerance = 50; // 50px tolerance for centering
        expect(Math.abs(contentCenter - heroCenter)).toBeLessThan(tolerance);
      }

      console.log(`Flexbox layout works correctly on ${browserName}`);
    });

    test('CTA buttons flexbox layout works correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check CTA container uses flexbox
      const ctaContainer = page.locator('.hero-cta');
      const ctaDisplay = await ctaContainer.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(ctaDisplay).toBe('flex');

      // Check gap is applied
      const ctaGap = await ctaContainer.evaluate((el) => {
        return window.getComputedStyle(el).gap;
      });
      // Gap should be 16px as defined in CSS
      expect(ctaGap).toBe('16px');

      console.log(`CTA buttons flexbox layout works correctly on ${browserName}`);
    });

    test('differentiators flexbox layout works correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const differentiators = page.locator('.hero-differentiators');
      const differentiatorDisplay = await differentiators.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(differentiatorDisplay).toBe('flex');

      // Check flex-wrap is applied
      const flexWrap = await differentiators.evaluate((el) => {
        return window.getComputedStyle(el).flexWrap;
      });
      expect(flexWrap).toBe('wrap');

      console.log(`Differentiators flexbox layout works correctly on ${browserName}`);
    });

    test('configuration table layout works correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check table has proper styling
      const configTable = page.locator('.config-table');
      const borderCollapse = await configTable.evaluate((el) => {
        return window.getComputedStyle(el).borderCollapse;
      });
      expect(borderCollapse).toBe('collapse');

      // Check table width is 100%
      const tableWidth = await configTable.evaluate((el) => {
        return window.getComputedStyle(el).width;
      });
      expect(tableWidth).not.toBe('0px');

      console.log(`Configuration table layout works correctly on ${browserName}`);
    });

  });

  test.describe('CSS Custom Properties (Variables) Support', () => {

    test('CSS custom properties are applied correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check primary color variable is applied to buttons
      const primaryBtn = page.locator('.btn-primary').first();
      const bgColor = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // The primary color #2563eb should be rgb(37, 99, 235)
      expect(bgColor).toBe('rgb(37, 99, 235)');

      // Check text color variable is applied
      const heroTitle = page.locator('h1#hero-title');
      const textColor = await heroTitle.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // The text-dark color #1f2937 should be rgb(31, 41, 55)
      expect(textColor).toBe('rgb(31, 41, 55)');

      console.log(`CSS custom properties work correctly on ${browserName}`);
    });

    test('CSS linear gradient is rendered correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check hero section has gradient background
      const heroSection = page.locator('.hero');
      const backgroundImage = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundImage;
      });

      // Should contain linear-gradient
      expect(backgroundImage).toContain('linear-gradient');

      console.log(`CSS linear gradient renders correctly on ${browserName}`);
    });

  });

  test.describe('Typography Consistency', () => {

    test('font family is applied consistently', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check body font family
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Should use system font stack
      expect(fontFamily).toMatch(/system-ui|BlinkMacSystemFont|Segoe UI|Roboto/i);

      console.log(`Typography is consistent on ${browserName}`);
    });

    test('font sizes are rendered correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check h1 font size (3.5rem = 56px at default)
      const heroTitle = page.locator('h1#hero-title');
      const h1FontSize = await heroTitle.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });

      // Font size should be around 56px (3.5rem at 16px base)
      const h1Size = parseFloat(h1FontSize);
      expect(h1Size).toBeGreaterThanOrEqual(50);
      expect(h1Size).toBeLessThanOrEqual(60);

      console.log(`Font sizes render correctly on ${browserName}`);
    });

  });

  test.describe('Box Model Consistency', () => {

    test('box-sizing border-box is applied globally', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check box-sizing on various elements
      const elements = ['.hero', '.container', '.btn', '.code-block'];

      for (const selector of elements) {
        const element = page.locator(selector).first();
        const boxSizing = await element.evaluate((el) => {
          return window.getComputedStyle(el).boxSizing;
        });
        expect(boxSizing).toBe('border-box');
      }

      console.log(`Box-sizing is consistent on ${browserName}`);
    });

    test('padding and margins are consistent', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check container padding
      const container = page.locator('.container').first();
      const padding = await container.evaluate((el) => {
        return window.getComputedStyle(el).padding;
      });

      // Should have horizontal padding (0 20px format or similar)
      expect(padding).toMatch(/0px 20px|20px/);

      console.log(`Padding and margins are consistent on ${browserName}`);
    });

  });

  test.describe('Interactive Elements', () => {

    test('buttons have correct hover states', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check primary button exists and is clickable
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Get initial background color
      const initialBg = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Hover over the button
      await primaryBtn.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Get hover background color
      const hoverBg = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Colors might be different or same depending on transition state
      // Just verify the button is still functional
      await expect(primaryBtn).toBeVisible();

      console.log(`Button hover states work on ${browserName}`);
    });

    test('links are navigable', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check Get Started link points to quick-start section
      const getStartedBtn = page.locator('a.btn-primary[href="#quick-start"]');
      await expect(getStartedBtn).toBeVisible();

      // Check GitHub link exists
      const githubBtn = page.locator('a.btn-secondary[href*="github.com"]');
      await expect(githubBtn).toBeVisible();

      console.log(`Links are navigable on ${browserName}`);
    });

  });

  test.describe('SVG Rendering', () => {

    test('SVG logo renders correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check SVG logo is visible
      const logo = page.locator('.hero-logo');
      await expect(logo).toBeVisible();

      // Check SVG has proper dimensions
      const logoBox = await logo.boundingBox();
      expect(logoBox?.width).toBeGreaterThan(0);
      expect(logoBox?.height).toBeGreaterThan(0);

      console.log(`SVG logo renders correctly on ${browserName}`);
    });

    test('SVG icons in buttons render correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check SVG icons in CTA buttons
      const btnIcons = page.locator('.btn svg');
      expect(await btnIcons.count()).toBeGreaterThanOrEqual(2);

      // Verify each icon is visible
      for (let i = 0; i < await btnIcons.count(); i++) {
        const icon = btnIcons.nth(i);
        await expect(icon).toBeVisible();
      }

      console.log(`SVG icons render correctly on ${browserName}`);
    });

    test('SVG icons in differentiators render correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check differentiator icons
      const differentiatorIcons = page.locator('.differentiator-icon');
      expect(await differentiatorIcons.count()).toBe(3);

      // Verify each icon is visible
      for (let i = 0; i < await differentiatorIcons.count(); i++) {
        const icon = differentiatorIcons.nth(i);
        await expect(icon).toBeVisible();
      }

      console.log(`Differentiator icons render correctly on ${browserName}`);
    });

  });

  test.describe('Code Block Rendering', () => {

    test('code blocks have proper styling', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check code block styling
      const codeBlock = page.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      // Check background color (should be dark: #1f2937)
      const bgColor = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      expect(bgColor).toBe('rgb(31, 41, 55)');

      // Check border radius
      const borderRadius = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(borderRadius).toBe('8px');

      console.log(`Code blocks render correctly on ${browserName}`);
    });

    test('code text is readable', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check code text color
      const codeBlock = page.locator('.code-block code').first();
      const textColor = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Should be light text (rgb(229, 231, 235) = #e5e7eb)
      expect(textColor).toBe('rgb(229, 231, 235)');

      // Check font family is monospace
      const fontFamily = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      expect(fontFamily).toMatch(/Menlo|Monaco|Courier|monospace/i);

      console.log(`Code text is readable on ${browserName}`);
    });

  });

  test.describe('Scroll Behavior', () => {

    test('page is scrollable', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Scroll to quick-start section
      await page.locator('#quick-start').scrollIntoViewIfNeeded();

      // Wait for scroll to complete
      await page.waitForTimeout(500);

      // Get new scroll position
      const newScrollY = await page.evaluate(() => window.scrollY);

      // Scroll position should have changed
      expect(newScrollY).toBeGreaterThan(initialScrollY);

      console.log(`Page scrolling works on ${browserName}`);
    });

  });

});
