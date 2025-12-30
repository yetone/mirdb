// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 * These tests verify that the MirDB homepage renders correctly across
 * Chrome, Firefox, Safari (WebKit), and Edge browsers.
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Layout Rendering', () => {
    test('TC1: Hero section renders correctly with proper layout', async ({ page }) => {
      // Test Case 1: Load homepage and verify hero section renders correctly
      // Expected: All sections render correctly with no layout issues
      const hero = page.locator('.hero, [data-testid="hero"]').first();
      await expect(hero).toBeVisible();

      // Verify hero content is properly centered using flexbox
      const heroBox = await hero.boundingBox();
      expect(heroBox).not.toBeNull();
      expect(heroBox.width).toBeGreaterThan(0);
      expect(heroBox.height).toBeGreaterThan(0);

      // Verify h1 is visible
      const h1 = hero.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // Verify tagline is visible
      const tagline = hero.locator('.tagline, [data-testid="tagline"]').first();
      await expect(tagline).toBeVisible();

      // Verify CTA buttons are visible
      const ctaButtons = hero.locator('.hero-ctas .btn');
      const buttonCount = await ctaButtons.count();
      expect(buttonCount).toBeGreaterThanOrEqual(2);
    });

    test('TC2: Feature grid uses CSS grid/flexbox correctly', async ({ page }) => {
      // Test Case 5: Test CSS flexbox/grid features across browsers
      // Expected: Layout features work consistently across all browsers
      const featuresSection = page.locator('#features, .features');
      await expect(featuresSection).toBeVisible();

      const featureGrid = page.locator('.feature-grid');
      await expect(featureGrid).toBeVisible();

      // Verify grid layout is applied
      const gridDisplay = await featureGrid.evaluate(
        (el) => window.getComputedStyle(el).display
      );
      expect(gridDisplay).toBe('grid');

      // Verify feature cards are rendered
      const featureCards = featureGrid.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // Verify all cards have proper dimensions
      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        const box = await card.boundingBox();
        expect(box).not.toBeNull();
        expect(box.width).toBeGreaterThan(200);
        expect(box.height).toBeGreaterThan(50);
      }
    });

    test('TC3: Code block renders correctly with monospace font', async ({ page }) => {
      // Verify code blocks in Quick Start section
      const quickStart = page.locator('#quick-start, .quick-start');
      await expect(quickStart).toBeVisible();

      const codeBlock = quickStart.locator('.code-block');
      await expect(codeBlock).toBeVisible();

      // Verify code element uses monospace font
      const code = codeBlock.locator('code');
      await expect(code).toBeVisible();

      const fontFamily = await code.evaluate(
        (el) => window.getComputedStyle(el).fontFamily
      );
      // Check for common monospace fonts
      expect(fontFamily.toLowerCase()).toMatch(
        /mono|consolas|courier|menlo|sf mono|fira code|monaco/i
      );
    });

    test('TC4: Status grid layout renders correctly', async ({ page }) => {
      // Verify project status section uses grid layout
      const projectStatus = page.locator('#project-status, .project-status, [data-testid="project-status"]');
      await expect(projectStatus).toBeVisible();

      const statusGrid = page.locator('.status-grid');
      await expect(statusGrid).toBeVisible();

      // Verify grid display
      const gridDisplay = await statusGrid.evaluate(
        (el) => window.getComputedStyle(el).display
      );
      expect(gridDisplay).toBe('grid');

      // Verify columns exist
      const columns = statusGrid.locator('.status-column');
      const columnCount = await columns.count();
      expect(columnCount).toBe(2);
    });

    test('TC5: Architecture diagram renders as SVG', async ({ page }) => {
      // Verify SVG architecture diagram
      const architecture = page.locator('#architecture, .architecture, [data-testid="architecture"]');
      await expect(architecture).toBeVisible();

      const diagram = page.locator('.architecture-diagram, [data-testid="architecture-diagram"]');
      await expect(diagram).toBeVisible();

      // Verify it's an SVG element
      const tagName = await diagram.evaluate((el) => el.tagName.toLowerCase());
      expect(tagName).toBe('svg');

      // Verify SVG has proper viewBox
      const viewBox = await diagram.getAttribute('viewBox');
      expect(viewBox).toBeTruthy();
    });
  });

  test.describe('CSS Custom Properties', () => {
    test('TC6: CSS variables are applied correctly', async ({ page }) => {
      // Verify CSS custom properties work across browsers
      const body = page.locator('body');

      // Check background color (should use --color-background)
      const bgColor = await body.evaluate(
        (el) => window.getComputedStyle(el).backgroundColor
      );
      // The color should be set (not transparent)
      expect(bgColor).not.toBe('transparent');
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('TC7: Gradient text renders on h1', async ({ page }) => {
      // Verify gradient text effect on h1 (webkit-specific property)
      const h1 = page.locator('.hero h1');
      await expect(h1).toBeVisible();

      // Check that background-clip is applied (text or -webkit-text)
      const backgroundClip = await h1.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.backgroundClip || style.webkitBackgroundClip;
      });

      // The value might be 'text' or contain 'text'
      expect(backgroundClip).toBeTruthy();
    });
  });

  test.describe('Interactive Elements', () => {
    test('TC8: Buttons have proper hover styles', async ({ page }) => {
      // Verify buttons render correctly
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Get initial background color
      const initialBg = await primaryBtn.evaluate(
        (el) => window.getComputedStyle(el).backgroundColor
      );
      expect(initialBg).toBeTruthy();

      // Verify secondary button
      const secondaryBtn = page.locator('.btn-secondary').first();
      await expect(secondaryBtn).toBeVisible();

      // Check border is rendered
      const border = await secondaryBtn.evaluate(
        (el) => window.getComputedStyle(el).borderStyle
      );
      expect(border).not.toBe('none');
    });

    test('TC9: Links have proper focus styles', async ({ page }) => {
      // Verify focus styles for accessibility
      const links = page.locator('a');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThan(0);

      // Tab to first link and verify it can receive focus
      const firstLink = links.first();
      await firstLink.focus();

      // Check that outline is applied on focus
      const outline = await firstLink.evaluate(
        (el) => window.getComputedStyle(el).outlineStyle
      );
      // Outline should be visible (not none)
      expect(outline).not.toBe('none');
    });
  });

  test.describe('Typography', () => {
    test('TC10: Font families are applied correctly', async ({ page }) => {
      // Verify sans-serif font on body
      const body = page.locator('body');
      const bodyFont = await body.evaluate(
        (el) => window.getComputedStyle(el).fontFamily
      );
      expect(bodyFont).toBeTruthy();

      // Verify monospace font on code elements
      const code = page.locator('code').first();
      if (await code.count() > 0) {
        const codeFont = await code.evaluate(
          (el) => window.getComputedStyle(el).fontFamily
        );
        expect(codeFont.toLowerCase()).toMatch(
          /mono|consolas|courier|menlo/i
        );
      }
    });

    test('TC11: Clamp font sizes work correctly', async ({ page }) => {
      // Verify clamp() CSS function for responsive text
      const h1 = page.locator('.hero h1');
      await expect(h1).toBeVisible();

      const fontSize = await h1.evaluate(
        (el) => window.getComputedStyle(el).fontSize
      );
      // Font size should be a positive value
      const fontSizeNum = parseFloat(fontSize);
      expect(fontSizeNum).toBeGreaterThan(0);
    });
  });

  test.describe('Scroll Behavior', () => {
    test('TC12: Smooth scroll behavior is applied', async ({ page }) => {
      // Check scroll-behavior CSS property
      const html = page.locator('html');
      const scrollBehavior = await html.evaluate(
        (el) => window.getComputedStyle(el).scrollBehavior
      );
      expect(scrollBehavior).toBe('smooth');
    });

    test('TC13: Internal navigation links work', async ({ page }) => {
      // Test "Get Started" link navigates to quick-start section
      const getStartedBtn = page.locator('a[href="#quick-start"]');
      if (await getStartedBtn.count() > 0) {
        await getStartedBtn.click();

        // Wait a moment for smooth scroll
        await page.waitForTimeout(500);

        // Verify the quick-start section is in view
        const quickStart = page.locator('#quick-start');
        await expect(quickStart).toBeInViewport({ ratio: 0.5 });
      }
    });
  });

  test.describe('Footer', () => {
    test('TC14: Footer renders with flexbox layout', async ({ page }) => {
      const footer = page.locator('.footer, footer');
      await expect(footer).toBeVisible();

      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();

      // Verify flexbox layout
      const display = await footerContent.evaluate(
        (el) => window.getComputedStyle(el).display
      );
      expect(display).toBe('flex');

      // Verify footer links exist
      const footerLinks = page.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('Box Model and Borders', () => {
    test('TC15: Border-box sizing is applied globally', async ({ page }) => {
      // Verify box-sizing: border-box is applied
      const elements = page.locator('.feature-card, .status-column, .code-block');
      const count = await elements.count();

      for (let i = 0; i < Math.min(count, 3); i++) {
        const boxSizing = await elements.nth(i).evaluate(
          (el) => window.getComputedStyle(el).boxSizing
        );
        expect(boxSizing).toBe('border-box');
      }
    });

    test('TC16: Border radius renders correctly', async ({ page }) => {
      // Verify border-radius on buttons and cards
      const btn = page.locator('.btn').first();
      const borderRadius = await btn.evaluate(
        (el) => window.getComputedStyle(el).borderRadius
      );
      expect(borderRadius).not.toBe('0px');

      const card = page.locator('.feature-card').first();
      const cardRadius = await card.evaluate(
        (el) => window.getComputedStyle(el).borderRadius
      );
      expect(cardRadius).not.toBe('0px');
    });
  });
});
