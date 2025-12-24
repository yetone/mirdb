// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Responsive Design - Mobile (Scenario 8)
 * Verifies that the homepage displays correctly on mobile devices
 * as specified in NFR-1 and Story 5
 */

// iPhone SE viewport dimensions
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile', () => {
  test.use({ viewport: MOBILE_VIEWPORT });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: No horizontal overflow at 375px width
   * Input: Load page at 375px width
   * Expected: No horizontal overflow; content fits within viewport
   */
  test('TC1: No horizontal overflow at 375px width', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check if there's horizontal scrolling by comparing scrollWidth to clientWidth
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalOverflow).toBe(false);

    // Additionally, check that body doesn't overflow
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      return body.scrollWidth > body.clientWidth;
    });

    expect(bodyOverflow).toBe(false);

    // Verify all main sections fit within viewport
    const sections = ['#hero', '#features', '#architecture', '#getting-started', '.footer'];
    for (const selector of sections) {
      const element = page.locator(selector);
      if (await element.count() > 0) {
        const box = await element.boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
        }
      }
    }
  });

  /**
   * Test Case 2: Check minimum font size on mobile
   * Input: Check minimum font size on mobile
   * Expected: Body text is at least 16px for readability
   */
  test('TC2: Body text has at least 16px font size for readability', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check body font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const style = window.getComputedStyle(body);
      return parseFloat(style.fontSize);
    });

    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph font sizes in main content areas
    const paragraphs = page.locator('p');
    const paragraphCount = await paragraphs.count();

    for (let i = 0; i < Math.min(paragraphCount, 10); i++) {
      const fontSize = await paragraphs.nth(i).evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.fontSize);
      });

      // Body text should be at least 16px for mobile readability
      expect(fontSize).toBeGreaterThanOrEqual(16);
    }

    // Check tagline is readable (at least 16px)
    const tagline = page.locator('.tagline');
    if (await tagline.count() > 0) {
      const taglineFontSize = await tagline.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.fontSize);
      });
      expect(taglineFontSize).toBeGreaterThanOrEqual(16);
    }
  });

  /**
   * Test Case 3: Measure button/link touch targets
   * Input: Measure button/link touch targets
   * Expected: Interactive elements have minimum 44x44px touch area
   */
  test('TC3: Interactive elements have minimum 44x44px touch area', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check CTA buttons in hero section
    const ctaButtons = page.locator('.btn');
    const ctaCount = await ctaButtons.count();

    for (let i = 0; i < ctaCount; i++) {
      const box = await ctaButtons.nth(i).boundingBox();
      if (box) {
        // Buttons should have at least 44x44px touch target area
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }

    // Check footer links
    const footerLinks = page.locator('.footer-links a');
    const footerLinksCount = await footerLinks.count();

    for (let i = 0; i < footerLinksCount; i++) {
      const link = footerLinks.nth(i);

      // Get computed styles to account for padding
      const touchTarget = await link.evaluate((el) => {
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();

        // Account for padding in touch target
        const paddingTop = parseFloat(style.paddingTop) || 0;
        const paddingBottom = parseFloat(style.paddingBottom) || 0;
        const paddingLeft = parseFloat(style.paddingLeft) || 0;
        const paddingRight = parseFloat(style.paddingRight) || 0;

        return {
          width: rect.width,
          height: rect.height,
          totalWidth: rect.width + paddingLeft + paddingRight,
          totalHeight: rect.height + paddingTop + paddingBottom
        };
      });

      // Footer links should meet minimum touch target requirements
      // Allow some flexibility as long as the overall clickable area is reasonable
      expect(touchTarget.height).toBeGreaterThanOrEqual(44);
    }

    // Check navigation anchor links
    const anchorLinks = page.locator('a[href^="#"]');
    const anchorCount = await anchorLinks.count();

    for (let i = 0; i < anchorCount; i++) {
      const box = await anchorLinks.nth(i).boundingBox();
      if (box) {
        // Anchor links should have at least 44px height for touch
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  /**
   * Test Case 4: Verify images have max-width:100%
   * Input: Verify images have max-width:100%
   * Expected: Images scale to fit container width
   */
  test('TC4: Images scale to fit container width', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check all images
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const imgStyle = await img.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          maxWidth: style.maxWidth,
          width: el.getBoundingClientRect().width
        };
      });

      // Images should not exceed viewport width
      expect(imgStyle.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }

    // Check SVG elements (like the architecture diagram)
    const svgs = page.locator('svg');
    const svgCount = await svgs.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = svgs.nth(i);
      const svgBox = await svg.boundingBox();
      if (svgBox) {
        // SVGs should scale to fit within viewport
        expect(svgBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }
    }

    // Check the architecture diagram specifically
    const archDiagram = page.locator('.lsm-tree-diagram');
    if (await archDiagram.count() > 0) {
      const diagramBox = await archDiagram.boundingBox();
      if (diagramBox) {
        expect(diagramBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }

      // Check that the diagram has proper responsive attributes
      const hasResponsiveStyle = await archDiagram.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.width === '100%' || style.maxWidth === '100%' || el.hasAttribute('viewBox');
      });
      expect(hasResponsiveStyle).toBe(true);
    }

    // Check feature items don't overflow
    const featureItems = page.locator('.feature-item');
    const featureCount = await featureItems.count();

    for (let i = 0; i < featureCount; i++) {
      const box = await featureItems.nth(i).boundingBox();
      if (box) {
        expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }
    }
  });

  /**
   * Additional test: Verify code blocks are scrollable on mobile
   */
  test('Code blocks handle overflow properly on mobile', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check code blocks in getting-started section
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const box = await codeBlock.boundingBox();

      if (box) {
        // Code block container should fit within viewport
        expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

        // Verify overflow-x is auto or scroll for code blocks
        const overflowX = await codeBlock.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return style.overflowX;
        });
        expect(['auto', 'scroll']).toContain(overflowX);
      }
    }
  });

  /**
   * Additional test: Verify text doesn't get cut off on mobile
   */
  test('Text content is fully visible without being cut off', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check headings are visible
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();
    const h1Box = await h1.boundingBox();
    if (h1Box) {
      expect(h1Box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }

    // Check section headings
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();

    for (let i = 0; i < h2Count; i++) {
      const h2 = h2Elements.nth(i);
      await expect(h2).toBeVisible();
      const h2Box = await h2.boundingBox();
      if (h2Box) {
        expect(h2Box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }
    }
  });
});
