// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Responsive Design - Desktop (Scenario 10)
 * Verifies that the homepage displays correctly on desktop screens
 * as specified in NFR-1
 */

// Standard desktop viewport dimensions
const DESKTOP_VIEWPORT_1280 = { width: 1280, height: 800 };
const DESKTOP_VIEWPORT_1920 = { width: 1920, height: 1080 };

test.describe('Responsive Design - Desktop', () => {

  /**
   * Test Case 1: Load page at 1280px width
   * Input: Load page at 1280px width
   * Expected: Desktop layout with appropriate use of space
   */
  test('TC1: Desktop layout at 1280px width with appropriate use of space', async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT_1280);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify no horizontal overflow at desktop width
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Verify hero section displays properly at desktop width
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();
    const heroBox = await hero.boundingBox();
    expect(heroBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT_1280.width * 0.9);

    // Verify features grid uses multi-column layout at desktop width
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check that features are in a multi-column layout (not stacked)
    const featureItems = page.locator('.feature-item');
    const featureCount = await featureItems.count();
    expect(featureCount).toBeGreaterThanOrEqual(2);

    // Get positions of first two feature items to verify they're side by side
    const firstFeatureBox = await featureItems.nth(0).boundingBox();
    const secondFeatureBox = await featureItems.nth(1).boundingBox();

    // On desktop, features should be on the same row (similar Y position)
    // or the second item should have a different X position indicating columns
    const sameRow = Math.abs(firstFeatureBox.y - secondFeatureBox.y) < 50;
    const differentColumn = firstFeatureBox.x !== secondFeatureBox.x;

    // Either same row (grid layout) or different columns
    expect(sameRow || differentColumn).toBe(true);

    // Verify hero heading is large enough for desktop
    const heroH1 = page.locator('.hero h1');
    const h1FontSize = await heroH1.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // At desktop, h1 should be 4rem (64px at default font size)
    expect(h1FontSize).toBeGreaterThanOrEqual(48);

    // Verify architecture section is visible and properly laid out
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    // Verify architecture description uses multi-column layout
    const archDescription = page.locator('.architecture-description');
    if (await archDescription.count() > 0) {
      const archDetails = page.locator('.arch-detail');
      const archDetailCount = await archDetails.count();

      if (archDetailCount >= 2) {
        const firstArchBox = await archDetails.nth(0).boundingBox();
        const secondArchBox = await archDetails.nth(1).boundingBox();

        // On desktop, architecture details should be side by side
        const archSameRow = Math.abs(firstArchBox.y - secondArchBox.y) < 50;
        expect(archSameRow).toBe(true);
      }
    }
  });

  /**
   * Test Case 2: Verify content max-width
   * Input: Verify content max-width
   * Expected: Content has reasonable max-width for readability (typically 1200-1400px)
   */
  test('TC2: Content has reasonable max-width for readability', async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT_1920);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check main container max-width
    const containers = page.locator('.container');
    const containerCount = await containers.count();

    for (let i = 0; i < containerCount; i++) {
      const container = containers.nth(i);
      const containerStyle = await container.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          maxWidth: style.maxWidth,
          width: el.getBoundingClientRect().width
        };
      });

      // Container should have a max-width set (not 'none')
      expect(containerStyle.maxWidth).not.toBe('none');

      // Actual width should not exceed reasonable readability limit (1400px)
      expect(containerStyle.width).toBeLessThanOrEqual(1400);
    }

    // Check hero content max-width
    const heroContent = page.locator('.hero-content');
    if (await heroContent.count() > 0) {
      const heroContentStyle = await heroContent.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          maxWidth: style.maxWidth,
          width: el.getBoundingClientRect().width
        };
      });

      // Hero content should be constrained for readability
      expect(heroContentStyle.width).toBeLessThanOrEqual(1000);
    }

    // Check getting-started content max-width
    const gettingStartedContent = page.locator('.getting-started-content');
    if (await gettingStartedContent.count() > 0) {
      const gsContentStyle = await gettingStartedContent.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          maxWidth: style.maxWidth,
          width: el.getBoundingClientRect().width
        };
      });

      // Getting started content should be constrained for readability
      expect(gsContentStyle.width).toBeLessThanOrEqual(1000);
    }

    // Verify text line lengths are reasonable for readability
    // Long lines of text are hard to read; optimal is 50-75 characters
    const paragraphs = page.locator('p');
    const paragraphCount = await paragraphs.count();

    // Check at least one paragraph
    if (paragraphCount > 0) {
      const firstParagraph = paragraphs.first();
      const paragraphWidth = await firstParagraph.evaluate((el) => {
        return el.getBoundingClientRect().width;
      });

      // Paragraph width should be reasonable (not stretching across entire 1920px screen)
      expect(paragraphWidth).toBeLessThanOrEqual(900);
    }
  });

  /**
   * Test Case 3: Test at 1920px width
   * Input: Test at 1920px width
   * Expected: Layout remains centered and readable at large sizes
   */
  test('TC3: Layout remains centered and readable at 1920px width', async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT_1920);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify no horizontal overflow at large width
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Verify content is centered
    const containers = page.locator('.container');
    const containerCount = await containers.count();

    for (let i = 0; i < containerCount; i++) {
      const container = containers.nth(i);
      const containerBox = await container.boundingBox();

      if (containerBox) {
        // Calculate expected center position
        const containerCenter = containerBox.x + (containerBox.width / 2);
        const viewportCenter = DESKTOP_VIEWPORT_1920.width / 2;

        // Container should be roughly centered (within 50px tolerance)
        expect(Math.abs(containerCenter - viewportCenter)).toBeLessThan(50);
      }
    }

    // Verify hero is centered
    const heroContent = page.locator('.hero-content');
    if (await heroContent.count() > 0) {
      const heroBox = await heroContent.boundingBox();
      if (heroBox) {
        const heroCenter = heroBox.x + (heroBox.width / 2);
        const viewportCenter = DESKTOP_VIEWPORT_1920.width / 2;

        // Hero should be centered
        expect(Math.abs(heroCenter - viewportCenter)).toBeLessThan(50);
      }
    }

    // Verify features grid doesn't stretch too wide
    const featuresGrid = page.locator('.features-grid');
    if (await featuresGrid.count() > 0) {
      const gridBox = await featuresGrid.boundingBox();

      // Grid should have a reasonable maximum width
      expect(gridBox.width).toBeLessThanOrEqual(1400);
    }

    // Verify architecture diagram is centered and reasonably sized
    const archDiagram = page.locator('.architecture-diagram');
    if (await archDiagram.count() > 0) {
      const diagramBox = await archDiagram.boundingBox();

      if (diagramBox) {
        // Diagram should be centered
        const diagramCenter = diagramBox.x + (diagramBox.width / 2);
        const viewportCenter = DESKTOP_VIEWPORT_1920.width / 2;
        expect(Math.abs(diagramCenter - viewportCenter)).toBeLessThan(100);

        // Diagram shouldn't stretch to full viewport width
        expect(diagramBox.width).toBeLessThanOrEqual(800);
      }
    }

    // Verify config table is centered and readable
    const configTable = page.locator('.config-table');
    if (await configTable.count() > 0) {
      const tableBox = await configTable.boundingBox();

      if (tableBox) {
        // Table should be within container bounds
        expect(tableBox.width).toBeLessThanOrEqual(1400);
      }
    }

    // Verify footer links are centered
    const footerLinks = page.locator('.footer-links');
    if (await footerLinks.count() > 0) {
      const footerStyle = await footerLinks.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          justifyContent: style.justifyContent,
          display: style.display
        };
      });

      // Footer links should be flex with center justification
      expect(footerStyle.display).toBe('flex');
      expect(footerStyle.justifyContent).toBe('center');
    }

    // Verify all main sections are visible and properly laid out
    const sections = ['#hero', '#features', '#architecture', '#getting-started', '#configuration', '.footer'];
    for (const selector of sections) {
      const section = page.locator(selector);
      if (await section.count() > 0) {
        await expect(section).toBeVisible();
        const sectionBox = await section.boundingBox();

        // Each section should span reasonable width
        expect(sectionBox.width).toBeGreaterThanOrEqual(DESKTOP_VIEWPORT_1920.width * 0.95);
      }
    }
  });

  /**
   * Additional test: Verify typography is optimized for desktop
   */
  test('Typography is appropriately sized for desktop viewing', async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT_1280);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check hero h1 font size (should be larger on desktop)
    const heroH1 = page.locator('.hero h1');
    const h1FontSize = await heroH1.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Desktop h1 should be at least 48px (3rem at 16px base)
    expect(h1FontSize).toBeGreaterThanOrEqual(48);

    // Check tagline font size
    const tagline = page.locator('.tagline');
    if (await tagline.count() > 0) {
      const taglineFontSize = await tagline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Tagline should be prominent on desktop
      expect(taglineFontSize).toBeGreaterThanOrEqual(20);
    }

    // Check section headings (h2)
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();

    for (let i = 0; i < h2Count; i++) {
      const h2FontSize = await h2Elements.nth(i).evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Section headings should be substantial on desktop
      expect(h2FontSize).toBeGreaterThanOrEqual(32);
    }

    // Verify body text is readable
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);
  });

  /**
   * Additional test: Verify whitespace and spacing at desktop size
   */
  test('Proper whitespace and spacing at desktop size', async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT_1280);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check hero section padding
    const hero = page.locator('.hero');
    const heroPadding = await hero.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(style.paddingTop),
        paddingBottom: parseFloat(style.paddingBottom)
      };
    });
    // Hero should have substantial padding on desktop
    expect(heroPadding.paddingTop).toBeGreaterThanOrEqual(60);
    expect(heroPadding.paddingBottom).toBeGreaterThanOrEqual(60);

    // Check features section padding
    const features = page.locator('.features');
    const featuresPadding = await features.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        paddingTop: parseFloat(style.paddingTop),
        paddingBottom: parseFloat(style.paddingBottom)
      };
    });
    // Features should have comfortable vertical spacing
    expect(featuresPadding.paddingTop).toBeGreaterThanOrEqual(60);
    expect(featuresPadding.paddingBottom).toBeGreaterThanOrEqual(60);

    // Check feature items have proper spacing
    const featureItems = page.locator('.feature-item');
    const featureItemPadding = await featureItems.first().evaluate((el) => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.padding);
    });
    // Feature cards should have comfortable internal padding
    expect(featureItemPadding).toBeGreaterThanOrEqual(20);

    // Check features grid has proper gap between items
    const featuresGrid = page.locator('.features-grid');
    const gridGap = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.gap) || parseFloat(style.gridGap) || 0;
    });
    // Grid should have comfortable spacing between cards
    expect(gridGap).toBeGreaterThanOrEqual(20);
  });
});
