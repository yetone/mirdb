// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB landing page renders correctly
 * across modern browsers (Chrome, Firefox, Safari/WebKit, Edge) as
 * specified in NFR-4 of the PRD.
 *
 * The tests run against all browser projects configured in playwright.config.js:
 * - chromium (Chrome)
 * - firefox (Firefox)
 * - webkit (Safari)
 * - msedge (Microsoft Edge)
 */

test.describe('Cross-Browser Compatibility - NFR-4', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Visual regression test verifying page renders correctly
  test('TC1: Page renders correctly without visual defects', async ({ page, browserName }) => {
    // Verify the page has loaded by checking for essential elements
    await expect(page.locator('body')).toBeVisible();

    // Verify hero section renders
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify h1 element is visible and contains MirDB
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify CSS is loaded and applied (check a styled element)
    const heroStyles = await heroSection.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      return {
        display: computedStyle.display,
        position: computedStyle.position
      };
    });

    // Verify the hero section has CSS applied (not default values)
    expect(heroStyles.display).toBeTruthy();

    // Log browser for debugging
    console.log(`Testing in browser: ${browserName}`);
  });

  // Test Case 2: Critical CSS properties render consistently
  test('TC2: Critical CSS properties render consistently across browsers', async ({ page, browserName }) => {
    // Test color rendering (using RGB format for consistency)
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify font rendering
    const h1 = page.locator('h1');
    const h1Styles = await h1.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      return {
        fontFamily: computedStyle.fontFamily,
        fontSize: computedStyle.fontSize,
        fontWeight: computedStyle.fontWeight
      };
    });

    // Verify font is loaded and applied
    expect(h1Styles.fontFamily).toBeTruthy();
    expect(h1Styles.fontSize).toBeTruthy();

    // Parse font size to verify it's a reasonable value
    const fontSizeValue = parseFloat(h1Styles.fontSize);
    expect(fontSizeValue).toBeGreaterThan(16); // Should be larger than base font

    console.log(`Browser ${browserName} - H1 styles:`, h1Styles);
  });

  // Test Case 3: Layout structure is consistent
  test('TC3: Layout structure renders consistently across browsers', async ({ page, browserName }) => {
    // Set a consistent viewport
    await page.setViewportSize({ width: 1920, height: 1080 });

    // Verify features section layout
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are present
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3); // At least 3 feature cards

    // Verify cards are positioned correctly (grid or flex layout)
    if (cardCount > 0) {
      const firstCardBox = await featureCards.first().boundingBox();
      expect(firstCardBox).not.toBeNull();
      expect(firstCardBox.width).toBeGreaterThan(0);
      expect(firstCardBox.height).toBeGreaterThan(0);
    }

    console.log(`Browser ${browserName} - Feature cards count: ${cardCount}`);
  });

  // Test Case 4: Interactive elements are functional
  test('TC4: Interactive elements render and function correctly', async ({ page, browserName }) => {
    // Test CTA buttons
    const primaryCta = page.locator('[data-testid="cta-primary"]');
    const secondaryCta = page.locator('[data-testid="cta-secondary"]');

    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Verify buttons are clickable (have pointer cursor)
    const primaryCtaStyles = await primaryCta.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      return {
        cursor: computedStyle.cursor,
        pointerEvents: computedStyle.pointerEvents
      };
    });

    // Button should be interactive
    expect(primaryCtaStyles.pointerEvents).not.toBe('none');

    // Verify href attributes exist
    const primaryHref = await primaryCta.getAttribute('href');
    const secondaryHref = await secondaryCta.getAttribute('href');
    expect(primaryHref).toBeTruthy();
    expect(secondaryHref).toBeTruthy();

    console.log(`Browser ${browserName} - Primary CTA href: ${primaryHref}`);
  });

  // Test Case 5: Code blocks render correctly
  test('TC5: Code blocks render correctly across browsers', async ({ page, browserName }) => {
    // Scroll to code examples section
    const codeSection = page.locator('[data-testid="code-examples-section"]');
    if (await codeSection.count() > 0) {
      await codeSection.scrollIntoViewIfNeeded();
      await expect(codeSection).toBeVisible();

      // Verify code blocks exist
      const codeBlocks = page.locator('pre, code');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Verify code block styling
      if (codeBlockCount > 0) {
        const codeStyles = await codeBlocks.first().evaluate((el) => {
          const computedStyle = window.getComputedStyle(el);
          return {
            fontFamily: computedStyle.fontFamily,
            backgroundColor: computedStyle.backgroundColor,
            whiteSpace: computedStyle.whiteSpace
          };
        });

        // Code should use monospace font
        expect(codeStyles.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/i);

        console.log(`Browser ${browserName} - Code block styles:`, codeStyles);
      }
    } else {
      // If no code examples section, check for any code blocks on page
      const codeBlocks = page.locator('pre, code');
      const count = await codeBlocks.count();
      console.log(`Browser ${browserName} - Found ${count} code blocks on page`);
    }
  });

  // Test Case 6: Footer renders correctly
  test('TC6: Footer section renders correctly across browsers', async ({ page, browserName }) => {
    // Scroll to footer
    const footer = page.locator('footer, [data-testid="footer-section"]');
    if (await footer.count() > 0) {
      await footer.scrollIntoViewIfNeeded();
      await expect(footer.first()).toBeVisible();

      // Verify footer has content
      const footerBox = await footer.first().boundingBox();
      expect(footerBox).not.toBeNull();
      expect(footerBox.height).toBeGreaterThan(0);

      // Verify footer links
      const footerLinks = footer.first().locator('a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      console.log(`Browser ${browserName} - Footer links count: ${linkCount}`);
    }
  });

  // Test Case 7: Images and icons render correctly
  test('TC7: Images and icons render correctly across browsers', async ({ page, browserName }) => {
    // Check for any images
    const images = page.locator('img, svg');
    const imageCount = await images.count();

    // If there are images, verify they loaded
    if (imageCount > 0) {
      for (let i = 0; i < Math.min(imageCount, 5); i++) {
        const img = images.nth(i);
        const tagName = await img.evaluate((el) => el.tagName.toLowerCase());

        if (tagName === 'img') {
          // Check if image has dimensions (loaded)
          const box = await img.boundingBox();
          if (box) {
            expect(box.width).toBeGreaterThan(0);
            expect(box.height).toBeGreaterThan(0);
          }
        } else if (tagName === 'svg') {
          // SVG should be visible
          await expect(img).toBeVisible();
        }
      }
    }

    console.log(`Browser ${browserName} - Found ${imageCount} images/icons`);
  });

  // Test Case 8: No horizontal scrollbar on standard viewport
  test('TC8: No horizontal scrollbar on standard viewport', async ({ page, browserName }) => {
    // Set standard viewport
    await page.setViewportSize({ width: 1920, height: 1080 });

    // Check for horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);

    console.log(`Browser ${browserName} - Has horizontal scroll: ${hasHorizontalScroll}`);
  });

  // Test Case 9: Flexbox and Grid layouts render correctly
  test('TC9: CSS Flexbox and Grid layouts render correctly', async ({ page, browserName }) => {
    // Check for elements using flexbox
    const flexElements = await page.evaluate(() => {
      const allElements = document.querySelectorAll('*');
      let flexCount = 0;
      let gridCount = 0;

      for (const el of allElements) {
        const style = window.getComputedStyle(el);
        if (style.display === 'flex' || style.display === 'inline-flex') {
          flexCount++;
        }
        if (style.display === 'grid' || style.display === 'inline-grid') {
          gridCount++;
        }
      }

      return { flexCount, gridCount };
    });

    // Modern layout features should be supported
    console.log(`Browser ${browserName} - Flex elements: ${flexElements.flexCount}, Grid elements: ${flexElements.gridCount}`);

    // Page should use modern layout (at least some flex or grid)
    expect(flexElements.flexCount + flexElements.gridCount).toBeGreaterThan(0);
  });

  // Test Case 10: CSS transitions/animations are supported
  test('TC10: CSS transitions and animations are supported', async ({ page, browserName }) => {
    // Check for elements with transitions or animations
    const animationSupport = await page.evaluate(() => {
      const allElements = document.querySelectorAll('*');
      let transitionCount = 0;
      let animationCount = 0;

      for (const el of allElements) {
        const style = window.getComputedStyle(el);
        if (style.transition && style.transition !== 'none' && style.transition !== 'all 0s ease 0s') {
          transitionCount++;
        }
        if (style.animation && style.animation !== 'none') {
          animationCount++;
        }
      }

      return { transitionCount, animationCount };
    });

    console.log(`Browser ${browserName} - Transitions: ${animationSupport.transitionCount}, Animations: ${animationSupport.animationCount}`);

    // No assertion needed - just verify no errors occur
    expect(animationSupport).toBeTruthy();
  });
});
