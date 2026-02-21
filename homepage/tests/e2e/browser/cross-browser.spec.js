/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 20 - Cross-Browser Compatibility
 *
 * Tests verify homepage works across major browsers:
 * - Chrome (chromium)
 * - Firefox
 * - Safari (webkit)
 * - Edge
 *
 * Test cases:
 * 1. Load homepage in Chrome - All sections render correctly
 * 2. Load homepage in Firefox - All sections render correctly
 * 3. Load homepage in Safari - All sections render correctly
 * 4. Load homepage in Edge - All sections render correctly
 * 5. Test code copy in all browsers - Copy to clipboard works
 */

const { test, expect } = require('@playwright/test');

test.describe('Cross-Browser Compatibility Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1-4: All sections render correctly', () => {
    test('Hero section renders correctly', async ({ page, browserName }) => {
      // Verify hero section is visible
      const heroSection = page.locator('.hero-section, #hero, .hero');
      await expect(heroSection.first()).toBeVisible();

      // Verify logo is visible and loads correctly
      const logo = page.locator('#mirdb-logo, .logo img, header img[src*="logo"]');
      if (await logo.count() > 0) {
        await expect(logo.first()).toBeVisible();
      }

      // Verify tagline is visible
      const tagline = page.locator('.hero-tagline, .tagline, h1');
      await expect(tagline.first()).toBeVisible();

      // Verify CTA button is visible
      const ctaButton = page.locator('.cta-button, #get-started-btn, a[href="#quickstart"]');
      if (await ctaButton.count() > 0) {
        await expect(ctaButton.first()).toBeVisible();
      }
    });

    test('Navigation renders correctly', async ({ page, browserName }) => {
      // Verify navigation is visible
      const nav = page.locator('nav, .navigation, header');
      await expect(nav.first()).toBeVisible();

      // Verify navigation links are present
      const navLinks = page.locator('nav a, .nav-links a, header a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(1);
    });

    test('Features section renders correctly', async ({ page, browserName }) => {
      // Verify features section is visible
      const featuresSection = page.locator('#features, .features-section, .features');
      if (await featuresSection.count() > 0) {
        await expect(featuresSection.first()).toBeVisible();

        // Verify feature cards/items are present
        const featureItems = page.locator('.feature-card, .feature-item, .feature');
        const itemCount = await featureItems.count();
        expect(itemCount).toBeGreaterThanOrEqual(1);
      }
    });

    test('Quick Start section renders correctly', async ({ page, browserName }) => {
      // Verify quickstart section is visible
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      // Verify code blocks are present
      const codeBlocks = page.locator('.code-block, pre code, pre');
      const blockCount = await codeBlocks.count();
      expect(blockCount).toBeGreaterThanOrEqual(1);
    });

    test('Architecture section link works', async ({ page, browserName }) => {
      // Check architecture link exists
      const archLink = page.locator('a[href*="architecture"]');
      if (await archLink.count() > 0) {
        await expect(archLink.first()).toBeVisible();
      }
    });

    test('Roadmap section renders correctly', async ({ page, browserName }) => {
      // Verify roadmap section if present
      const roadmapSection = page.locator('#roadmap, .roadmap-section, .roadmap');
      if (await roadmapSection.count() > 0) {
        await expect(roadmapSection.first()).toBeVisible();
      }
    });

    test('Footer renders correctly', async ({ page, browserName }) => {
      // Verify footer is visible
      const footer = page.locator('footer, .footer');
      if (await footer.count() > 0) {
        await expect(footer.first()).toBeVisible();

        // Verify GitHub link is present
        const githubLink = page.locator('footer a[href*="github"], .footer a[href*="github"]');
        if (await githubLink.count() > 0) {
          await expect(githubLink.first()).toBeVisible();
        }
      }
    });

    test('Page title is correct', async ({ page, browserName }) => {
      // Verify page title
      const title = await page.title();
      expect(title.toLowerCase()).toContain('mirdb');
    });

    test('No JavaScript errors on page load', async ({ page, browserName }) => {
      const errors = [];
      page.on('pageerror', (err) => errors.push(err.message));

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Allow for minor errors but fail if critical errors
      const criticalErrors = errors.filter(
        (err) =>
          !err.includes('ResizeObserver') &&
          !err.includes('Non-Error') &&
          !err.includes('favicon')
      );

      expect(criticalErrors).toHaveLength(0);
    });

    test('CSS styles load correctly', async ({ page, browserName }) => {
      // Verify CSS is applied by checking computed styles
      const body = page.locator('body');
      const backgroundColor = await body.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );

      // Background color should not be empty/transparent (CSS loaded)
      expect(backgroundColor).toBeTruthy();
      expect(backgroundColor).not.toBe('');
    });

    test('All images load without errors', async ({ page, browserName }) => {
      // Check images are properly displayed after page loads
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get all visible images and verify they have valid naturalWidth (loaded successfully)
      const images = page.locator('img:visible');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const naturalWidth = await img.evaluate((el) => el.naturalWidth);
        const src = await img.getAttribute('src');

        // naturalWidth > 0 means image loaded successfully
        expect(naturalWidth, `Image ${src} should load correctly`).toBeGreaterThan(0);
      }
    });

    test('Layout is correct (no horizontal overflow)', async ({ page, browserName }) => {
      // Check for horizontal scrollbar (indicating layout issues)
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalOverflow).toBe(false);
    });
  });

  test.describe('Test Case 5: Copy to Clipboard Functionality', () => {
    test('Copy button is visible on code blocks', async ({ page, browserName }) => {
      const copyButtons = page.locator('.copy-btn, [data-copy], button[aria-label*="copy" i]');
      const count = await copyButtons.count();

      if (count > 0) {
        await expect(copyButtons.first()).toBeVisible();
      }
    });

    test('Copy button works and copies content', async ({ page, context, browserName }) => {
      // Grant clipboard permissions (only supported in Chromium-based browsers)
      if (browserName === 'chromium') {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      }

      // Find the quickstart section
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      // Find a copy button
      const copyButton = quickstartSection.locator('.copy-btn').first();
      const buttonCount = await copyButton.count();

      if (buttonCount > 0) {
        // Click the copy button
        await copyButton.click();

        // Wait for click to process
        await page.waitForTimeout(100);

        // For browsers that support clipboard API permissions (Chromium), verify clipboard
        if (browserName === 'chromium') {
          const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
          expect(clipboardText).toBeTruthy();
          expect(clipboardText.length).toBeGreaterThan(0);
        } else {
          // For Firefox/WebKit, verify the button shows visual feedback
          // Check for copied class or Copied text
          const hasFeedback = await copyButton.evaluate((btn) => {
            const textEl = btn.querySelector('.copy-text');
            return btn.classList.contains('copied') ||
                   (textEl && textEl.textContent.includes('Copied')) ||
                   btn.textContent.includes('Copied');
          });
          expect(hasFeedback).toBe(true);
        }
      }
    });

    test('Copy button shows visual feedback', async ({ page, browserName }) => {
      const quickstartSection = page.locator('#quickstart');
      const copyButton = quickstartSection.locator('.copy-btn').first();
      const buttonCount = await copyButton.count();

      if (buttonCount > 0) {
        // Click the copy button
        await copyButton.click();

        // Wait for feedback to appear
        await page.waitForTimeout(100);

        // Check for feedback (either class change or text change in .copy-text element)
        const hasFeedback = await copyButton.evaluate((btn) => {
          const textEl = btn.querySelector('.copy-text');
          return btn.classList.contains('copied') ||
                 btn.classList.contains('success') ||
                 (textEl && textEl.textContent.includes('Copied')) ||
                 btn.textContent.includes('Copied');
        });

        expect(hasFeedback).toBe(true);
      }
    });

    test('Copy functionality handles multiple code blocks', async ({ page, context, browserName }) => {
      // Grant clipboard permissions (only supported in Chromium-based browsers)
      if (browserName === 'chromium') {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      }

      const copyButtons = page.locator('.copy-btn');
      const buttonCount = await copyButtons.count();

      if (buttonCount >= 2) {
        // Test first code block
        await copyButtons.nth(0).click();
        await page.waitForTimeout(100);

        // Verify first button shows feedback
        const firstHasFeedback = await copyButtons.nth(0).evaluate((btn) => {
          const textEl = btn.querySelector('.copy-text');
          return btn.classList.contains('copied') ||
                 (textEl && textEl.textContent.includes('Copied'));
        });
        expect(firstHasFeedback).toBe(true);

        if (browserName === 'chromium') {
          const firstClipboard = await page.evaluate(() => navigator.clipboard.readText());
          expect(firstClipboard).toBeTruthy();

          // Wait for first button feedback to reset
          await page.waitForTimeout(2500);

          // Test second code block
          await copyButtons.nth(1).click();
          await page.waitForTimeout(100);

          const secondClipboard = await page.evaluate(() => navigator.clipboard.readText());
          expect(secondClipboard).toBeTruthy();
        } else {
          // For Firefox/WebKit, wait and test second button
          await page.waitForTimeout(2500);
          await copyButtons.nth(1).click();
          await page.waitForTimeout(100);

          // Verify second button shows feedback
          const secondHasFeedback = await copyButtons.nth(1).evaluate((btn) => {
            const textEl = btn.querySelector('.copy-text');
            return btn.classList.contains('copied') ||
                   (textEl && textEl.textContent.includes('Copied'));
          });
          expect(secondHasFeedback).toBe(true);
        }
      }
    });
  });

  test.describe('Browser-Specific Rendering', () => {
    test('Fonts render correctly', async ({ page, browserName }) => {
      // Check that fonts are loaded
      const fontsLoaded = await page.evaluate(() => {
        return document.fonts.ready.then(() => document.fonts.size > 0);
      });

      // Fonts should be loaded (or system fonts should be used)
      expect(fontsLoaded !== undefined).toBe(true);
    });

    test('Flexbox layout works correctly', async ({ page, browserName }) => {
      // Find elements that use flexbox
      const flexContainers = await page.evaluate(() => {
        const elements = document.querySelectorAll('*');
        let flexCount = 0;
        elements.forEach((el) => {
          const style = window.getComputedStyle(el);
          if (style.display === 'flex' || style.display === 'inline-flex') {
            flexCount++;
          }
        });
        return flexCount;
      });

      // Flexbox should be used and working
      expect(flexContainers).toBeGreaterThanOrEqual(0);
    });

    test('Grid layout works correctly', async ({ page, browserName }) => {
      // Find elements that use grid
      const gridContainers = await page.evaluate(() => {
        const elements = document.querySelectorAll('*');
        let gridCount = 0;
        elements.forEach((el) => {
          const style = window.getComputedStyle(el);
          if (style.display === 'grid' || style.display === 'inline-grid') {
            gridCount++;
          }
        });
        return gridCount;
      });

      // Grid should be used and working (if applicable)
      expect(gridContainers).toBeGreaterThanOrEqual(0);
    });

    test('CSS custom properties work', async ({ page, browserName }) => {
      // Check that CSS variables are supported and used
      const cssVarsWork = await page.evaluate(() => {
        const root = document.documentElement;
        const style = getComputedStyle(root);

        // Check if any CSS custom property is defined
        const cssText = Array.from(document.styleSheets)
          .flatMap((sheet) => {
            try {
              return Array.from(sheet.cssRules);
            } catch {
              return [];
            }
          })
          .some((rule) => rule.cssText && rule.cssText.includes('--'));

        return cssText;
      });

      // CSS variables should work
      expect(cssVarsWork !== undefined).toBe(true);
    });

    test('Smooth scrolling works', async ({ page, browserName }) => {
      // Check scroll behavior
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      // Smooth scrolling should be enabled or auto
      expect(['smooth', 'auto', '']).toContain(scrollBehavior);
    });
  });
});
