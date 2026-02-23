// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser E2E Tests
 * Owner: Scenario 18 - Cross-Browser Compatibility
 *
 * End-to-end tests for cross-browser compatibility:
 * - Page renders in Chrome
 * - Page renders in Firefox
 * - Page renders in Safari (WebKit)
 * - Page renders in Edge
 * - Copy functionality in all browsers
 * - Layout consistency across browsers
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Page Rendering', () => {
    test('TC1: Page loads and renders correctly with all main sections', async ({ page, browserName }) => {
      // Verify page title
      await expect(page).toHaveTitle(/MirDB/);

      // Verify all main sections are visible
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const roadmapSection = page.locator('#roadmap');
      await expect(roadmapSection).toBeVisible();

      const configurationSection = page.locator('#configuration');
      await expect(configurationSection).toBeVisible();

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Log browser name for test report
      console.log(`TC1 passed in ${browserName}`);
    });

    test('TC2: Hero section displays correctly', async ({ page, browserName }) => {
      // Verify logo loads
      const logo = page.locator('.hero-logo');
      await expect(logo).toBeVisible();

      // Check logo actually loaded (not broken)
      const logoNaturalWidth = await logo.evaluate((img) => {
        return (img).naturalWidth;
      });
      expect(logoNaturalWidth).toBeGreaterThan(0);

      // Verify headline
      const headline = page.locator('.hero-headline');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('MirDB');

      // Verify CTA button
      const ctaButton = page.locator('.hero-cta');
      await expect(ctaButton).toBeVisible();

      console.log(`TC2 passed in ${browserName}`);
    });

    test('TC3: Navigation links are visible and functional', async ({ page, browserName }) => {
      // Check navigation exists
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Check navigation links
      const navLinks = page.locator('.nav__links a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThanOrEqual(4);

      // Verify each link is visible
      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        await expect(link).toBeVisible();
      }

      console.log(`TC3 passed in ${browserName}`);
    });

    test('TC4: Code blocks render with proper styling', async ({ page, browserName }) => {
      // Find code blocks
      const codeBlocks = page.locator('.code-block');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      // Check first code block has proper structure
      const firstBlock = codeBlocks.first();
      await expect(firstBlock).toBeVisible();

      // Verify code element exists
      const codeElement = firstBlock.locator('code');
      await expect(codeElement).toBeVisible();

      // Check for language label
      const languageLabel = firstBlock.locator('.code-block__language');
      await expect(languageLabel).toBeVisible();

      // Check for copy button
      const copyButton = firstBlock.locator('.code-block__copy');
      await expect(copyButton).toBeVisible();

      console.log(`TC4 passed in ${browserName}`);
    });
  });

  test.describe('Copy to Clipboard Functionality', () => {
    test('TC5: Copy button copies code to clipboard', async ({ page, context, browserName }) => {
      // Grant clipboard permissions - only for Chromium-based browsers
      if (browserName === 'chromium') {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      }

      // Find the first code block copy button
      const firstCodeBlock = page.locator('#quickstart .code-block').first();
      const copyButton = firstCodeBlock.locator('.code-block__copy');

      await expect(copyButton).toBeVisible();
      await copyButton.click();

      // Wait for copy feedback - this works across all browsers
      await expect(copyButton).toHaveClass(/code-block__copy--copied/, { timeout: 5000 });

      // Verify clipboard content - only for Chromium where we can read clipboard
      if (browserName === 'chromium') {
        const clipboardText = await page.evaluate(() => navigator.clipboard.readText());
        expect(clipboardText).toBe('cargo install mirdb');
      }

      console.log(`TC5 passed in ${browserName}`);
    });

    test('TC6: Copy button shows visual feedback after click', async ({ page, browserName }) => {
      const copyButton = page.locator('#quickstart .code-block__copy').first();
      await expect(copyButton).toBeVisible();

      // Check initial state
      const copyText = copyButton.locator('.code-block__copy-text');
      await expect(copyText).toHaveText('Copy');

      // Click the button
      await copyButton.click();

      // Check visual feedback - this should work across all browsers
      await expect(copyText).toHaveText('Copied!');
      await expect(copyButton).toHaveClass(/code-block__copy--copied/);

      console.log(`TC6 passed in ${browserName}`);
    });

    test('TC7: Copy button is keyboard accessible', async ({ page, browserName }) => {
      const copyButton = page.locator('#quickstart .code-block__copy').first();

      // Tab to the button
      await copyButton.focus();
      await expect(copyButton).toBeFocused();

      // Verify button has accessible label
      const ariaLabel = await copyButton.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();

      console.log(`TC7 passed in ${browserName}`);
    });
  });

  test.describe('CSS Layout Consistency', () => {
    test('TC8: Hero section has correct layout', async ({ page, browserName }) => {
      const heroSection = page.locator('#hero');
      const boundingBox = await heroSection.boundingBox();

      // Verify section has reasonable dimensions
      expect(boundingBox).not.toBeNull();
      expect(boundingBox.width).toBeGreaterThan(0);
      expect(boundingBox.height).toBeGreaterThan(0);

      // Verify hero elements are centered
      const logo = page.locator('.hero-logo');
      const logoBoundingBox = await logo.boundingBox();
      expect(logoBoundingBox).not.toBeNull();

      // Logo should be somewhat centered horizontally
      const heroCenter = boundingBox.x + boundingBox.width / 2;
      const logoCenter = logoBoundingBox.x + logoBoundingBox.width / 2;
      const tolerance = boundingBox.width * 0.2; // 20% tolerance
      expect(Math.abs(heroCenter - logoCenter)).toBeLessThan(tolerance);

      console.log(`TC8 passed in ${browserName}`);
    });

    test('TC9: No horizontal scroll on page', async ({ page, browserName }) => {
      // Check that there's no horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBeFalsy();

      console.log(`TC9 passed in ${browserName}`);
    });

    test('TC10: Features section displays as list or grid', async ({ page, browserName }) => {
      const featuresList = page.locator('.features__list');
      await expect(featuresList).toBeVisible();

      const featureItems = page.locator('.features__item');
      const count = await featureItems.count();
      expect(count).toBeGreaterThan(0);

      // Verify all items are visible
      for (let i = 0; i < count; i++) {
        const item = featureItems.nth(i);
        await expect(item).toBeVisible();
      }

      console.log(`TC10 passed in ${browserName}`);
    });

    test('TC11: Footer renders correctly', async ({ page, browserName }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Check GitHub link
      const githubLink = page.locator('[data-testid="github-link"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Check CircleCI badge
      const circleciImg = page.locator('[data-testid="circleci-img"]');
      await expect(circleciImg).toBeVisible();

      console.log(`TC11 passed in ${browserName}`);
    });
  });

  test.describe('Interactive Elements', () => {
    test('TC12: CTA button scrolls to Quick Start section', async ({ page, browserName }) => {
      const ctaButton = page.locator('.hero-cta');
      await expect(ctaButton).toBeVisible();

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click CTA button
      await ctaButton.click();

      // Wait for scroll animation
      await page.waitForTimeout(1000);

      // Verify scroll position changed
      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(finalScrollY).toBeGreaterThan(initialScrollY);

      // Verify quickstart section is in view
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();

      console.log(`TC12 passed in ${browserName}`);
    });

    test('TC13: Navigation links scroll to sections', async ({ page, browserName }) => {
      // Click on Features link
      const featuresLink = page.locator('.nav__links a[href="#features"]');
      await featuresLink.click();

      // Wait for scroll
      await page.waitForTimeout(500);

      // Verify features section is in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();

      console.log(`TC13 passed in ${browserName}`);
    });

    test('TC14: External links have correct attributes', async ({ page, browserName }) => {
      // Check GitHub link in navigation
      const githubNavLink = page.locator('.nav__links a[href="https://github.com/yetone/mirdb"]');
      await expect(githubNavLink).toHaveAttribute('target', '_blank');
      await expect(githubNavLink).toHaveAttribute('rel', 'noopener');

      // Check footer GitHub link
      const githubFooterLink = page.locator('[data-testid="github-link"]');
      await expect(githubFooterLink).toHaveAttribute('target', '_blank');
      await expect(githubFooterLink).toHaveAttribute('rel', 'noopener');

      console.log(`TC14 passed in ${browserName}`);
    });
  });

  test.describe('Fonts and Typography', () => {
    test('TC15: Headings render correctly', async ({ page, browserName }) => {
      // Check h1 exists
      const h1 = page.locator('h1');
      await expect(h1).toHaveCount(1);
      await expect(h1).toBeVisible();

      // Check h2 headings
      const h2s = page.locator('h2');
      const h2Count = await h2s.count();
      expect(h2Count).toBeGreaterThan(0);

      // Verify h2s are visible
      for (let i = 0; i < h2Count; i++) {
        const h2 = h2s.nth(i);
        await expect(h2).toBeVisible();
      }

      console.log(`TC15 passed in ${browserName}`);
    });

    test('TC16: Code blocks use monospace font', async ({ page, browserName }) => {
      const codeElement = page.locator('.code-block code').first();
      await expect(codeElement).toBeVisible();

      // Check font-family contains monospace
      const fontFamily = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Font family should include monospace or a known mono font
      const hasMonospace = fontFamily.toLowerCase().includes('mono') ||
                          fontFamily.includes('Consolas') ||
                          fontFamily.includes('Courier') ||
                          fontFamily.includes('Source Code') ||
                          fontFamily.includes('Menlo');
      expect(hasMonospace).toBeTruthy();

      console.log(`TC16 passed in ${browserName}`);
    });
  });

  test.describe('Images and Assets', () => {
    test('TC17: Logo image loads correctly', async ({ page, browserName }) => {
      const logo = page.locator('.hero-logo');
      await expect(logo).toBeVisible();

      // Check image dimensions
      const dimensions = await logo.evaluate((img) => ({
        natural: { width: img.naturalWidth, height: img.naturalHeight },
        display: { width: img.width, height: img.height }
      }));

      expect(dimensions.natural.width).toBeGreaterThan(0);
      expect(dimensions.natural.height).toBeGreaterThan(0);
      expect(dimensions.display.width).toBeGreaterThan(0);
      expect(dimensions.display.height).toBeGreaterThan(0);

      console.log(`TC17 passed in ${browserName}`);
    });

    test('TC18: All images have alt text', async ({ page, browserName }) => {
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        expect(alt).toBeTruthy();
        expect(alt.length).toBeGreaterThan(0);
      }

      console.log(`TC18 passed in ${browserName}`);
    });
  });
});
