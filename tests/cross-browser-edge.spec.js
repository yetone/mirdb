// @ts-check
const { test, expect, devices } = require('@playwright/test');

/**
 * Cross-Browser Microsoft Edge Compatibility Tests
 *
 * These tests verify that the MirDB homepage works correctly in Microsoft Edge,
 * addressing NFR-5 cross-browser compatibility requirement.
 */

// Configure test to use Microsoft Edge browser
test.use({
  channel: 'msedge',
});

test.describe('Cross-Browser - Edge Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Visual Elements Render Correctly in Edge', () => {
    test('Hero section renders with all visual elements', async ({ page }) => {
      // Verify hero section is visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify hero logo renders
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Verify hero title renders
      const heroTitle = page.locator('#hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Verify tagline renders
      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();

      // Verify CTA buttons render
      const primaryCta = page.locator('.hero-ctas .btn-primary');
      await expect(primaryCta).toBeVisible();
      const secondaryCta = page.locator('.hero-ctas .btn-secondary');
      await expect(secondaryCta).toBeVisible();
    });

    test('Navigation renders with all links', async ({ page }) => {
      // Verify navigation is visible
      const nav = page.locator('nav.nav');
      await expect(nav).toBeVisible();

      // Verify nav brand renders
      const navBrand = page.locator('.nav-brand');
      await expect(navBrand).toBeVisible();

      // Verify nav logo renders
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeVisible();

      // Verify nav links render
      const navLinks = page.locator('.nav-links li');
      await expect(navLinks).toHaveCount(4); // Features, Demo, Quick Start, GitHub
    });

    test('Features section renders with all feature cards', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Verify section title renders
      const featuresTitle = page.locator('#features-title');
      await expect(featuresTitle).toBeVisible();
      await expect(featuresTitle).toHaveText('Key Features');

      // Verify all 6 feature cards render
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(6);

      // Verify each feature card has visible title and description
      for (let i = 0; i < 6; i++) {
        const card = featureCards.nth(i);
        await expect(card.locator('.feature-title')).toBeVisible();
        await expect(card.locator('.feature-description')).toBeVisible();
        await expect(card.locator('.feature-icon')).toBeVisible();
      }
    });

    test('Demo section renders correctly', async ({ page }) => {
      // Navigate to demo section
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();
      await expect(demoSection).toBeVisible();

      // Verify demo title renders
      const demoTitle = page.locator('#demo-title');
      await expect(demoTitle).toBeVisible();
      await expect(demoTitle).toHaveText('See It in Action');

      // Verify demo GIF renders
      const demoGif = page.locator('.demo-gif');
      await expect(demoGif).toBeVisible();
      await expect(demoGif).toHaveAttribute('src', 'assets/usage.gif');
    });

    test('Quick start section renders with code blocks', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();
      await expect(quickStartSection).toBeVisible();

      // Verify code blocks render
      const codeBlocks = page.locator('.code-block');
      await expect(codeBlocks).toHaveCount(2); // Installation and Operations

      // Verify copy buttons render
      const copyButtons = page.locator('.copy-btn');
      await expect(copyButtons).toHaveCount(2);
    });

    test('Roadmap section renders with completed and planned items', async ({ page }) => {
      // Navigate to roadmap section
      const roadmapSection = page.locator('#roadmap');
      await roadmapSection.scrollIntoViewIfNeeded();
      await expect(roadmapSection).toBeVisible();

      // Verify completed list renders
      const completedList = page.locator('.roadmap-list.completed');
      await expect(completedList).toBeVisible();
      const completedItems = completedList.locator('li');
      await expect(completedItems).toHaveCount(4);

      // Verify planned list renders
      const plannedList = page.locator('.roadmap-list.planned');
      await expect(plannedList).toBeVisible();
    });

    test('Footer renders with all content', async ({ page }) => {
      // Navigate to footer
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify author attribution renders
      const authorLink = page.locator('.footer-author a');
      await expect(authorLink).toBeVisible();
      await expect(authorLink).toHaveAttribute('href', 'mailto:yetoneful@gmail.com');

      // Verify GitHub link renders
      const githubLink = page.locator('.footer-links a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toBeVisible();
    });

    test('CSS styles are applied correctly in Edge', async ({ page }) => {
      // Verify primary button styling is applied
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Check that CSS is loaded by verifying computed styles
      const bgColor = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      // Background color should be applied (not transparent/empty)
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(bgColor).not.toBe('transparent');

      // Verify body has styles applied
      const body = page.locator('body');
      const fontFamily = await body.evaluate(el =>
        window.getComputedStyle(el).fontFamily
      );
      // Font family should be defined
      expect(fontFamily).toBeTruthy();
      expect(fontFamily.length).toBeGreaterThan(0);
    });

    test('GIF images render with proper dimensions in Edge', async ({ page }) => {
      // Check that all GIF images render with proper dimensions in Edge
      const gifImages = page.locator('img[src$=".gif"]');
      const count = await gifImages.count();
      expect(count).toBeGreaterThan(0); // Ensure GIF images exist

      for (let i = 0; i < count; i++) {
        const img = gifImages.nth(i);
        await expect(img).toBeVisible();

        // Verify image has loaded and has valid rendered dimensions in Edge
        const dimensions = await img.evaluate((el) => ({
          naturalWidth: el.naturalWidth,
          naturalHeight: el.naturalHeight,
          clientWidth: el.clientWidth,
          clientHeight: el.clientHeight,
          complete: el.complete
        }));

        // Image should be fully loaded
        expect(dimensions.complete).toBe(true);
        // Image should have valid natural dimensions
        expect(dimensions.naturalWidth).toBeGreaterThan(0);
        expect(dimensions.naturalHeight).toBeGreaterThan(0);
        // Image should have valid rendered dimensions
        expect(dimensions.clientWidth).toBeGreaterThan(0);
        expect(dimensions.clientHeight).toBeGreaterThan(0);
      }
    });
  });

  test.describe('TC2: Clipboard API Copy Functionality in Edge', () => {
    test('Installation copy button works with Clipboard API in Edge', async ({ page, context }) => {
      // Grant clipboard permissions for Edge
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Find and click the installation copy button
      const copyButton = page.locator('.copy-btn[data-copy="install"]');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveText('Copy');

      // Click the copy button
      await copyButton.click();

      // Verify the clipboard contains the installation commands
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toContain('git clone https://github.com/yetone/mirdb');
      expect(clipboardContent).toContain('cargo build --release');
      expect(clipboardContent).toContain('mirdb-server');
    });

    test('Operations copy button works with Clipboard API in Edge', async ({ page, context }) => {
      // Grant clipboard permissions for Edge
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Find and click the operations copy button
      const copyButton = page.locator('.copy-btn[data-copy="operations"]');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveText('Copy');

      // Click the copy button
      await copyButton.click();

      // Verify the clipboard contains the operation commands
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toContain('set mykey');
      expect(clipboardContent).toContain('get mykey');
      expect(clipboardContent).toContain('delete mykey');
    });

    test('Copy button shows visual feedback after clicking in Edge', async ({ page, context }) => {
      // Grant clipboard permissions for Edge
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Find the copy button
      const copyButton = page.locator('.copy-btn[data-copy="install"]');
      await expect(copyButton).toBeVisible();

      // Initial state should be "Copy"
      await expect(copyButton).toHaveText('Copy');

      // Click the copy button
      await copyButton.click();

      // Should show "Copied!" as visual feedback
      await expect(copyButton).toHaveText('Copied!');

      // Should revert back to "Copy" after 2 seconds
      await expect(copyButton).toHaveText('Copy', { timeout: 3000 });
    });

    test('Clipboard API is available in Edge', async ({ page }) => {
      // Verify that the Clipboard API is available in Microsoft Edge
      const clipboardApiAvailable = await page.evaluate(() => {
        return typeof navigator.clipboard !== 'undefined' &&
               typeof navigator.clipboard.writeText === 'function' &&
               typeof navigator.clipboard.readText === 'function';
      });
      expect(clipboardApiAvailable).toBe(true);
    });
  });

  test.describe('Edge-Specific Feature Tests', () => {
    test('Logo is loaded and displayed in Edge', async ({ page }) => {
      // Check the hero logo
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Verify the image has loaded (naturalWidth > 0 means loaded)
      const isLoaded = await heroLogo.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(isLoaded).toBe(true);
    });

    test('Usage demo GIF is loaded correctly in Edge', async ({ page }) => {
      // Navigate to demo section
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();

      // Check the demo GIF
      const demoGif = page.locator('.demo-gif');
      await expect(demoGif).toBeVisible();

      // Verify it's a GIF file
      const src = await demoGif.getAttribute('src');
      expect(src).toBe('assets/usage.gif');

      // Wait for image to load with lazy loading
      await page.waitForFunction(
        selector => {
          const img = document.querySelector(selector);
          return img && img.complete && img.naturalWidth > 0;
        },
        '.demo-gif',
        { timeout: 10000 }
      );

      // Verify the image dimensions are valid
      const dimensions = await demoGif.evaluate((img) => ({
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight
      }));
      expect(dimensions.naturalWidth).toBeGreaterThan(0);
      expect(dimensions.naturalHeight).toBeGreaterThan(0);
    });

    test('Interactive elements are clickable in Edge', async ({ page }) => {
      // Test navigation links are clickable
      const featuresLink = page.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();

      // Verify scroll to features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('External links open correctly in Edge', async ({ page }) => {
      // Verify GitHub link has correct attributes for external navigation
      const githubLink = page.locator('.hero-ctas .btn-secondary');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', /noopener/);
    });

    test('Smooth scroll behavior works in Edge', async ({ page }) => {
      // Click on Quick Start link in the navigation (more specific selector)
      const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
      await expect(quickStartLink).toBeVisible();
      await quickStartLink.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(500);

      // Verify Quick Start section is now in viewport
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('Font rendering is correct in Edge', async ({ page }) => {
      // Verify fonts are loading and rendering correctly
      const heroTitle = page.locator('#hero-title');
      const fontFamily = await heroTitle.evaluate(el =>
        window.getComputedStyle(el).fontFamily
      );
      // Font family should be properly defined
      expect(fontFamily).toBeTruthy();
      expect(fontFamily.length).toBeGreaterThan(0);
    });
  });
});
