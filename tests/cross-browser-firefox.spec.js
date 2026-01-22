// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Firefox Compatibility Tests
 *
 * These tests verify that the MirDB homepage works correctly in Mozilla Firefox,
 * addressing NFR-5 cross-browser compatibility requirement.
 *
 * Test Cases:
 * TC1: Load page in Firefox latest - All visual elements render correctly
 * TC2: Test copy functionality in Firefox - Clipboard API works for copy buttons
 * TC3: Test CSS grid/flexbox in Firefox - Layout displays correctly without vendor prefix issues
 */

// Only run these tests in Firefox
test.describe('Cross-Browser - Firefox Compatibility', () => {
  test.skip(({ browserName }) => browserName !== 'firefox', 'Firefox-only tests');

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Visual Elements Render Correctly in Firefox', () => {
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
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify CTA buttons render
      const primaryCta = page.locator('.hero-ctas .btn-primary');
      await expect(primaryCta).toBeVisible();
      const secondaryCta = page.locator('.hero-ctas .btn-secondary');
      await expect(secondaryCta).toBeVisible();
    });

    test('Navigation renders with all links in Firefox', async ({ page }) => {
      // Verify navigation is visible
      const nav = page.locator('nav.nav');
      await expect(nav).toBeVisible();

      // Verify nav brand renders
      const navBrand = page.locator('.nav-brand');
      await expect(navBrand).toBeVisible();

      // Verify nav logo renders
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeVisible();

      // Verify nav links render - 4 links: Features, Demo, Quick Start, GitHub
      const navLinks = page.locator('.nav-links li');
      await expect(navLinks).toHaveCount(4);
    });

    test('Features section renders with all feature cards in Firefox', async ({ page }) => {
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

    test('Demo section renders correctly in Firefox', async ({ page }) => {
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

    test('Quick start section renders with code blocks in Firefox', async ({ page }) => {
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

    test('Roadmap section renders with completed and planned items in Firefox', async ({ page }) => {
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

    test('Footer renders with all content in Firefox', async ({ page }) => {
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

    test('CSS styles are applied correctly in Firefox', async ({ page }) => {
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

    test('Images load correctly in Firefox', async ({ page }) => {
      // Check hero logo loads
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Verify the image has loaded (naturalWidth > 0 means loaded)
      const heroLoaded = await heroLogo.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(heroLoaded).toBe(true);

      // Check nav logo loads
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeVisible();
      const navLoaded = await navLogo.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(navLoaded).toBe(true);
    });
  });

  test.describe('TC2: Clipboard API Copy Functionality in Firefox', () => {
    test('Clipboard API is available in Firefox', async ({ page }) => {
      // Verify that the Clipboard API is available in Firefox
      const clipboardApiAvailable = await page.evaluate(() => {
        return typeof navigator.clipboard !== 'undefined' &&
               typeof navigator.clipboard.writeText === 'function';
      });
      expect(clipboardApiAvailable).toBe(true);
    });

    test('Installation copy button is functional and accessible in Firefox', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Find the installation copy button
      const copyButton = page.locator('.copy-btn[data-copy="install"]');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveText('Copy');

      // Verify button has proper accessibility attributes
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy installation commands');

      // Verify the code block has the content to be copied
      const codeBlock = page.locator('#install-code');
      await expect(codeBlock).toBeVisible();
      const codeContent = await codeBlock.textContent();
      expect(codeContent).toContain('git clone https://github.com/yetone/mirdb');
      expect(codeContent).toContain('cargo build --release');
      expect(codeContent).toContain('mirdb-server');
    });

    test('Operations copy button is functional and accessible in Firefox', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Find the operations copy button
      const copyButton = page.locator('.copy-btn[data-copy="operations"]');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveText('Copy');

      // Verify button has proper accessibility attributes
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy operation examples');

      // Verify the code block has the content to be copied
      const codeBlock = page.locator('#operations-code');
      await expect(codeBlock).toBeVisible();
      const codeContent = await codeBlock.textContent();
      expect(codeContent).toContain('set mykey');
      expect(codeContent).toContain('get mykey');
      expect(codeContent).toContain('delete mykey');
    });

    test('Copy button shows visual feedback after clicking in Firefox', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Find the copy button
      const copyButton = page.locator('.copy-btn[data-copy="install"]');
      await expect(copyButton).toBeVisible();

      // Initial state should be "Copy"
      await expect(copyButton).toHaveText('Copy');

      // Click the copy button - in Firefox, the clipboard write will be attempted
      // and visual feedback should be shown regardless of clipboard permission
      await copyButton.click();

      // Should show "Copied!" as visual feedback (indicates clipboard.writeText was called successfully)
      await expect(copyButton).toHaveText('Copied!');

      // Should revert back to "Copy" after 2 seconds
      await expect(copyButton).toHaveText('Copy', { timeout: 3000 });
    });

    test('Copy buttons respond to click events correctly in Firefox', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Test both copy buttons respond correctly
      const installCopyBtn = page.locator('.copy-btn[data-copy="install"]');
      const operationsCopyBtn = page.locator('.copy-btn[data-copy="operations"]');

      // Click install button and verify feedback
      await installCopyBtn.click();
      await expect(installCopyBtn).toHaveText('Copied!');

      // Wait for reset
      await expect(installCopyBtn).toHaveText('Copy', { timeout: 3000 });

      // Click operations button and verify feedback
      await operationsCopyBtn.click();
      await expect(operationsCopyBtn).toHaveText('Copied!');

      // Both buttons should work independently
      await expect(installCopyBtn).toHaveText('Copy'); // Should still be reset
    });
  });

  test.describe('TC3: CSS Grid/Flexbox Layout in Firefox', () => {
    test('Features grid layout displays correctly without vendor prefix issues', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Get the features grid container
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Verify CSS Grid is applied correctly in Firefox
      const gridStyles = await featuresGrid.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns
        };
      });

      // Should use CSS Grid
      expect(gridStyles.display).toBe('grid');
      // Grid should have columns defined (not just 'none')
      expect(gridStyles.gridTemplateColumns).not.toBe('none');
    });

    test('Navigation uses flexbox correctly in Firefox', async ({ page }) => {
      // Check navigation flexbox layout
      const nav = page.locator('nav.nav');
      await expect(nav).toBeVisible();

      const navStyles = await nav.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          justifyContent: styles.justifyContent,
          alignItems: styles.alignItems
        };
      });

      // Navigation should use flexbox
      expect(navStyles.display).toBe('flex');
      // Should have proper alignment
      expect(navStyles.justifyContent).toBe('space-between');
      expect(navStyles.alignItems).toBe('center');
    });

    test('Hero CTAs use flexbox layout correctly in Firefox', async ({ page }) => {
      // Check hero CTAs flexbox layout
      const heroCtas = page.locator('.hero-ctas');
      await expect(heroCtas).toBeVisible();

      const ctaStyles = await heroCtas.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          justifyContent: styles.justifyContent,
          flexWrap: styles.flexWrap,
          gap: styles.gap
        };
      });

      // CTAs container should use flexbox
      expect(ctaStyles.display).toBe('flex');
      // Should center the buttons
      expect(ctaStyles.justifyContent).toBe('center');
    });

    test('Feature cards layout in grid with proper gap in Firefox', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();
      expect(count).toBe(6);

      // Verify each card is visible and properly positioned
      for (let i = 0; i < count; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        // Check card has proper dimensions (not collapsed)
        const boundingBox = await card.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.width).toBeGreaterThan(100);
        expect(boundingBox.height).toBeGreaterThan(50);
      }
    });

    test('Roadmap section layout displays correctly in Firefox', async ({ page }) => {
      // Navigate to roadmap section
      const roadmapSection = page.locator('#roadmap');
      await roadmapSection.scrollIntoViewIfNeeded();

      // Check roadmap content layout
      const roadmapContent = page.locator('.roadmap-content');
      await expect(roadmapContent).toBeVisible();

      // Verify both roadmap sections are visible
      const roadmapSections = page.locator('.roadmap-section');
      await expect(roadmapSections).toHaveCount(2);

      // Both sections should have proper width
      for (let i = 0; i < 2; i++) {
        const section = roadmapSections.nth(i);
        const boundingBox = await section.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.width).toBeGreaterThan(100);
      }
    });

    test('Footer layout uses flexbox correctly in Firefox', async ({ page }) => {
      // Navigate to footer
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Check footer content layout
      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();

      const footerStyles = await footerContent.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          justifyContent: styles.justifyContent,
          alignItems: styles.alignItems
        };
      });

      // Footer content should use flexbox
      expect(footerStyles.display).toBe('flex');
    });

    test('Code blocks layout displays correctly in Firefox', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Check code blocks
      const codeBlocks = page.locator('.code-block');
      await expect(codeBlocks).toHaveCount(2);

      // Verify each code block has proper dimensions
      for (let i = 0; i < 2; i++) {
        const block = codeBlocks.nth(i);
        await expect(block).toBeVisible();

        const boundingBox = await block.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.width).toBeGreaterThan(200);
        expect(boundingBox.height).toBeGreaterThan(50);
      }
    });

    test('CSS custom properties (variables) work correctly in Firefox', async ({ page }) => {
      // Firefox has good support for CSS custom properties, but let's verify
      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Check that CSS custom properties are resolved correctly
      const btnStyles = await primaryBtn.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          borderRadius: styles.borderRadius
        };
      });

      // Background color should be the primary color (blue)
      // RGB values for #2563eb
      expect(btnStyles.backgroundColor).toMatch(/rgb\(37,\s*99,\s*235\)/);
    });

    test('Responsive container max-width works in Firefox', async ({ page }) => {
      // Check container max-width
      const containers = page.locator('.container');
      const count = await containers.count();
      expect(count).toBeGreaterThan(0);

      // First container should have max-width set
      const container = containers.first();
      const containerStyles = await container.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          maxWidth: styles.maxWidth,
          margin: styles.margin
        };
      });

      // Container should have max-width defined
      expect(containerStyles.maxWidth).not.toBe('none');
    });
  });
});
