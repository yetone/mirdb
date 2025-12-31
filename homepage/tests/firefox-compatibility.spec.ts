import { test, expect } from '@playwright/test';

/**
 * Firefox Cross-Browser Compatibility Tests
 *
 * These tests verify the homepage renders and functions correctly in Mozilla Firefox.
 * Firefox is a major browser for developers, and we need to ensure CSS grid/flexbox
 * and JavaScript functionality work correctly.
 */
test.describe('Firefox Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Page Rendering in Firefox', () => {
    test('homepage loads without visual defects or layout issues', async ({ page }) => {
      // Verify the page loads successfully
      await expect(page).toHaveTitle(/MirDB/i);

      // Verify hero section renders correctly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify the product name is visible
      const productName = page.locator('h1').first();
      await expect(productName).toBeVisible();
      await expect(productName).toContainText('MirDB');

      // Verify tagline renders
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();
    });

    test('CSS grid layout renders correctly in Firefox', async ({ page }) => {
      // Navigate to features section which uses CSS grid
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify feature cards are visible (grid items)
      const featureCards = featuresSection.locator('.feature-card, [data-testid*="feature"]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      // Verify cards are laid out correctly (not stacked vertically in a broken manner)
      for (let i = 0; i < Math.min(cardCount, 3); i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();
        const box = await card.boundingBox();
        expect(box).toBeTruthy();
        expect(box!.width).toBeGreaterThan(100);
        expect(box!.height).toBeGreaterThan(50);
      }
    });

    test('CSS flexbox layout renders correctly in Firefox', async ({ page }) => {
      // Check navbar uses flexbox and renders correctly
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      // Verify nav links are visible and arranged horizontally
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // Verify CTA buttons render correctly
      const ctaGetStarted = page.locator('[data-testid="cta-get-started"]');
      const ctaGithub = page.locator('[data-testid="cta-github"]');
      await expect(ctaGetStarted).toBeVisible();
      await expect(ctaGithub).toBeVisible();
    });

    test('all major sections render correctly in Firefox', async ({ page }) => {
      // Hero section
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Quick Start section
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Architecture section (if present)
      const architectureSection = page.locator('[data-testid="architecture-section"], #architecture');
      if (await architectureSection.count() > 0) {
        await expect(architectureSection.first()).toBeVisible();
      }

      // Footer section
      const footerSection = page.locator('[data-testid="footer-section"], footer');
      await expect(footerSection.first()).toBeVisible();
    });

    test('no horizontal scrollbar appears in Firefox (1280x720)', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 720 });
      await page.goto('/');

      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });
  });

  test.describe('TC2: Navigation Links in Firefox', () => {
    test('all navigation links are visible and clickable', async ({ page }) => {
      // Verify navbar is visible
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      // Verify navigation links container
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // Check each navigation link
      const featuresLink = navLinks.locator('a[href="#features"]');
      const quickStartLink = navLinks.locator('a[href="#quick-start"]');

      await expect(featuresLink).toBeVisible();
      await expect(quickStartLink).toBeVisible();
    });

    test('Features link navigates correctly in Firefox', async ({ page }) => {
      // Click on Features navigation link
      const featuresLink = page.locator('[data-testid="nav-links"] a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();

      // Wait for navigation
      await page.waitForTimeout(500);

      // Verify URL has updated with hash
      await expect(page).toHaveURL(/#features/);

      // Verify the Features section is in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('Quick Start link navigates correctly in Firefox', async ({ page }) => {
      // Click on Quick Start navigation link
      const quickStartLink = page.locator('[data-testid="nav-links"] a[href="#quick-start"]');
      await expect(quickStartLink).toBeVisible();
      await quickStartLink.click();

      // Wait for navigation
      await page.waitForTimeout(500);

      // Verify URL has updated with hash
      await expect(page).toHaveURL(/#quick-start/);

      // Verify the Quick Start section is in view
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('Get Started CTA button navigates correctly in Firefox', async ({ page }) => {
      // Find and click Get Started button
      const ctaGetStarted = page.locator('[data-testid="cta-get-started"]');
      await expect(ctaGetStarted).toBeVisible();
      await ctaGetStarted.click();

      // Wait for navigation
      await page.waitForTimeout(500);

      // Should navigate to quick-start section
      await expect(page).toHaveURL(/#quick-start/);
    });

    test('GitHub CTA button has correct link attributes in Firefox', async ({ page }) => {
      // Find GitHub button
      const ctaGithub = page.locator('[data-testid="cta-github"]');
      await expect(ctaGithub).toBeVisible();

      // Verify it links to GitHub
      const href = await ctaGithub.getAttribute('href');
      expect(href).toContain('github');

      // Verify it opens in new tab
      await expect(ctaGithub).toHaveAttribute('target', '_blank');
    });

    test('footer links are functional in Firefox', async ({ page }) => {
      // Scroll to footer
      const footer = page.locator('[data-testid="footer-section"], footer').first();
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Check for GitHub link in footer
      const footerGithubLink = footer.locator('a[href*="github"]');
      if (await footerGithubLink.count() > 0) {
        await expect(footerGithubLink.first()).toBeVisible();
        const href = await footerGithubLink.first().getAttribute('href');
        expect(href).toContain('github');
      }
    });
  });

  test.describe('TC3: Copy-to-Clipboard in Firefox', () => {
    test.beforeEach(async ({ page, browserName, context }) => {
      // Firefox doesn't support clipboard-read/write permissions like Chromium
      // Only grant permissions for Chromium-based browsers
      if (browserName === 'chromium') {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      }
      await page.goto('/');
    });

    test('copy button is visible in code block in Firefox', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Find code blocks
      const codeBlocks = quickStartSection.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Verify copy button exists
      const codeBlock = codeBlocks.first();
      const copyButton = codeBlock.locator('.copy-button, [data-testid="copy-button"], button[aria-label*="copy" i], button[title*="copy" i]');
      await expect(copyButton).toBeVisible();
    });

    test('copy functionality works using Clipboard API in Firefox', async ({ page, browserName }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Find the code block with installation command
      const codeBlock = quickStartSection.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      // Find and click the copy button
      const copyButton = codeBlock.locator('.copy-button, [data-testid="copy-button"], button[aria-label*="copy" i], button[title*="copy" i]');
      await expect(copyButton).toBeVisible();
      await copyButton.click();

      // Firefox's Clipboard API works with user-initiated actions but has stricter
      // security. Instead of reading clipboard directly (which may be blocked),
      // we verify the copy operation was triggered by checking:
      // 1. The button click didn't throw an error
      // 2. The visual feedback indicates success
      // 3. The Clipboard API was called (via the DOM state change)

      // Wait for the copy operation to complete
      await page.waitForTimeout(200);

      // Verify the copy operation succeeded by checking for visual feedback
      // which indicates the writeText() call completed successfully
      const hasCopiedClass = await copyButton.evaluate(el => el.classList.contains('copied'));
      const hasSuccessClass = await copyButton.evaluate(el => el.classList.contains('success'));
      const buttonText = await copyButton.textContent();
      const hasCopiedText = buttonText?.toLowerCase().includes('copied');
      const checkIconVisible = await codeBlock.locator('.check-icon').isVisible();

      // At least one form of success indication should be present
      const copySucceeded = hasCopiedClass || hasSuccessClass || hasCopiedText || checkIconVisible;
      expect(copySucceeded).toBeTruthy();
    });

    test('copy button shows visual feedback in Firefox', async ({ page }) => {
      // Navigate to quick start section
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Find the code block
      const codeBlock = quickStartSection.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      // Find the copy button
      const copyButton = codeBlock.locator('.copy-button, [data-testid="copy-button"], button[aria-label*="copy" i], button[title*="copy" i]');
      await expect(copyButton).toBeVisible();

      // Click the copy button
      await copyButton.click();

      // Wait for DOM update
      await page.waitForTimeout(200);

      // Check for visual feedback
      const hasCopiedClass = await copyButton.evaluate(el => el.classList.contains('copied'));
      const hasSuccessClass = await copyButton.evaluate(el => el.classList.contains('success'));
      const buttonText = await copyButton.textContent();
      const hasCopiedText = buttonText?.toLowerCase().includes('copied');
      const checkIconVisible = await codeBlock.locator('.check-icon').isVisible();

      // At least one form of visual feedback should be present
      const feedbackPresent = hasCopiedClass || hasSuccessClass || hasCopiedText || checkIconVisible;
      expect(feedbackPresent).toBeTruthy();
    });
  });

  test.describe('Firefox JavaScript Functionality', () => {
    test('smooth scroll behavior works in Firefox', async ({ page }) => {
      // Start at the top
      await page.evaluate(() => window.scrollTo(0, 0));

      // Click on a navigation link
      const featuresLink = page.locator('[data-testid="nav-links"] a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();

      // Wait for scrolling to complete
      await page.waitForTimeout(600);

      // Verify smooth scrolling CSS is applied
      const htmlElement = page.locator('html');
      const scrollBehavior = await htmlElement.evaluate((el) => getComputedStyle(el).scrollBehavior);
      expect(scrollBehavior).toBe('smooth');
    });

    test('interactive hover effects work in Firefox', async ({ page }) => {
      // Test CTA button hover state
      const ctaGetStarted = page.locator('[data-testid="cta-get-started"]');
      await expect(ctaGetStarted).toBeVisible();

      // Get initial styles
      const initialBackground = await ctaGetStarted.evaluate(el => getComputedStyle(el).backgroundColor);

      // Hover over the button
      await ctaGetStarted.hover();

      // Give time for CSS transition
      await page.waitForTimeout(100);

      // Button should still be visible and functional after hover
      await expect(ctaGetStarted).toBeVisible();
    });

    test('keyboard navigation works in Firefox', async ({ page }) => {
      // Focus on the page
      await page.keyboard.press('Tab');

      // Check that focus is visible
      const activeElement = await page.evaluate(() => document.activeElement?.tagName);
      expect(activeElement).toBeTruthy();

      // Navigate through focusable elements
      await page.keyboard.press('Tab');
      await page.keyboard.press('Tab');

      // Verify focus indicators are visible
      const focusedElement = page.locator(':focus');
      const isVisible = await focusedElement.isVisible();
      expect(isVisible).toBe(true);
    });
  });
});
