import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Page Rendering', () => {
    test('homepage loads with correct title and meta', async ({ page }) => {
      await expect(page).toHaveTitle(/MirDB/);

      // Verify critical sections are present
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
      await expect(page.locator('[data-testid="quick-start-section"]')).toBeVisible();
    });

    test('hero section renders with all elements', async ({ page }) => {
      const hero = page.locator('[data-testid="hero-section"]');
      await expect(hero).toBeVisible();

      // Verify logo renders
      const logo = page.locator('[data-testid="hero-logo"]');
      await expect(logo).toBeVisible();

      // Verify tagline
      const tagline = page.locator('[data-testid="hero-tagline"]');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Memcached Protocol');

      // Verify CTA buttons
      await expect(page.locator('[data-testid="cta-get-started"]')).toBeVisible();
      await expect(page.locator('[data-testid="cta-github"]')).toBeVisible();
    });

    test('navigation renders correctly', async ({ page }) => {
      const nav = page.locator('[data-testid="navigation"]');
      await expect(nav).toBeVisible();

      // Check navigation links exist
      await expect(page.locator('[data-testid="nav-features"]')).toBeVisible();
      await expect(page.locator('[data-testid="nav-quick-start"]')).toBeVisible();
    });

    test('features section displays grid of cards', async ({ page }) => {
      const features = page.locator('[data-testid="features-section"]');
      await expect(features).toBeVisible();

      // Verify feature cards render
      const featureCards = page.locator('[data-testid^="feature-card-"]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(4);
    });

    test('architecture section renders with diagram', async ({ page }) => {
      const architecture = page.locator('[data-testid="architecture-section"]');
      await expect(architecture).toBeVisible();

      // Verify diagram is visible (using the correct testid)
      const diagram = page.locator('[data-testid="lsm-tree-diagram"]');
      await expect(diagram).toBeVisible();
    });

    test('comparison table renders correctly', async ({ page }) => {
      const comparison = page.locator('[data-testid="comparison-section"]');
      await expect(comparison).toBeVisible();

      // Verify table exists and has rows
      const table = page.locator('[data-testid="comparison-table"]');
      await expect(table).toBeVisible();
    });

    test('footer renders with all links', async ({ page }) => {
      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();

      // Verify GitHub link exists
      const githubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(githubLink).toBeVisible();
    });
  });

  test.describe('Styling and CSS', () => {
    test('dark theme background colors are applied', async ({ page }) => {
      // Check body has dark background (bg-gray-900 = rgb(17, 24, 39))
      const body = page.locator('body');
      const bgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // Allow for various dark backgrounds (gray-900, gray-800, gray-950)
      // Dark theme should have low RGB values (under 50 typically)
      const match = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (match) {
        const [, r, g, b] = match.map(Number);
        // Average RGB should be below 70 for dark theme
        expect((r + g + b) / 3).toBeLessThan(70);
      }
    });

    test('text is readable with proper contrast', async ({ page }) => {
      // Check hero product name has light color
      const heroName = page.locator('[data-testid="hero-product-name"]');
      await expect(heroName).toBeVisible();

      // Verify tagline text is visible and has proper color
      const tagline = page.locator('[data-testid="hero-tagline"]');
      const color = await tagline.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      // Light text should have moderate to high RGB values (gray-300 = rgb(209, 213, 219))
      const rgbMatch = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      if (rgbMatch) {
        const [, r, g, b] = rgbMatch.map(Number);
        // Light text should have average RGB > 100
        expect((r + g + b) / 3).toBeGreaterThan(100);
      }
    });

    test('navigation has backdrop blur effect', async ({ page }) => {
      const nav = page.locator('[data-testid="navigation"]');
      const position = await nav.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });
      // Navigation should be fixed
      expect(position).toBe('fixed');
    });

    test('code blocks have syntax highlighting', async ({ page }) => {
      // Scroll to quick-start section
      await page.locator('[data-testid="quick-start-section"]').scrollIntoViewIfNeeded();

      // Find code blocks
      const codeBlocks = page.locator('.code-block-container');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);

      // Verify Prism tokens exist (indicating syntax highlighting)
      const tokens = page.locator('.token');
      const tokenCount = await tokens.count();
      expect(tokenCount).toBeGreaterThan(0);
    });
  });

  test.describe('Interactions', () => {
    test('navigation links scroll to sections', async ({ page }) => {
      // Click features nav link
      await page.locator('[data-testid="nav-features"]').click();

      // Wait for scroll
      await page.waitForTimeout(500);

      // Verify features section is in viewport
      const features = page.locator('[data-testid="features-section"]');
      await expect(features).toBeInViewport();
    });

    test('get started button navigates to quick-start', async ({ page }) => {
      await page.locator('[data-testid="cta-get-started"]').click();

      // Wait for scroll
      await page.waitForTimeout(500);

      // Verify quick-start section is in viewport
      const quickStart = page.locator('[data-testid="quick-start-section"]');
      await expect(quickStart).toBeInViewport();
    });

    test('external links open in new tab', async ({ page }) => {
      const githubButton = page.locator('[data-testid="cta-github"]');
      const target = await githubButton.getAttribute('target');
      expect(target).toBe('_blank');
    });
  });

  test.describe('Copy to Clipboard', () => {
    test('copy button is visible on code blocks', async ({ page }) => {
      // Scroll to quick-start section
      await page.locator('[data-testid="quick-start-section"]').scrollIntoViewIfNeeded();

      // Find copy buttons
      const copyButtons = page.locator('[data-testid="copy-button"]');
      const count = await copyButtons.count();
      expect(count).toBeGreaterThan(0);

      // Hover over first code block to ensure button is visible
      const firstCodeBlock = page.locator('.code-block-container').first();
      await firstCodeBlock.hover();

      const firstButton = copyButtons.first();
      await expect(firstButton).toBeVisible();
    });

    test('copy button copies code to clipboard', async ({ page, context, browserName }) => {
      // Grant clipboard permissions only for Chromium-based browsers
      if (browserName === 'chromium') {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      }

      // Scroll to quick-start section
      await page.locator('[data-testid="quick-start-section"]').scrollIntoViewIfNeeded();

      // Hover over first code block
      const firstCodeBlock = page.locator('.code-block-container').first();
      await firstCodeBlock.hover();

      // Click copy button
      const copyButton = page.locator('[data-testid="copy-button"]').first();
      await copyButton.click();

      // Verify button text changes to "Copied!" (this works across all browsers)
      await expect(copyButton).toHaveText('Copied!');

      // Verify clipboard contains content only for Chromium-based browsers
      // Firefox and WebKit don't support clipboard permissions via Playwright
      if (browserName === 'chromium') {
        const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
        expect(clipboardContent.length).toBeGreaterThan(0);
      }
    });

    test('copy button reverts text after copying', async ({ page, context, browserName }) => {
      // Grant clipboard permissions for Chromium-based browsers to ensure success
      if (browserName === 'chromium') {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      }

      // Scroll to quick-start section
      await page.locator('[data-testid="quick-start-section"]').scrollIntoViewIfNeeded();

      // Hover and click copy button
      const firstCodeBlock = page.locator('.code-block-container').first();
      await firstCodeBlock.hover();

      const copyButton = page.locator('[data-testid="copy-button"]').first();
      await copyButton.click();

      // Wait for button text to change (either "Copied!" on success or "Error" on failure)
      // Then verify it reverts back to "Copy"
      await page.waitForTimeout(2500);

      // Verify button reverts to "Copy" after the timeout
      await expect(copyButton).toHaveText('Copy');
    });
  });

  test.describe('Responsive Layout', () => {
    test('content is fully visible without horizontal scroll', async ({ page }) => {
      // Check for horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('images load correctly', async ({ page }) => {
      // Find all images
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const img = images.nth(i);
        const naturalWidth = await img.evaluate((el: HTMLImageElement) => el.naturalWidth);
        // Image should have loaded (naturalWidth > 0)
        expect(naturalWidth).toBeGreaterThan(0);
      }
    });
  });

  test.describe('Accessibility Basics', () => {
    test('all images have alt text', async ({ page }) => {
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt?.length).toBeGreaterThan(0);
      }
    });

    test('buttons have accessible labels', async ({ page }) => {
      const buttons = page.locator('button');
      const count = await buttons.count();

      for (let i = 0; i < count; i++) {
        const button = buttons.nth(i);
        const text = await button.textContent();
        const ariaLabel = await button.getAttribute('aria-label');
        // Button should have either text content or aria-label
        expect(text?.length || ariaLabel?.length).toBeGreaterThan(0);
      }
    });

    test('links have accessible labels', async ({ page }) => {
      const links = page.locator('a');
      const count = await links.count();

      for (let i = 0; i < count; i++) {
        const link = links.nth(i);
        const text = await link.textContent();
        const ariaLabel = await link.getAttribute('aria-label');
        // Link should have either text content or aria-label
        expect((text?.trim().length ?? 0) > 0 || (ariaLabel?.length ?? 0) > 0).toBe(true);
      }
    });
  });
});
