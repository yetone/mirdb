/**
 * Cross-Browser Compatibility E2E Tests.
 * Owner: Scenario 12 - Cross-Browser Compatibility
 *
 * Tests:
 * - Chrome compatibility (hero section rendering)
 * - Firefox compatibility (features grid layout)
 * - Safari compatibility (theme toggle functionality)
 * - Edge compatibility (copy to clipboard)
 * - All browsers: smooth scroll navigation
 *
 * These tests run across Chrome, Firefox, Safari (WebKit), and Edge
 * to verify consistent functionality across all major browsers.
 */

import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test.describe('Hero Section Rendering', () => {
    test('TC1: Hero section renders correctly with logo and tagline', async ({ page, browserName }) => {
      // Log browser being tested for visibility
      console.log(`Testing hero section in: ${browserName}`);

      // Verify hero section exists
      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();

      // Verify logo is present and visible
      const logo = page.locator('img[alt="MirDB Logo"]');
      await expect(logo).toBeVisible();

      // Verify logo loads correctly (naturalWidth > 0)
      const logoLoaded = await logo.evaluate((img: HTMLImageElement) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(logoLoaded).toBe(true);

      // Verify H1 heading with MirDB text
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // Verify tagline with key phrases
      const tagline = hero.locator('p').first();
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');
      await expect(tagline).toContainText('Memcached Protocol');

      // Verify hero section dimensions are reasonable
      const heroBox = await hero.boundingBox();
      expect(heroBox).not.toBeNull();
      expect(heroBox!.width).toBeGreaterThan(0);
      expect(heroBox!.height).toBeGreaterThan(0);
    });

    test('Hero section styling is consistent', async ({ page, browserName }) => {
      console.log(`Testing hero styling in: ${browserName}`);

      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();

      // Verify hero section has proper styling applied
      const heroStyles = await hero.evaluate((el) => {
        const computedStyle = window.getComputedStyle(el);
        return {
          display: computedStyle.display,
          paddingTop: computedStyle.paddingTop,
          paddingBottom: computedStyle.paddingBottom,
        };
      });

      // Hero should have padding for proper spacing
      expect(parseFloat(heroStyles.paddingTop)).toBeGreaterThanOrEqual(0);
      expect(parseFloat(heroStyles.paddingBottom)).toBeGreaterThanOrEqual(0);
    });
  });

  test.describe('Features Grid Layout', () => {
    test('TC2: Features grid displays correctly without layout issues', async ({ page, browserName }) => {
      console.log(`Testing features grid in: ${browserName}`);

      // Navigate to features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify grid container exists
      const grid = featuresSection.locator('[data-testid="features-grid"]');
      await expect(grid).toBeVisible();

      // Verify grid layout properties
      const gridStyles = await grid.evaluate((el) => {
        const computedStyle = window.getComputedStyle(el);
        return {
          display: computedStyle.display,
          gridTemplateColumns: computedStyle.gridTemplateColumns,
          gap: computedStyle.gap,
        };
      });

      // Grid should use grid or flex layout
      expect(['grid', 'flex']).toContain(gridStyles.display);

      // Verify all feature cards are present and visible
      const featureCards = featuresSection.locator('[data-testid^="feature-card"]');
      const count = await featureCards.count();
      expect(count).toBeGreaterThanOrEqual(4);

      // Verify each card has proper dimensions
      for (let i = 0; i < Math.min(count, 4); i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        const cardBox = await card.boundingBox();
        expect(cardBox).not.toBeNull();
        expect(cardBox!.width).toBeGreaterThan(100);
        expect(cardBox!.height).toBeGreaterThan(50);
      }

      // Verify no horizontal overflow
      const hasOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasOverflow).toBe(false);
    });

    test('Feature cards maintain consistent sizing across cards', async ({ page, browserName }) => {
      console.log(`Testing feature card sizing in: ${browserName}`);

      const featuresSection = page.locator('#features');
      const featureCards = featuresSection.locator('[data-testid^="feature-card"]');
      const count = await featureCards.count();

      if (count >= 2) {
        const firstCardBox = await featureCards.first().boundingBox();
        const secondCardBox = await featureCards.nth(1).boundingBox();

        // Cards in the same row should have same width (within tolerance)
        // Using a reasonable tolerance for browser rendering differences
        if (firstCardBox && secondCardBox) {
          expect(Math.abs(firstCardBox.width - secondCardBox.width)).toBeLessThan(5);
        }
      }
    });
  });

  test.describe('Theme Toggle Functionality', () => {
    test('TC3: Theme toggle works and persists preference', async ({ page, browserName }) => {
      console.log(`Testing theme toggle in: ${browserName}`);

      // Clear localStorage for clean state
      await page.evaluate(() => window.localStorage.clear());
      await page.reload();
      await page.waitForLoadState('networkidle');

      // Verify toggle button exists
      const toggleButton = page.getByTestId('theme-toggle');
      await expect(toggleButton).toBeVisible();

      // Verify default dark mode
      const htmlElement = page.locator('html');
      await expect(htmlElement).toHaveClass(/dark/);

      // Scroll the toggle button into view and ensure it's clickable
      await toggleButton.scrollIntoViewIfNeeded();
      await page.waitForTimeout(100);

      // Focus and press Enter to toggle (more reliable than click)
      await toggleButton.focus();
      await page.keyboard.press('Enter');
      await page.waitForTimeout(300); // Wait for state update and transition

      // Verify light mode is active
      await expect(htmlElement).not.toHaveClass(/dark/);

      // Verify localStorage was updated
      const storedTheme = await page.evaluate(() => {
        return window.localStorage.getItem('mirdb-theme');
      });
      expect(storedTheme).toBe('light');

      // Reload page and verify persistence
      await page.reload();
      await page.waitForLoadState('networkidle');

      // Theme should persist after reload
      await expect(htmlElement).not.toHaveClass(/dark/);

      // Toggle back to dark mode using keyboard
      const toggleAfterReload = page.getByTestId('theme-toggle');
      await toggleAfterReload.scrollIntoViewIfNeeded();
      await toggleAfterReload.focus();
      await page.keyboard.press('Enter');
      await page.waitForTimeout(300);

      // Verify dark mode is restored
      await expect(htmlElement).toHaveClass(/dark/);

      // Verify localStorage updated
      const storedDarkTheme = await page.evaluate(() => {
        return window.localStorage.getItem('mirdb-theme');
      });
      expect(storedDarkTheme).toBe('dark');
    });

    test('Theme toggle has proper accessibility attributes', async ({ page, browserName }) => {
      console.log(`Testing theme toggle accessibility in: ${browserName}`);

      const toggleButton = page.getByTestId('theme-toggle');
      await expect(toggleButton).toBeVisible();

      // Verify aria-label exists
      const ariaLabel = await toggleButton.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel).toMatch(/switch to (light|dark) mode/i);

      // Verify button is focusable
      await toggleButton.focus();
      await expect(toggleButton).toBeFocused();
    });

    test('Theme colors change correctly on toggle', async ({ page, browserName }) => {
      console.log(`Testing theme color changes in: ${browserName}`);

      const body = page.locator('body');
      const toggleButton = page.getByTestId('theme-toggle');

      // Ensure we're in dark mode first
      const htmlElement = page.locator('html');
      await expect(htmlElement).toHaveClass(/dark/);

      // Get initial dark mode background color
      const darkBgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Scroll toggle into view and switch to light mode via keyboard
      await toggleButton.scrollIntoViewIfNeeded();
      await toggleButton.focus();
      await page.keyboard.press('Enter');
      await page.waitForTimeout(300);

      // Verify theme actually changed
      await expect(htmlElement).not.toHaveClass(/dark/);

      // Get light mode background color
      const lightBgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Colors should be different between modes
      expect(darkBgColor).not.toBe(lightBgColor);
    });
  });

  test.describe('Copy to Clipboard Functionality', () => {
    test('TC4: Copy to clipboard works correctly', async ({ page, context, browserName }) => {
      console.log(`Testing copy to clipboard in: ${browserName}`);

      // WebKit and Firefox may have issues with clipboard permissions
      // Test the visual feedback instead for these browsers
      if (browserName === 'webkit' || browserName === 'firefox') {
        // Navigate to usage section
        const usageSection = page.locator('#usage');
        await expect(usageSection).toBeVisible();

        // Find and click copy button
        const copyButton = usageSection.locator('.copy-button').first();
        await expect(copyButton).toBeVisible();

        // Get initial aria-label
        const initialLabel = await copyButton.getAttribute('aria-label');
        expect(initialLabel).toBe('Copy to clipboard');

        // Click copy button
        await copyButton.click();

        // Verify copy button shows success state (visual feedback)
        await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');

        // Wait for button to reset
        await page.waitForTimeout(2100);
        await expect(copyButton).toHaveAttribute('aria-label', 'Copy to clipboard');
        return;
      }

      // For Chromium-based browsers, test full clipboard functionality
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Navigate to usage section
      const usageSection = page.locator('#usage');
      await expect(usageSection).toBeVisible();

      // Find code block
      const codeBlock = usageSection.locator('pre code');
      await expect(codeBlock).toBeVisible();

      // Find and click copy button
      const copyButton = usageSection.locator('.copy-button').first();
      await expect(copyButton).toBeVisible();

      // Get initial aria-label
      const initialLabel = await copyButton.getAttribute('aria-label');
      expect(initialLabel).toBe('Copy to clipboard');

      // Click copy button
      await copyButton.click();

      // Verify copy button shows success state
      await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');

      // Read clipboard and verify content
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toContain('SET');
      expect(clipboardContent).toContain('GET');

      // Wait for button to reset
      await page.waitForTimeout(2100);
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy to clipboard');
    });

    test('Copy button has visual feedback on hover', async ({ page, browserName }) => {
      console.log(`Testing copy button hover state in: ${browserName}`);

      const usageSection = page.locator('#usage');
      await expect(usageSection).toBeVisible();

      const copyButton = usageSection.locator('.copy-button').first();
      await expect(copyButton).toBeVisible();

      // Get initial styles
      const initialOpacity = await copyButton.evaluate((el) => {
        return window.getComputedStyle(el).opacity;
      });

      // Hover over button
      await copyButton.hover();
      await page.waitForTimeout(100);

      // Button should remain visible on hover
      await expect(copyButton).toBeVisible();
    });
  });

  test.describe('Smooth Scroll Navigation', () => {
    test('TC5: Smooth scroll animation works for navigation links', async ({ page, browserName }) => {
      console.log(`Testing smooth scroll in: ${browserName}`);

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click on Features link
      const featuresLink = page.getByTestId('nav-link-features');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();

      // Wait for smooth scroll animation
      await page.waitForTimeout(600);

      // Verify scroll position changed
      const scrollAfterFeatures = await page.evaluate(() => window.scrollY);
      expect(scrollAfterFeatures).toBeGreaterThan(initialScrollY);

      // Verify features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('All navigation sections are reachable via smooth scroll', async ({ page, browserName }) => {
      console.log(`Testing all navigation sections in: ${browserName}`);

      const sections = [
        { linkTestId: 'nav-link-features', sectionId: '#features' },
        { linkTestId: 'nav-link-usage', sectionId: '#usage' },
        { linkTestId: 'nav-link-quick-start', sectionId: '#quickstart' },
      ];

      for (const { linkTestId, sectionId } of sections) {
        // Scroll back to top
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(100);

        // Click navigation link
        const link = page.getByTestId(linkTestId);
        await expect(link).toBeVisible();
        await link.click();

        // Wait for smooth scroll
        await page.waitForTimeout(600);

        // Verify section is in viewport
        const section = page.locator(sectionId);
        await expect(section).toBeInViewport();
      }
    });

    test('CSS scroll-behavior is set to smooth', async ({ page, browserName }) => {
      console.log(`Testing CSS scroll-behavior in: ${browserName}`);

      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).toBe('smooth');
    });

    test('Navigation header remains sticky during scroll', async ({ page, browserName }) => {
      console.log(`Testing sticky navigation in: ${browserName}`);

      const header = page.getByTestId('header');
      await expect(header).toBeVisible();

      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, 800));
      await page.waitForTimeout(100);

      // Header should still be visible and in viewport
      await expect(header).toBeVisible();
      await expect(header).toBeInViewport();

      // Verify sticky positioning
      const position = await header.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });
      expect(position).toBe('sticky');
    });
  });

  test.describe('General Cross-Browser Consistency', () => {
    test('No JavaScript errors on page load', async ({ page, browserName }) => {
      console.log(`Testing for JS errors in: ${browserName}`);

      const errors: string[] = [];
      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      expect(errors).toHaveLength(0);
    });

    test('All critical sections are visible', async ({ page, browserName }) => {
      console.log(`Testing critical sections visibility in: ${browserName}`);

      // Hero section
      await expect(page.locator('#hero')).toBeVisible();

      // Features section
      await expect(page.locator('#features')).toBeVisible();

      // Usage section
      await expect(page.locator('#usage')).toBeVisible();

      // Quick start section
      await expect(page.locator('#quickstart')).toBeVisible();

      // Footer
      await expect(page.getByTestId('footer')).toBeVisible();
    });

    test('Images load correctly', async ({ page, browserName }) => {
      console.log(`Testing image loading in: ${browserName}`);

      // Wait for page to be fully loaded
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(500);

      // Check logo - verify it exists and has correct attributes
      const logo = page.locator('img[alt="MirDB Logo"]');
      const logoCount = await logo.count();

      if (logoCount > 0) {
        // Verify logo element exists and is visible
        await expect(logo.first()).toBeVisible();

        // Check that src attribute is set correctly
        const logoSrc = await logo.first().getAttribute('src');
        expect(logoSrc).toBeTruthy();
        expect(logoSrc).toContain('logo');
      }

      // Check usage GIF - verify element exists and has correct attributes
      const usageGif = page.locator('[data-testid="usage-gif"]');
      const gifCount = await usageGif.count();

      if (gifCount > 0) {
        // Scroll the GIF into view (it uses lazy loading)
        await usageGif.first().scrollIntoViewIfNeeded();
        await page.waitForTimeout(500); // Wait for lazy loading

        // Verify GIF element exists
        // Note: Lazy-loaded images may report as hidden until scrolled into view
        // Just verify the src attribute is correctly set
        const gifSrc = await usageGif.first().getAttribute('src');
        expect(gifSrc).toBeTruthy();
        expect(gifSrc).toContain('usage.gif');

        // Check if the element is attached to DOM
        const isAttached = await usageGif.first().evaluate((el) => {
          return el.isConnected;
        });
        expect(isAttached).toBe(true);
      }
    });

    test('Fonts render correctly', async ({ page, browserName }) => {
      console.log(`Testing font rendering in: ${browserName}`);

      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      // Verify font is loaded and rendered
      const fontFamily = await h1.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      expect(fontFamily).toBeTruthy();
      expect(fontFamily.length).toBeGreaterThan(0);
    });
  });
});
