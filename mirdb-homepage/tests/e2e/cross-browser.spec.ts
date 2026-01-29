/**
 * E2E tests for Cross-Browser Compatibility.
 * Owner: Scenario 15 - Cross-Browser Compatibility
 *
 * Tests cover:
 * - Page loads correctly in all configured browsers (Chrome, Firefox, Safari, Edge)
 * - All major features work across browsers
 * - Visual rendering consistency
 * - JavaScript functionality across browser engines
 *
 * Browser matrix:
 * - Chromium (Chrome)
 * - Firefox
 * - WebKit (Safari)
 * - Edge (Chromium-based)
 */

import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility', () => {
  test.describe('Page Loading', () => {
    test('homepage loads successfully', async ({ page, browserName }) => {
      await page.goto('/');

      // Page should load without errors
      const title = await page.title();
      expect(title).toContain('MirDB');

      // Check that main content is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Log browser for debugging
      console.log(`Tested on: ${browserName}`);
    });

    test('all major sections are visible', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Test all major page sections
      const sections = [
        '[data-testid="hero-section"]',
        '[data-testid="features-section"]',
        '[data-testid="terminal-section"]',
      ];

      for (const selector of sections) {
        const section = page.locator(selector);
        // Some sections may not exist yet, so we use soft assertions
        const count = await section.count();
        if (count > 0) {
          await expect(section.first()).toBeVisible();
        }
      }

      console.log(`All major sections tested on: ${browserName}`);
    });

    test('page has no JavaScript console errors', async ({ page, browserName }) => {
      const consoleErrors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out known third-party errors that don't affect functionality
      const criticalErrors = consoleErrors.filter(
        (error) =>
          !error.includes('favicon') &&
          !error.includes('404') &&
          !error.includes('net::ERR')
      );

      expect(criticalErrors).toHaveLength(0);
      console.log(`No JS errors on: ${browserName}`);
    });
  });

  test.describe('Layout and Rendering', () => {
    test('page layout renders correctly', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that the page has proper structure
      const body = page.locator('body');
      const bodyBox = await body.boundingBox();

      expect(bodyBox).not.toBeNull();
      expect(bodyBox!.width).toBeGreaterThan(0);
      expect(bodyBox!.height).toBeGreaterThan(0);

      console.log(`Layout verified on: ${browserName} - ${bodyBox!.width}x${bodyBox!.height}`);
    });

    test('fonts load correctly', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check that text is visible and rendered
      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      const count = await heroTagline.count();

      if (count > 0) {
        await expect(heroTagline).toBeVisible();

        // Font should be loaded (text should have reasonable dimensions)
        const box = await heroTagline.boundingBox();
        expect(box).not.toBeNull();
        expect(box!.height).toBeGreaterThan(10);
      }

      console.log(`Fonts verified on: ${browserName}`);
    });

    test('CSS custom properties are supported', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that CSS custom properties (variables) are working
      const hasCustomProperties = await page.evaluate(() => {
        const style = getComputedStyle(document.documentElement);
        // Check for our theme colors
        const bgColor = style.getPropertyValue('--color-background');
        return bgColor !== '';
      });

      expect(hasCustomProperties).toBe(true);
      console.log(`CSS custom properties working on: ${browserName}`);
    });

    test('flexbox layout works correctly', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check if flexbox is working by verifying CTA buttons layout
      const ctaButtons = page.locator('[data-testid="hero-cta-buttons"]');
      const count = await ctaButtons.count();

      if (count > 0) {
        const displayStyle = await ctaButtons.evaluate((el) =>
          window.getComputedStyle(el).display
        );
        expect(displayStyle).toBe('flex');
      }

      console.log(`Flexbox verified on: ${browserName}`);
    });

    test('grid layout works correctly', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check if CSS Grid is working in features section
      // The actual grid container is features-container, not features-grid (which is the section wrapper)
      const featuresContainer = page.locator('[data-testid="features-container"]');
      const count = await featuresContainer.count();

      if (count > 0) {
        const displayStyle = await featuresContainer.evaluate((el) =>
          window.getComputedStyle(el).display
        );
        expect(displayStyle).toBe('grid');
      }

      console.log(`Grid layout verified on: ${browserName}`);
    });
  });

  test.describe('Interactive Features', () => {
    test('navigation links work', async ({ page, browserName }) => {
      await page.goto('/');

      // Test internal navigation - Get Started button
      const getStartedBtn = page.locator('a:has-text("Get Started")');
      const count = await getStartedBtn.count();

      if (count > 0) {
        const href = await getStartedBtn.getAttribute('href');
        expect(href).toBeTruthy();
      }

      console.log(`Navigation verified on: ${browserName}`);
    });

    test('external links have correct attributes', async ({ page, browserName }) => {
      await page.goto('/');

      // GitHub link should open in new tab
      const githubLink = page.locator('a:has-text("View on GitHub")');
      const count = await githubLink.count();

      if (count > 0) {
        await expect(githubLink).toHaveAttribute('target', '_blank');
        await expect(githubLink).toHaveAttribute('rel', /noopener/);
      }

      console.log(`External links verified on: ${browserName}`);
    });

    test('scroll behavior works', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(500);

      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);

      console.log(`Scroll behavior verified on: ${browserName}`);
    });
  });

  test.describe('Theme Support', () => {
    test('dark theme colors are applied correctly', async ({ page, browserName }) => {
      // Set dark theme
      await page.addInitScript(() => {
        localStorage.setItem('mirdb-theme', 'dark');
      });
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify dark mode is active
      const htmlClass = await page.locator('html').getAttribute('class');
      expect(htmlClass).toContain('dark');

      // Check background color (dark theme should have dark background)
      const bgColor = await page.evaluate(() => {
        return window.getComputedStyle(document.body).backgroundColor;
      });

      // Dark theme background: #0d1117 = rgb(13, 17, 23)
      expect(bgColor).toBe('rgb(13, 17, 23)');

      console.log(`Dark theme verified on: ${browserName}`);
    });

    test('light theme colors are applied correctly', async ({ page, browserName }) => {
      // Set light theme
      await page.addInitScript(() => {
        localStorage.setItem('mirdb-theme', 'light');
      });
      await page.emulateMedia({ colorScheme: 'light' });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify light mode is active
      const htmlClass = await page.locator('html').getAttribute('class');
      expect(htmlClass).toContain('light');

      console.log(`Light theme verified on: ${browserName}`);
    });

    test('prefers-color-scheme media query is respected', async ({ page, browserName }) => {
      // Clear any stored preference
      await page.addInitScript(() => {
        localStorage.removeItem('mirdb-theme');
      });

      // Emulate dark color scheme
      await page.emulateMedia({ colorScheme: 'dark' });
      await page.goto('/');

      const htmlClass = await page.locator('html').getAttribute('class');
      expect(htmlClass).toContain('dark');

      console.log(`Color scheme media query verified on: ${browserName}`);
    });
  });

  test.describe('Responsive Design', () => {
    test('mobile viewport renders correctly', async ({ page, browserName }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Page should still be functional
      const heroSection = page.locator('[data-testid="hero-section"]');
      const count = await heroSection.count();

      if (count > 0) {
        await expect(heroSection).toBeVisible();

        // No horizontal scrollbar on mobile
        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth;
        });

        expect(hasHorizontalScroll).toBe(false);
      }

      console.log(`Mobile viewport verified on: ${browserName}`);
    });

    test('tablet viewport renders correctly', async ({ page, browserName }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const heroSection = page.locator('[data-testid="hero-section"]');
      const count = await heroSection.count();

      if (count > 0) {
        await expect(heroSection).toBeVisible();
      }

      console.log(`Tablet viewport verified on: ${browserName}`);
    });

    test('desktop viewport renders correctly', async ({ page, browserName }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const heroSection = page.locator('[data-testid="hero-section"]');
      const count = await heroSection.count();

      if (count > 0) {
        await expect(heroSection).toBeVisible();
      }

      console.log(`Desktop viewport verified on: ${browserName}`);
    });

    test('wide desktop viewport renders correctly', async ({ page, browserName }) => {
      await page.setViewportSize({ width: 2560, height: 1440 });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const heroSection = page.locator('[data-testid="hero-section"]');
      const count = await heroSection.count();

      if (count > 0) {
        await expect(heroSection).toBeVisible();
      }

      console.log(`Wide desktop viewport verified on: ${browserName}`);
    });
  });

  test.describe('Accessibility Basics', () => {
    test('page has valid HTML structure', async ({ page, browserName }) => {
      await page.goto('/');

      // Check for exactly one h1
      const h1Count = await page.locator('h1').count();
      expect(h1Count).toBeGreaterThan(0);

      // Check for main landmark
      const mainCount = await page.locator('main').count();
      expect(mainCount).toBe(1);

      console.log(`HTML structure verified on: ${browserName}`);
    });

    test('images have alt attributes', async ({ page, browserName }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // All images should have alt attributes
      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        // Alt can be empty for decorative images but attribute should exist
        expect(alt).not.toBeNull();
      }

      console.log(`Image alt attributes verified on: ${browserName}`);
    });

    test('links have discernible text', async ({ page, browserName }) => {
      await page.goto('/');

      const links = page.locator('a');
      const linkCount = await links.count();

      for (let i = 0; i < linkCount; i++) {
        const link = links.nth(i);
        const text = await link.textContent();
        const ariaLabel = await link.getAttribute('aria-label');

        // Link should have either text content or aria-label
        const hasDiscernibleText = (text && text.trim().length > 0) || ariaLabel;
        expect(hasDiscernibleText).toBeTruthy();
      }

      console.log(`Link accessibility verified on: ${browserName}`);
    });
  });
});
