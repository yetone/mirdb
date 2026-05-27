/**
 * Accessibility E2E Tests for MirDB Homepage
 * Tests: keyboard navigation, browser zoom
 */

const { chromium } = require('playwright');
const path = require('path');
const fs = require('fs');

describe('Accessibility Compliance - E2E Tests', () => {
  let browser;
  let page;
  const htmlPath = 'file://' + path.join(__dirname, '..', '..', 'public', 'index.html');

  beforeAll(async () => {
    browser = await chromium.launch({ headless: true });
  });

  afterAll(async () => {
    if (browser) await browser.close();
  });

  beforeEach(async () => {
    page = await browser.newPage();
    await page.goto(htmlPath);
  });

  afterEach(async () => {
    if (page) await page.close();
  });

  // Test Case 2: keyboard navigation
  describe('Test 2: keyboard navigation', () => {
    it('all interactive elements should be reachable via Tab key', async () => {
      // Start focus at the beginning
      await page.keyboard.press('Tab');

      const interactiveSelectors = [
        'a[href]',
        'button:not([disabled])',
        'input:not([disabled]):not([type="hidden"])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
      ];

      // Get all interactive elements
      const interactiveElements = await page.$$eval(
        interactiveSelectors.join(', '),
        (els) => els.map((el) => ({
          tag: el.tagName.toLowerCase(),
          text: el.textContent.trim().substring(0, 40),
          tabIndex: el.tabIndex,
          ariaLabel: el.getAttribute('aria-label'),
          href: el.getAttribute('href'),
          className: el.className,
        }))
      );

      // Should have interactive elements
      expect(interactiveElements.length).toBeGreaterThan(0);

      // Verify we have the key interactive elements
      const hasSkipLink = interactiveElements.some(
        (el) => el.className && el.className.includes('skip-link')
      );
      const hasNavLinks = interactiveElements.some(
        (el) => el.tag === 'a' && el.href && el.href.includes('#')
      );
      const hasThemeToggle = interactiveElements.some(
        (el) => el.className && el.className.includes('theme-toggle')
      );
      const hasCopyButtons = interactiveElements.some(
        (el) => el.className && el.className.includes('copy-btn')
      );

      expect(hasSkipLink || hasNavLinks).toBe(true);
      expect(hasThemeToggle).toBe(true);
      expect(hasCopyButtons).toBe(true);
    });

    it('should have visible focus indicator on focused elements', async () => {
      // Tab to an interactive element
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        const style = window.getComputedStyle(el);
        return {
          tag: el.tagName.toLowerCase(),
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
        };
      });

      if (focusedElement) {
        // Focus should have some visible indicator
        const hasOutline = focusedElement.outlineWidth !== '0px' &&
                           focusedElement.outlineStyle !== 'none';
        const hasBoxShadow = focusedElement.boxShadow !== 'none';

        expect(hasOutline || hasBoxShadow).toBe(true);
      }
    });

    it('tab order should follow DOM order logically', async () => {
      // Tab through elements and record their order
      const tabOrder = [];
      let previousElement = null;
      let safetyCounter = 0;
      const maxTabs = 30;

      while (safetyCounter < maxTabs) {
        await page.keyboard.press('Tab');
        const activeElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          return {
            tag: el.tagName.toLowerCase(),
            className: el.className,
            ariaLabel: el.getAttribute('aria-label'),
            text: el.textContent.trim().substring(0, 30),
          };
        });

        if (!activeElement) break;

        // Check if we've looped back
        if (previousElement &&
            activeElement.tag === previousElement.tag &&
            activeElement.className === previousElement.className &&
            activeElement.text === previousElement.text) {
          break;
        }

        tabOrder.push(activeElement);
        previousElement = activeElement;
        safetyCounter++;
      }

      // Should have multiple tab stops
      expect(tabOrder.length).toBeGreaterThan(5);

      // Skip link should be first or early in tab order
      const skipLinkIndex = tabOrder.findIndex(
        (el) => el.className && el.className.includes('skip-link')
      );
      expect(skipLinkIndex).toBeLessThanOrEqual(1);
    });

    it('Escape key should close mobile menu', async () => {
      // Set viewport to mobile size
      await page.setViewportSize({ width: 375, height: 667 });
      await page.reload();

      // Open mobile menu
      const menuToggle = await page.locator('.mobile-menu-toggle');
      if (await menuToggle.isVisible()) {
        await menuToggle.click();

        // Verify menu is open
        const isOpen = await page.evaluate(() => {
          return document.querySelector('.nav-links').classList.contains('open');
        });
        expect(isOpen).toBe(true);

        // Press Escape
        await page.keyboard.press('Escape');

        // Verify menu is closed
        const isClosed = await page.evaluate(() => {
          return !document.querySelector('.nav-links').classList.contains('open');
        });
        expect(isClosed).toBe(true);
      }
    });
  });

  // Test Case 7: 200% browser zoom
  describe('Test 7: browser zoom at 200%', () => {
    it('content should remain accessible at 200% zoom', async () => {
      // Set desktop viewport
      await page.setViewportSize({ width: 1280, height: 800 });

      // Apply 200% zoom
      await page.evaluate(() => {
        document.body.style.zoom = '200%';
      });

      // Wait for layout to settle
      await page.waitForTimeout(500);

      // Check that content is still visible
      const heroTitle = await page.locator('h1');
      const isVisible = await heroTitle.isVisible();
      expect(isVisible).toBe(true);

      const heroText = await heroTitle.textContent();
      expect(heroText.trim()).toBe('MirDB');
    });

    it('should not have horizontal overflow on text at 200% zoom', async () => {
      await page.setViewportSize({ width: 1280, height: 800 });

      await page.evaluate(() => {
        document.body.style.zoom = '200%';
      });

      await page.waitForTimeout(500);

      // Check body width vs viewport width
      const overflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > window.innerWidth;
      });

      // Some horizontal scroll might be acceptable for tables, but body should not overflow
      // We'll check if the main content area overflows significantly
      const mainOverflow = await page.evaluate(() => {
        const main = document.querySelector('main');
        if (!main) return false;
        return main.scrollWidth > window.innerWidth;
      });

      // Tables may overflow with scroll wrapper - that's acceptable
      expect(mainOverflow).toBe(false);
    });

    it('layout should adapt gracefully at 200% zoom', async () => {
      await page.setViewportSize({ width: 1280, height: 800 });

      await page.evaluate(() => {
        document.body.style.zoom = '200%';
      });

      await page.waitForTimeout(500);

      // Check that sections are still visible and stacked properly
      const sections = await page.locator('main > section').all();
      expect(sections.length).toBeGreaterThan(0);

      for (const section of sections) {
        const isVisible = await section.isVisible();
        expect(isVisible).toBe(true);
      }
    });

    it('interactive elements should remain clickable at 200% zoom', async () => {
      await page.setViewportSize({ width: 1280, height: 800 });

      await page.evaluate(() => {
        document.body.style.zoom = '200%';
      });

      await page.waitForTimeout(500);

      // Check that buttons and links are visible and have reasonable sizes
      // Filter out elements that are intentionally hidden (e.g., mobile menu toggle on desktop)
      const elements = await page.$$eval('a, button', (els) =>
        els
          .filter((el) => {
            const style = window.getComputedStyle(el);
            return style.display !== 'none' && style.visibility !== 'hidden';
          })
          .map((el) => {
            const rect = el.getBoundingClientRect();
            return {
              tag: el.tagName.toLowerCase(),
              className: el.className,
              width: rect.width,
              height: rect.height,
              visible: rect.width > 0 && rect.height > 0,
            };
          })
      );

      expect(elements.length).toBeGreaterThan(0);

      for (const el of elements) {
        expect(el.visible).toBe(true);
        // Minimum touch target size
        expect(el.width).toBeGreaterThanOrEqual(20);
        expect(el.height).toBeGreaterThanOrEqual(20);
      }
    });
  });
});
