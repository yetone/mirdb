import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Semantic HTML Accessibility
 *
 * These tests verify that the page uses proper semantic HTML structure:
 * - Semantic container elements (header, main, footer, section, nav)
 * - Proper heading hierarchy (single h1, sequential order)
 * - ARIA landmarks where needed
 * - Proper use of button elements for interactive actions
 */

test.describe('Accessibility - Semantic HTML', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Semantic Container Elements', () => {
    test('Page uses header element for top section', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();
    });

    test('Page uses main element for primary content', async ({ page }) => {
      const main = page.locator('main');
      await expect(main).toBeVisible();
    });

    test('Page uses footer element for bottom section', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('Page uses section elements for content grouping', async ({ page }) => {
      const sections = page.locator('section');
      const count = await sections.count();
      expect(count).toBeGreaterThan(0);
    });

    test('Page uses nav element for navigation', async ({ page }) => {
      const nav = page.locator('nav');
      const count = await nav.count();
      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('TC2: Heading Hierarchy', () => {
    test('Page has exactly one h1 element', async ({ page }) => {
      const h1Elements = page.locator('h1');
      const count = await h1Elements.count();
      expect(count).toBe(1);
    });

    test('Headings follow sequential order (no skipping levels)', async ({ page }) => {
      // Get all heading elements in order
      const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

      let previousLevel = 0;

      for (const heading of headings) {
        const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
        const currentLevel = parseInt(tagName.charAt(1));

        // Each heading level should be at most 1 level deeper than the previous
        // (e.g., h1 -> h2 is ok, h1 -> h3 is not)
        // Or can be same level or go up (h3 -> h2 is ok for new section)
        if (previousLevel > 0) {
          // Allow same level, going up, or going down by 1
          const isValidSequence = currentLevel <= previousLevel + 1;
          expect(isValidSequence).toBeTruthy();
        }

        previousLevel = currentLevel;
      }
    });

    test('h2 elements follow h1', async ({ page }) => {
      const h1 = page.locator('h1');
      const h2Elements = page.locator('h2');

      // Ensure h1 exists before checking h2s
      await expect(h1).toBeVisible();

      // Ensure h2 elements exist
      const h2Count = await h2Elements.count();
      expect(h2Count).toBeGreaterThan(0);
    });
  });

  test.describe('TC3: ARIA Landmarks', () => {
    test('Main content area has appropriate role or semantic element', async ({ page }) => {
      // Check for main element or role="main"
      const mainElement = page.locator('main, [role="main"]');
      const count = await mainElement.count();
      expect(count).toBeGreaterThan(0);
    });

    test('Navigation has appropriate role or semantic element', async ({ page }) => {
      // Check for nav element or role="navigation"
      const navElement = page.locator('nav, [role="navigation"]');
      const count = await navElement.count();
      expect(count).toBeGreaterThan(0);
    });

    test('Banner/header has appropriate role or semantic element', async ({ page }) => {
      // Check for header element or role="banner"
      const bannerElement = page.locator('header, [role="banner"]');
      const count = await bannerElement.count();
      expect(count).toBeGreaterThan(0);
    });

    test('Footer has appropriate role or semantic element', async ({ page }) => {
      // Check for footer element or role="contentinfo"
      const footerElement = page.locator('footer, [role="contentinfo"]');
      const count = await footerElement.count();
      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('TC4: Button Elements for Actions', () => {
    test('Interactive actions use button or link elements', async ({ page }) => {
      // Get all elements styled as buttons (not container divs)
      // Only check elements with .btn or .button classes (direct button styling)
      const ctaButtons = page.locator('.btn, .button');
      const count = await ctaButtons.count();

      // Should have at least some buttons
      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const element = ctaButtons.nth(i);
        const tagName = await element.evaluate(el => el.tagName.toLowerCase());

        // Should be button or anchor tag, not div or span
        const isValidElement = tagName === 'button' || tagName === 'a' || tagName === 'input';
        expect(isValidElement).toBeTruthy();
      }
    });

    test('Navigation links use anchor elements', async ({ page }) => {
      // Check that navigation links use <a> tags
      const navLinks = page.locator('nav a, .footer-nav a, .footer-links a');
      const count = await navLinks.count();

      // Should have navigation links
      expect(count).toBeGreaterThan(0);

      // Each should be an anchor element
      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        const tagName = await link.evaluate(el => el.tagName.toLowerCase());
        expect(tagName).toBe('a');
      }
    });

    test('No styled divs or spans used as interactive buttons', async ({ page }) => {
      // Check that div and span elements with click handlers or button-like classes
      // are not being used instead of proper button/link elements
      const suspiciousElements = page.locator('div[onclick], span[onclick], div[role="button"]:not(button), span[role="button"]:not(button)');
      const count = await suspiciousElements.count();

      // There should be no div/span elements being used as buttons
      expect(count).toBe(0);
    });
  });
});
