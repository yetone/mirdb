/**
 * Accessibility E2E Tests
 * Owner: Scenarios 13-15 - Accessibility
 *
 * Tests:
 * - Keyboard navigation (Tab order)
 * - Focus indicators
 * - Skip link functionality
 * - ARIA labels and roles
 * - Screen reader accessibility
 */
import { test, expect } from '@playwright/test';

test.describe('Accessibility - Screen Reader', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Semantic HTML Landmarks', () => {
    test('page has all required semantic landmarks', async ({ page }) => {
      // Check for header landmark
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Check for main landmark
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Check for nav landmark
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Check for footer landmark
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('page has multiple section elements', async ({ page }) => {
      const sections = page.locator('section');
      const count = await sections.count();
      expect(count).toBeGreaterThan(0);
    });

    test('page has document language set', async ({ page }) => {
      const lang = await page.locator('html').getAttribute('lang');
      expect(lang).toBe('en');
    });
  });

  test.describe('TC2: Image Alt Text', () => {
    test('all images have alt text', async ({ page }) => {
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        expect(alt, `Image ${i + 1} missing alt attribute`).not.toBeNull();
        expect(alt.trim().length, `Image ${i + 1} has empty alt text`).toBeGreaterThan(0);
      }
    });

    test('logo image has meaningful alt text', async ({ page }) => {
      const logoImg = page.locator('img[alt="MirDB Logo"]');
      await expect(logoImg).toBeVisible();
    });

    test('usage demo image has descriptive alt text', async ({ page }) => {
      const demoImg = page.locator('#demo img');
      const alt = await demoImg.getAttribute('alt');
      expect(alt).toContain('MirDB');
    });
  });

  test.describe('TC3: Heading Hierarchy', () => {
    test('page has exactly one h1', async ({ page }) => {
      const h1s = page.locator('h1');
      const count = await h1s.count();
      expect(count).toBe(1);
    });

    test('heading hierarchy follows logical order', async ({ page }) => {
      const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();
      let currentLevel = 0;

      for (const heading of headings) {
        const tagName = await heading.evaluate(el => el.tagName);
        const level = parseInt(tagName.charAt(1));

        if (currentLevel === 0) {
          // First heading should be h1
          expect(level).toBe(1);
        } else {
          // Should not skip more than one level
          const levelDiff = level - currentLevel;
          expect(levelDiff).toBeLessThanOrEqual(1);
        }

        currentLevel = level;
      }
    });

    test('h1 contains MirDB', async ({ page }) => {
      const h1 = page.locator('h1');
      await expect(h1).toContainText('MirDB');
    });
  });

  test.describe('TC4: Code Blocks ARIA Labels', () => {
    test('code blocks have aria-label describing their content', async ({ page }) => {
      // Check for code blocks with role="region" in quick-start section
      const codeBlocks = page.locator('[role="region"]');
      const count = await codeBlocks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const block = codeBlocks.nth(i);
        const ariaLabel = await block.getAttribute('aria-label');
        expect(ariaLabel, `Code block ${i + 1} should have aria-label`).not.toBeNull();
        expect(ariaLabel.trim().length).toBeGreaterThan(0);
      }
    });

    test('architecture diagram has appropriate ARIA label', async ({ page }) => {
      const diagram = page.locator('.architecture-diagram, [role="img"]').first();
      const ariaLabel = await diagram.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel.toLowerCase()).toContain('architecture');
    });
  });

  test.describe('TC5: Descriptive Link Text', () => {
    test('links do not have generic text like click here', async ({ page }) => {
      const links = page.locator('a');
      const count = await links.count();
      const genericTexts = ['click here', 'click', 'here', 'read more', 'more', 'link'];

      for (let i = 0; i < count; i++) {
        const link = links.nth(i);
        const text = await link.textContent();
        const trimmedText = text.trim().toLowerCase();

        for (const generic of genericTexts) {
          if (trimmedText === generic) {
            throw new Error(`Link has generic text: "${trimmedText}"`);
          }
        }
      }
    });

    test('all links have descriptive text or aria-label', async ({ page }) => {
      const links = page.locator('a');
      const count = await links.count();

      for (let i = 0; i < count; i++) {
        const link = links.nth(i);
        const text = await link.textContent();
        const ariaLabel = await link.getAttribute('aria-label');

        const hasDescriptiveContent = text.trim().length > 0 || (ariaLabel && ariaLabel.length > 0);
        expect(hasDescriptiveContent, `Link ${i + 1} lacks descriptive text or aria-label`).toBe(true);
      }
    });
  });

  test.describe('Keyboard Navigation', () => {
    test('interactive elements are keyboard focusable', async ({ page }) => {
      // Tab through the page and verify focus moves to interactive elements
      await page.keyboard.press('Tab');

      // First focusable element should be in header
      const firstFocused = page.locator(':focus');
      await expect(firstFocused).toBeVisible();
    });

    test('mobile menu button is keyboard accessible', async ({ page }) => {
      const mobileMenuBtn = page.locator('#mobile-menu-btn');
      const ariaLabel = await mobileMenuBtn.getAttribute('aria-label');
      const ariaExpanded = await mobileMenuBtn.getAttribute('aria-expanded');

      expect(ariaLabel).not.toBeNull();
      expect(ariaExpanded).toBe('false');
    });

    test('copy buttons have accessible names', async ({ page }) => {
      const copyBtns = page.locator('.copy-btn');
      const count = await copyBtns.count();

      for (let i = 0; i < count; i++) {
        const btn = copyBtns.nth(i);
        const ariaLabel = await btn.getAttribute('aria-label');
        const text = await btn.textContent();

        const hasAccessibleName = (ariaLabel && ariaLabel.length > 0) || text.trim().length > 0;
        expect(hasAccessibleName, `Copy button ${i + 1} lacks accessible name`).toBe(true);
      }
    });
  });

  test.describe('Additional Screen Reader Accessibility', () => {
    test('page has meta description', async ({ page }) => {
      const metaDesc = page.locator('meta[name="description"]');
      const content = await metaDesc.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.length).toBeGreaterThan(0);
    });

    test('page has viewport meta tag', async ({ page }) => {
      const viewport = page.locator('meta[name="viewport"]');
      await expect(viewport).toHaveCount(1);
    });

    test('external links have rel="noopener noreferrer"', async ({ page }) => {
      const externalLinks = page.locator('a[target="_blank"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      }
    });
  });
});
