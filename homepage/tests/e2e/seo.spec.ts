/**
 * E2E tests for SEO Optimization.
 * Owner: Scenario 14 - SEO Optimization
 *
 * Requirements: NFR-5
 *
 * Test cases:
 * 1. Page has title tag with 'MirDB'
 * 2. Page has meta description tag
 * 3. Page has Open Graph meta tags (og:title, og:description, og:image)
 * 4. Page has canonical URL meta tag
 * 5. Page has exactly one h1 element
 * 6. Headings follow proper hierarchy without skipping levels
 */

import { test, expect } from '@playwright/test';

test.describe('SEO Optimization E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Test Case 1: Page has title tag with MirDB', () => {
    test('title tag contains MirDB', async ({ page }) => {
      const title = await page.title();
      expect(title).toContain('MirDB');
    });

    test('title tag is properly formatted', async ({ page }) => {
      const title = await page.title();
      expect(title).toBe('MirDB - Persistent Key-Value Store');
    });
  });

  test.describe('Test Case 2: Page has meta description tag', () => {
    test('meta description tag exists', async ({ page }) => {
      const metaDescription = page.locator('meta[name="description"]');
      await expect(metaDescription).toHaveCount(1);
    });

    test('meta description has content', async ({ page }) => {
      const content = await page.getAttribute('meta[name="description"]', 'content');
      expect(content).toBeTruthy();
      expect(content!.length).toBeGreaterThan(50);
    });

    test('meta description mentions MirDB', async ({ page }) => {
      const content = await page.getAttribute('meta[name="description"]', 'content');
      expect(content).toContain('MirDB');
    });
  });

  test.describe('Test Case 3: Page has Open Graph meta tags', () => {
    test('og:title meta tag exists and has content', async ({ page }) => {
      const ogTitle = page.locator('meta[property="og:title"]');
      await expect(ogTitle).toHaveCount(1);

      const content = await page.getAttribute('meta[property="og:title"]', 'content');
      expect(content).toBeTruthy();
      expect(content).toContain('MirDB');
    });

    test('og:description meta tag exists and has content', async ({ page }) => {
      const ogDescription = page.locator('meta[property="og:description"]');
      await expect(ogDescription).toHaveCount(1);

      const content = await page.getAttribute('meta[property="og:description"]', 'content');
      expect(content).toBeTruthy();
      expect(content!.length).toBeGreaterThan(50);
    });

    test('og:image meta tag exists and has content', async ({ page }) => {
      const ogImage = page.locator('meta[property="og:image"]');
      await expect(ogImage).toHaveCount(1);

      const content = await page.getAttribute('meta[property="og:image"]', 'content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });

    test('og:url meta tag exists', async ({ page }) => {
      const ogUrl = page.locator('meta[property="og:url"]');
      await expect(ogUrl).toHaveCount(1);

      const content = await page.getAttribute('meta[property="og:url"]', 'content');
      expect(content).toBeTruthy();
      expect(content).toMatch(/^https?:\/\//);
    });

    test('og:type meta tag exists', async ({ page }) => {
      const ogType = page.locator('meta[property="og:type"]');
      await expect(ogType).toHaveCount(1);

      const content = await page.getAttribute('meta[property="og:type"]', 'content');
      expect(content).toBe('website');
    });
  });

  test.describe('Test Case 4: Page has canonical URL meta tag', () => {
    test('canonical link tag exists', async ({ page }) => {
      const canonical = page.locator('link[rel="canonical"]');
      await expect(canonical).toHaveCount(1);
    });

    test('canonical URL is a valid URL', async ({ page }) => {
      const href = await page.getAttribute('link[rel="canonical"]', 'href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  test.describe('Test Case 5: Page has exactly one h1 element', () => {
    test('exactly one h1 element exists', async ({ page }) => {
      // Wait for the app to render
      await page.waitForSelector('#hero');

      const h1Elements = page.locator('h1');
      await expect(h1Elements).toHaveCount(1);
    });

    test('h1 element contains MirDB', async ({ page }) => {
      await page.waitForSelector('#hero');

      const h1 = page.locator('h1');
      const text = await h1.textContent();
      expect(text).toContain('MirDB');
    });
  });

  test.describe('Test Case 6: Headings follow proper hierarchy', () => {
    test('no heading levels are skipped', async ({ page }) => {
      // Wait for all sections to render
      await page.waitForSelector('#hero');
      await page.waitForSelector('#features');
      await page.waitForSelector('#quickstart');
      await page.waitForSelector('#protocol');
      await page.waitForSelector('#configuration');
      await page.waitForSelector('#architecture');

      // Get all headings in document order
      const headings = await page.evaluate(() => {
        const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        return Array.from(allHeadings).map(h => ({
          level: parseInt(h.tagName.substring(1)),
          text: h.textContent?.trim() || ''
        }));
      });

      // Verify heading hierarchy
      expect(headings.length).toBeGreaterThan(0);

      // First heading should be h1
      expect(headings[0].level).toBe(1);

      // Check that no levels are skipped
      for (let i = 1; i < headings.length; i++) {
        const currentLevel = headings[i].level;
        const previousLevel = headings[i - 1].level;

        // When going to a higher level number (deeper nesting),
        // we should only increase by 1 at most
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    test('h2 elements exist for main sections', async ({ page }) => {
      await page.waitForSelector('#features');

      const h2Elements = page.locator('h2');
      const count = await h2Elements.count();

      // Should have h2 for Features, Quick Start, Protocol, Configuration, Architecture
      expect(count).toBeGreaterThanOrEqual(5);
    });

    test('h3 elements are children of h2 sections', async ({ page }) => {
      await page.waitForSelector('#features');

      // Get the structure of headings
      const headingStructure = await page.evaluate(() => {
        const headings = document.querySelectorAll('h2, h3');
        let lastH2Index = -1;
        const h3BeforeH2: string[] = [];

        Array.from(headings).forEach((h, index) => {
          if (h.tagName === 'H2') {
            lastH2Index = index;
          } else if (h.tagName === 'H3' && lastH2Index === -1) {
            h3BeforeH2.push(h.textContent || '');
          }
        });

        return {
          h3BeforeH2,
          totalH2: document.querySelectorAll('h2').length,
          totalH3: document.querySelectorAll('h3').length
        };
      });

      // No h3 should appear before any h2
      expect(headingStructure.h3BeforeH2).toHaveLength(0);
    });
  });

  test.describe('Additional SEO Best Practices', () => {
    test('html lang attribute is set', async ({ page }) => {
      const lang = await page.getAttribute('html', 'lang');
      expect(lang).toBe('en');
    });

    test('viewport meta tag exists', async ({ page }) => {
      const viewport = page.locator('meta[name="viewport"]');
      await expect(viewport).toHaveCount(1);

      const content = await page.getAttribute('meta[name="viewport"]', 'content');
      expect(content).toContain('width=device-width');
    });

    test('charset meta tag exists', async ({ page }) => {
      const charset = page.locator('meta[charset]');
      await expect(charset).toHaveCount(1);

      const charsetValue = await page.getAttribute('meta[charset]', 'charset');
      expect(charsetValue?.toLowerCase()).toBe('utf-8');
    });
  });
});
