/**
 * Responsive Design Tests
 * Owner: Scenario 9 - Responsive Design - Mobile
 *
 * Test cases:
 * - Page renders at 320px viewport
 * - Hamburger menu works on mobile
 * - Touch targets are 44x44px minimum
 * - Code blocks scroll horizontally
 * - Tablet (768px) layout works
 * - Font sizes readable on mobile
 */

import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile', () => {
  test.describe('Test Case 1: 320px Viewport Width', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('page renders without horizontal overflow at 320px', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check that body doesn't have horizontal overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const windowWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth);
    });

    test('all sections are visible and accessible at 320px', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Hero section
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Features section
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Getting started section
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();
    });

    test('no content is clipped at 320px viewport', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check main content areas are fully within viewport width
      const heroContainer = page.locator('.hero-container');
      const heroBox = await heroContainer.boundingBox();

      if (heroBox) {
        expect(heroBox.x).toBeGreaterThanOrEqual(0);
        expect(heroBox.x + heroBox.width).toBeLessThanOrEqual(320);
      }
    });
  });

  test.describe('Test Case 2: Hamburger Menu on Mobile', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('hamburger menu button is visible on mobile', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navToggle = page.locator('.nav-toggle');
      await expect(navToggle).toBeVisible();
    });

    test('hamburger menu opens when clicked', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navToggle = page.locator('.nav-toggle');
      const navLinks = page.locator('.nav-links');

      // Initially nav-links should be hidden
      await expect(navLinks).not.toHaveClass(/open/);

      // Click the hamburger menu
      await navToggle.click();

      // Now nav-links should be visible with 'open' class
      await expect(navLinks).toHaveClass(/open/);
    });

    test('hamburger menu closes when clicked again', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navToggle = page.locator('.nav-toggle');
      const navLinks = page.locator('.nav-links');

      // Open the menu
      await navToggle.click();
      await expect(navLinks).toHaveClass(/open/);

      // Close the menu
      await navToggle.click();
      await expect(navLinks).not.toHaveClass(/open/);
    });

    test('hamburger menu aria-expanded updates correctly', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navToggle = page.locator('.nav-toggle');

      // Initially should be false
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

      // Click to open
      await navToggle.click();
      await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

      // Click to close
      await navToggle.click();
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
    });

    test('navigation links are accessible when menu is open', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navToggle = page.locator('.nav-toggle');
      await navToggle.click();

      // Check that navigation links are visible
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
      await expect(gettingStartedLink).toBeVisible();
    });
  });

  test.describe('Test Case 3: Touch Targets on Mobile', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('navigation toggle has minimum 44x44px touch target', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navToggle = page.locator('.nav-toggle');
      const box = await navToggle.boundingBox();

      expect(box).not.toBeNull();
      if (box) {
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('CTA buttons have adequate touch targets', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const primaryBtn = page.locator('.btn-primary').first();
      const box = await primaryBtn.boundingBox();

      expect(box).not.toBeNull();
      if (box) {
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('copy buttons have minimum touch target size', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Scroll to the first code block
      const firstCopyButton = page.locator('.copy-button').first();
      await firstCopyButton.scrollIntoViewIfNeeded();

      const box = await firstCopyButton.boundingBox();

      expect(box).not.toBeNull();
      if (box) {
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('navigation links have adequate touch targets when menu is open', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Open the mobile menu
      const navToggle = page.locator('.nav-toggle');
      await navToggle.click();

      // Check each nav link has sufficient touch target
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        const box = await link.boundingBox();

        if (box) {
          // Links should have at least 44px height for touch accessibility
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });
  });

  test.describe('Test Case 4: Code Blocks on Mobile', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('code blocks are horizontally scrollable when content overflows', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Find the code blocks
      const codeBlocks = page.locator('.code-block pre');
      const count = await codeBlocks.count();

      expect(count).toBeGreaterThan(0);

      // Check that at least one code block has overflow-x set to auto or scroll
      const firstCodeBlock = codeBlocks.first();
      await firstCodeBlock.scrollIntoViewIfNeeded();

      const overflowX = await firstCodeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(['auto', 'scroll']).toContain(overflowX);
    });

    test('code blocks content is visible without clipping', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Navigate to Getting Started section
      const gettingStarted = page.locator('#getting-started');
      await gettingStarted.scrollIntoViewIfNeeded();

      // Check that code blocks are visible
      const codeExample = page.locator('.code-block').first();
      await expect(codeExample).toBeVisible();
    });

    test('code blocks do not cause horizontal page overflow', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Scroll through the entire page
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(500);

      // Check that body doesn't have horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.body.scrollWidth > window.innerWidth;
      });

      expect(hasHorizontalOverflow).toBe(false);
    });
  });

  test.describe('Test Case 5: Tablet Viewport (768px)', () => {
    test.use({ viewport: { width: 768, height: 1024 } });

    test('page renders appropriately at tablet width', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check no horizontal overflow at tablet size
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const windowWidth = await page.evaluate(() => window.innerWidth);

      expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth);
    });

    test('hamburger menu is hidden on tablet', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navToggle = page.locator('.nav-toggle');
      // On tablet (768px+), hamburger should be hidden
      await expect(navToggle).toBeHidden();
    });

    test('navigation links are visible inline on tablet', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Check navigation is displayed as flex row
      const display = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });

      expect(display).toBe('flex');
    });

    test('features grid adapts to tablet layout', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const featuresGrid = page.locator('.features-grid');
      await featuresGrid.scrollIntoViewIfNeeded();

      // Grid should display multiple columns on tablet
      const gridTemplateColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have multiple columns (not just 1fr)
      const columnCount = gridTemplateColumns.split(' ').length;
      expect(columnCount).toBeGreaterThanOrEqual(2);
    });
  });

  test.describe('Test Case 6: Font Sizes on Mobile', () => {
    test.use({ viewport: { width: 320, height: 568 } });

    test('body text is at least 16px for readability', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const fontSize = await page.evaluate(() => {
        const body = document.body;
        return parseFloat(window.getComputedStyle(body).fontSize);
      });

      expect(fontSize).toBeGreaterThanOrEqual(16);
    });

    test('paragraph text in features is readable', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const featureText = page.locator('.feature-card p').first();
      await featureText.scrollIntoViewIfNeeded();

      const fontSize = await featureText.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Should be at least 14px (var(--font-size-sm) is 0.875rem = 14px)
      expect(fontSize).toBeGreaterThanOrEqual(14);
    });

    test('headings scale appropriately on mobile', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check h1 (hero title)
      const h1 = page.locator('.hero-title');
      const h1FontSize = await h1.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // H1 should still be larger than body text
      expect(h1FontSize).toBeGreaterThan(20);

      // Check section h2
      const h2 = page.locator('.features h2');
      await h2.scrollIntoViewIfNeeded();

      const h2FontSize = await h2.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // H2 should be larger than body text but maintain hierarchy
      expect(h2FontSize).toBeGreaterThan(18);
      expect(h2FontSize).toBeLessThan(h1FontSize);
    });

    test('tagline text is readable on mobile', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const tagline = page.locator('.hero-tagline');
      const fontSize = await tagline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Tagline should be at least 16px (base font size)
      expect(fontSize).toBeGreaterThanOrEqual(16);
    });

    test('code block text is readable', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const codeBlock = page.locator('.code-block code').first();
      await codeBlock.scrollIntoViewIfNeeded();

      const fontSize = await codeBlock.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Code should be at least 14px (var(--font-size-sm))
      expect(fontSize).toBeGreaterThanOrEqual(14);
    });
  });
});
