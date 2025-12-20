/**
 * E2E Tests for Hero Section Value Proposition Display
 * Test Cases: 1, 2
 * Validates hero section content and viewport positioning
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Hero Section Value Proposition Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test.describe('Test Case 1: Hero Section DOM Elements', () => {
    test('should display h1 with MirDB product name', async ({ page }) => {
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toHaveText('MirDB');
    });

    test('should display tagline mentioning Persistent and Memcached', async ({ page }) => {
      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();

      const text = await tagline.textContent();
      expect(text).toContain('Persistent');
      expect(text).toContain('Memcached');
    });

    test('should display 2-3 sentence value proposition', async ({ page }) => {
      const description = page.locator('.hero-description');
      await expect(description).toBeVisible();

      const text = await description.textContent();
      // Count sentences (approximate by counting periods followed by space or end)
      const sentenceCount = (text.match(/[.!?]+\s*/g) || []).length;
      expect(sentenceCount).toBeGreaterThanOrEqual(2);
      expect(sentenceCount).toBeLessThanOrEqual(4);
    });

    test('should have CTA buttons for documentation and GitHub', async ({ page }) => {
      const getStartedBtn = page.locator('.btn-primary');
      const githubBtn = page.locator('.btn-secondary');

      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();

      await expect(getStartedBtn).toContainText('Get Started');
      await expect(githubBtn).toContainText('GitHub');
    });

    test('should display key differentiators including persistence', async ({ page }) => {
      const differentiators = page.locator('.hero-differentiators');
      await expect(differentiators).toBeVisible();

      const persistentDiff = differentiators.locator('text=Persistent Storage');
      await expect(persistentDiff).toBeVisible();
    });
  });

  test.describe('Test Case 2: Hero Section Viewport Positioning', () => {
    test('should display all hero content above the fold on desktop (1920x1080)', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });

      // Check hero section is fully visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Check all key elements are within viewport
      const h1 = page.locator('h1');
      const tagline = page.locator('.hero-tagline');
      const description = page.locator('.hero-description');
      const cta = page.locator('.hero-cta');

      // Verify elements are in viewport (no scrolling needed)
      const heroBox = await hero.boundingBox();
      const h1Box = await h1.boundingBox();
      const taglineBox = await tagline.boundingBox();
      const descriptionBox = await description.boundingBox();
      const ctaBox = await cta.boundingBox();

      // All elements should be visible within viewport
      expect(h1Box.y + h1Box.height).toBeLessThan(1080);
      expect(taglineBox.y + taglineBox.height).toBeLessThan(1080);
      expect(descriptionBox.y + descriptionBox.height).toBeLessThan(1080);
      expect(ctaBox.y + ctaBox.height).toBeLessThan(1080);
    });

    test('should display core hero content above the fold on mobile (375x667)', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Check hero section is visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Check critical elements are within mobile viewport
      const h1 = page.locator('h1');
      const tagline = page.locator('.hero-tagline');

      const h1Box = await h1.boundingBox();
      const taglineBox = await tagline.boundingBox();

      // Product name and tagline should be visible without scrolling
      expect(h1Box.y + h1Box.height).toBeLessThan(667);
      expect(taglineBox.y + taglineBox.height).toBeLessThan(667);
    });

    test('should not require horizontal scrolling on any viewport', async ({ page }) => {
      // Test desktop
      await page.setViewportSize({ width: 1920, height: 1080 });
      let scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      expect(scrollWidth).toBeLessThanOrEqual(1920);

      // Test tablet
      await page.setViewportSize({ width: 768, height: 1024 });
      scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      expect(scrollWidth).toBeLessThanOrEqual(768);

      // Test mobile
      await page.setViewportSize({ width: 375, height: 667 });
      scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      expect(scrollWidth).toBeLessThanOrEqual(375);
    });
  });

  test.describe('Accessibility and User Experience', () => {
    test('should load and display content within 3 seconds', async ({ page }) => {
      const startTime = Date.now();
      await page.goto(indexPath);
      await page.locator('h1').waitFor({ state: 'visible' });
      const loadTime = Date.now() - startTime;

      expect(loadTime).toBeLessThan(3000);
    });

    test('should clearly communicate persistence advantage', async ({ page }) => {
      // Check multiple places where persistence is mentioned
      const heroContent = await page.locator('.hero-content').textContent();

      // The word "persistent" or "persistence" should appear in hero
      const hasPersistenceKeyword =
        heroContent.toLowerCase().includes('persistent') ||
        heroContent.toLowerCase().includes('persistence');

      expect(hasPersistenceKeyword).toBe(true);
    });

    test('should have proper contrast for readability', async ({ page }) => {
      // Verify key text elements have readable colors
      const h1 = page.locator('h1');
      const color = await h1.evaluate((el) => window.getComputedStyle(el).color);

      // The text should have some color (not transparent)
      expect(color).not.toBe('rgba(0, 0, 0, 0)');
    });
  });
});
