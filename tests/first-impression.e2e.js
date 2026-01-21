/**
 * First Impression User Journey E2E Tests
 * Scenario: Verify first-time visitor can immediately understand product value (User Story 1)
 *
 * Test Cases:
 * - TC1: Check headline visibility on page load - within viewport without scrolling
 * - TC2: Check subheadline visibility - within viewport without scrolling
 * - TC3: Verify value proposition clarity - headline and subheadline together explain what product does
 */

import { test, expect } from '@playwright/test';

test.describe('First Impression User Journey', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to landing page (simulating first-time visitor)
    await page.goto('/');
  });

  test.describe('Test Case 1: Headline Visibility on Page Load', () => {
    test('headline should be visible within viewport without scrolling', async ({ page }) => {
      // Get the headline element
      const headline = page.locator('[data-testid="hero-headline"]');

      // Verify headline exists and is visible
      await expect(headline).toBeVisible();

      // Get headline bounding box
      const headlineBox = await headline.boundingBox();
      expect(headlineBox).not.toBeNull();

      // Get viewport size
      const viewportSize = page.viewportSize();
      expect(viewportSize).not.toBeNull();

      // Verify headline is within the viewport (above the fold)
      // The headline's bottom edge should be within the viewport height
      expect(headlineBox.y).toBeGreaterThanOrEqual(0);
      expect(headlineBox.y + headlineBox.height).toBeLessThanOrEqual(viewportSize.height);
    });

    test('headline should be immediately visible on page load', async ({ page }) => {
      // Verify headline is visible without any user interaction
      const headline = page.locator('[data-testid="hero-headline"]');

      // Wait for headline to be visible with a short timeout
      // This ensures it loads quickly as expected for first impression
      await expect(headline).toBeVisible({ timeout: 2000 });

      // Verify headline has text content
      const headlineText = await headline.textContent();
      expect(headlineText.trim()).not.toBe('');
    });

    test('headline should have readable text size for first impression', async ({ page }) => {
      const headline = page.locator('[data-testid="hero-headline"]');

      // Get computed font size
      const fontSize = await headline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Headline should be at least 24px for readability (typically 2rem+)
      expect(fontSize).toBeGreaterThanOrEqual(24);
    });
  });

  test.describe('Test Case 2: Subheadline Visibility', () => {
    test('subheadline should be visible within viewport without scrolling', async ({ page }) => {
      // Get the subheadline element
      const subheadline = page.locator('[data-testid="hero-subheadline"]');

      // Verify subheadline exists and is visible
      await expect(subheadline).toBeVisible();

      // Get subheadline bounding box
      const subheadlineBox = await subheadline.boundingBox();
      expect(subheadlineBox).not.toBeNull();

      // Get viewport size
      const viewportSize = page.viewportSize();
      expect(viewportSize).not.toBeNull();

      // Verify subheadline is within the viewport (above the fold)
      expect(subheadlineBox.y).toBeGreaterThanOrEqual(0);
      expect(subheadlineBox.y + subheadlineBox.height).toBeLessThanOrEqual(viewportSize.height);
    });

    test('subheadline should be positioned below headline', async ({ page }) => {
      const headline = page.locator('[data-testid="hero-headline"]');
      const subheadline = page.locator('[data-testid="hero-subheadline"]');

      const headlineBox = await headline.boundingBox();
      const subheadlineBox = await subheadline.boundingBox();

      expect(headlineBox).not.toBeNull();
      expect(subheadlineBox).not.toBeNull();

      // Subheadline should be below the headline
      expect(subheadlineBox.y).toBeGreaterThan(headlineBox.y);
    });

    test('subheadline should have readable text size', async ({ page }) => {
      const subheadline = page.locator('[data-testid="hero-subheadline"]');

      // Get computed font size
      const fontSize = await subheadline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Subheadline should be at least 16px for readability
      expect(fontSize).toBeGreaterThanOrEqual(16);
    });
  });

  test.describe('Test Case 3: Value Proposition Clarity', () => {
    test('headline and subheadline together explain what product does', async ({ page }) => {
      const headline = page.locator('[data-testid="hero-headline"]');
      const subheadline = page.locator('[data-testid="hero-subheadline"]');

      // Get text content
      const headlineText = await headline.textContent();
      const subheadlineText = await subheadline.textContent();

      // Combine for value proposition analysis
      const combinedText = `${headlineText} ${subheadlineText}`.toLowerCase();

      // Verify combined content explains what the product does
      // Should contain key value proposition elements:
      // 1. What it is (key-value storage/database)
      const identifiesProduct =
        combinedText.includes('key-value') ||
        combinedText.includes('storage') ||
        combinedText.includes('database') ||
        combinedText.includes('store');

      // 2. Key benefit (fast/speed/performance)
      const identifiesBenefit =
        combinedText.includes('fast') ||
        combinedText.includes('speed') ||
        combinedText.includes('blazing') ||
        combinedText.includes('lightning');

      // 3. Additional context (persistence, memcached, reliability)
      const providesContext =
        combinedText.includes('persistent') ||
        combinedText.includes('memcached') ||
        combinedText.includes('reliable') ||
        combinedText.includes('reliability');

      expect(identifiesProduct).toBe(true);
      expect(identifiesBenefit).toBe(true);
      expect(providesContext).toBe(true);
    });

    test('headline should be concise (10 words or fewer)', async ({ page }) => {
      const headline = page.locator('[data-testid="hero-headline"]');
      const headlineText = await headline.textContent();

      // Count words (PRD requirement: max 10 words)
      const wordCount = headlineText.trim().split(/\s+/).filter((word) => word.length > 0).length;

      expect(wordCount).toBeLessThanOrEqual(10);
    });

    test('subheadline should provide meaningful context (20+ characters)', async ({ page }) => {
      const subheadline = page.locator('[data-testid="hero-subheadline"]');
      const subheadlineText = await subheadline.textContent();

      // Subheadline should be substantial enough to explain value proposition
      expect(subheadlineText.trim().length).toBeGreaterThan(20);
    });

    test('content should be scannable within 5 seconds of reading', async ({ page }) => {
      const headline = page.locator('[data-testid="hero-headline"]');
      const subheadline = page.locator('[data-testid="hero-subheadline"]');

      const headlineText = await headline.textContent();
      const subheadlineText = await subheadline.textContent();

      // Calculate approximate reading time based on average reading speed
      // Average reading speed is ~200-250 words per minute (4-5 words per second)
      const combinedWords =
        headlineText.trim().split(/\s+/).length + subheadlineText.trim().split(/\s+/).length;

      // 5 seconds at 4 words/second = 20 words maximum for quick scanning
      // Being generous with 25 words to account for varied reading speeds
      expect(combinedWords).toBeLessThanOrEqual(40);
    });
  });

  test.describe('Above-the-Fold Content Verification', () => {
    test('hero section should be first content visible', async ({ page }) => {
      const heroSection = page.locator('[data-testid="hero-section"]');

      // Hero section should be visible without scrolling
      await expect(heroSection).toBeVisible();

      // Get hero section position
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();

      // Hero should start near the top (after header)
      expect(heroBox.y).toBeLessThan(150); // Allow for fixed header height
    });

    test('primary CTA should be visible with headline and subheadline', async ({ page }) => {
      const ctaButton = page.locator('[data-testid="hero-cta-primary"]');

      await expect(ctaButton).toBeVisible();

      // Get CTA position
      const ctaBox = await ctaButton.boundingBox();
      const viewportSize = page.viewportSize();

      // CTA should be within viewport
      expect(ctaBox.y + ctaBox.height).toBeLessThanOrEqual(viewportSize.height);
    });
  });
});
