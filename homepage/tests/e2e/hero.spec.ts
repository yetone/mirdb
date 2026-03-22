/**
 * Hero Section E2E Tests
 * Owners: Scenario 1 (Hero), Scenario 2 (Badges), Scenario 18 (Theme)
 *
 * Test groups:
 * - Hero section visibility and content
 * - Logo display and animation
 * - Value proposition text validation
 * - Primary CTA presence and functionality
 * - Status badges display and links
 * - Theme toggle functionality
 */

import { test, expect } from '@playwright/test';
import { waitForLoad, countWords } from './utils';

test.describe('Hero Section Display (Scenario 1)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: Hero section contains logo image with logo.gif source', async ({ page }) => {
    // Test Case 1: Load homepage and inspect hero section DOM
    // Expected: Hero section contains img element with src pointing to logo.gif or logo asset

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const logo = page.locator('#hero-logo');
    await expect(logo).toBeVisible();

    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');
  });

  test('TC2: Headline text is present and contains 15 words or fewer', async ({ page }) => {
    // Test Case 2: Extract headline text from hero section
    // Expected: Headline text is present, non-empty, and contains 15 words or fewer

    const headline = page.locator('#hero-headline');
    await expect(headline).toBeVisible();

    const headlineText = await headline.textContent();
    expect(headlineText).toBeTruthy();
    expect(headlineText!.trim().length).toBeGreaterThan(0);

    const wordCount = countWords(headlineText!);
    expect(wordCount).toBeLessThanOrEqual(15);
  });

  test('TC3: Value proposition contains relevant keywords', async ({ page }) => {
    // Test Case 3: Check value proposition content
    // Expected: Text includes keywords like 'persistent', 'key-value', or 'Memcached'

    const headline = page.locator('#hero-headline');
    const headlineText = await headline.textContent();

    const lowerText = headlineText!.toLowerCase();
    const hasRelevantKeyword =
      lowerText.includes('persistent') ||
      lowerText.includes('key-value') ||
      lowerText.includes('memcached');

    expect(hasRelevantKeyword).toBe(true);
  });

  test('TC4: Primary CTA button exists and is clickable', async ({ page }) => {
    // Test Case 4: Locate and click primary CTA button
    // Expected: CTA button exists with visible text and is clickable

    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeVisible();

    const ctaText = await primaryCta.textContent();
    expect(ctaText).toBeTruthy();
    expect(ctaText!.trim().length).toBeGreaterThan(0);

    // Check that the button is enabled and clickable
    await expect(primaryCta).toBeEnabled();

    // Verify it has an href attribute (links somewhere)
    const href = await primaryCta.getAttribute('href');
    expect(href).toBeTruthy();
  });
});
