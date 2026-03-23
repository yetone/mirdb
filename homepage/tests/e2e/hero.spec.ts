/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases:
 * - Product name "MirDB" is displayed
 * - Value proposition is visible
 * - CTA button is present and clickable
 * - CTA navigates to Quick Start section
 */

import { test, expect } from '@playwright/test';
import { navigateToHomepage, selectors, viewports } from './test-utils';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToHomepage(page);
  });

  test('TC1: Product name MirDB is displayed prominently in the hero section', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator(selectors.hero.section);
    await expect(heroSection).toBeVisible();

    // Verify product name "MirDB" is displayed
    const heroTitle = page.locator(selectors.hero.title);
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify the title is prominent (large font size)
    const fontSize = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeValue = parseFloat(fontSize);
    expect(fontSizeValue).toBeGreaterThan(40); // Should be larger than 40px

    // Verify hero section is above the fold (visible without scrolling)
    const boundingBox = await heroSection.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox!.y).toBeLessThan(100); // Should start near top of page
  });

  test('TC2: Value proposition describing MirDB as persistent key-value store is visible', async ({ page }) => {
    // Verify tagline/value proposition is visible
    const heroTagline = page.locator(selectors.hero.tagline);
    await expect(heroTagline).toBeVisible();

    // Get tagline text
    const taglineText = await heroTagline.textContent();
    expect(taglineText).not.toBeNull();

    // Verify it mentions key concepts
    const lowerText = taglineText!.toLowerCase();
    expect(lowerText).toContain('persistent');
    expect(lowerText).toContain('key-value');
    expect(lowerText).toContain('memcached');

    // Verify the tagline is readable (good font size)
    const fontSize = await heroTagline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeValue = parseFloat(fontSize);
    expect(fontSizeValue).toBeGreaterThanOrEqual(16); // At least 16px for readability
  });

  test('TC3: CTA button Get Started is present and clickable', async ({ page }) => {
    // Verify CTA button exists
    const ctaButton = page.locator(selectors.hero.ctaButton);
    await expect(ctaButton).toBeVisible();

    // Verify button text
    await expect(ctaButton).toHaveText('Get Started');

    // Verify button is clickable (enabled)
    await expect(ctaButton).toBeEnabled();

    // Verify button has proper role
    const role = await ctaButton.getAttribute('role');
    expect(role).toBe('button');

    // Verify button has href (is a link)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Verify button is visually prominent
    const backgroundColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Should have a colored background (not transparent or white)
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('transparent');
  });

  test('TC4: CTA button navigates to Quick Start section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the CTA button
    const ctaButton = page.locator(selectors.hero.ctaButton);
    await ctaButton.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Verify URL hash changed to quickstart
    const url = page.url();
    expect(url).toContain('#quickstart');

    // Verify Quick Start section is now visible in viewport
    const quickstartSection = page.locator(selectors.quickstart.section);
    await expect(quickstartSection).toBeInViewport();

    // Verify we scrolled down (not on hero section anymore)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);
  });

  test('Hero section is accessible', async ({ page }) => {
    // Verify hero section has proper aria-labelledby
    const heroSection = page.locator(selectors.hero.section);
    const ariaLabelledBy = await heroSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('hero-title');

    // Verify heading hierarchy
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    const h1Count = await h1.count();
    expect(h1Count).toBe(1); // Only one h1 on the page

    // Verify CTA button is keyboard accessible
    const ctaButton = page.locator(selectors.hero.ctaButton);
    await ctaButton.focus();
    const isFocused = await ctaButton.evaluate((el) => document.activeElement === el);
    expect(isFocused).toBe(true);
  });

  test('Hero section displays correctly on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(viewports.mobile);

    // Verify hero content is still visible
    const heroTitle = page.locator(selectors.hero.title);
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    const heroTagline = page.locator(selectors.hero.tagline);
    await expect(heroTagline).toBeVisible();

    const ctaButton = page.locator(selectors.hero.ctaButton);
    await expect(ctaButton).toBeVisible();

    // Verify no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });
});
