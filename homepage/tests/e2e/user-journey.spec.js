/**
 * User Journey E2E Tests
 * Owner: Scenario 1 (primary), Contributors: Scenarios 2-5
 *
 * Expected tests:
 * - User can see hero section on page load
 * - User can scroll to view all features
 * - User can view quick start code examples
 * - User can click navigation links
 * - User can click external links (GitHub, docs)
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section and Branding (Scenario 1)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Page loads successfully with hero section visible
  test('TC1: Page loads successfully with hero section visible', async ({ page }) => {
    // Verify page loads
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero is above the fold (visible without scrolling)
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.y).toBeLessThan(100); // Hero should start near top of page
  });

  // Test Case 2: Logo element exists with proper alt text
  test('TC2: Logo element exists with proper alt text "MirDB logo"', async ({ page }) => {
    const logo = page.locator('.hero__logo img');
    await expect(logo).toBeVisible();

    // Verify alt text
    const altText = await logo.getAttribute('alt');
    expect(altText).toBe('MirDB logo');
  });

  // Test Case 3: H1 contains 'MirDB' text
  test('TC3: H1 heading contains "MirDB" text', async ({ page }) => {
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');
  });

  // Test Case 4: Tagline contains required keywords
  test('TC4: Tagline contains "Persistent Key-Value Store" and "Memcached protocol"', async ({ page }) => {
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText).toMatch(/Persistent Key-Value Store/i);
    expect(taglineText).toMatch(/Memcached protocol/i);
  });

  // Test Case 5: Primary CTA button is visible and clickable
  test('TC5: CTA button with text "Get Started" is visible and clickable', async ({ page }) => {
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Check for "Get Started" text
    await expect(ctaButton).toContainText('Get Started');

    // Verify it's clickable (has href)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();
  });

  // Test Case 6: Click CTA navigates to Quick Start section
  test('TC6: Clicking CTA scrolls/navigates to Quick Start section', async ({ page }) => {
    const ctaButton = page.locator('.hero__cta');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the CTA
    await ctaButton.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(500);

    // Verify we've scrolled or the quick-start section is now in view
    const quickStartSection = page.locator('#quick-start');

    // Check if quick-start section exists and is scrolled into view
    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= 0 && rect.top < window.innerHeight;
    });

    expect(isInViewport).toBe(true);
  });
});

// Additional tests for future scenarios can be added below
