/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * End-to-end tests for hero section:
 * - Logo displays correctly
 * - Headline text is correct
 * - Tagline text is present
 * - Technology indicators visible
 * - Get Started button scrolls to Quick Start
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section displays MirDB logo from repository assets', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify logo is present and has correct source
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();
    await expect(logo).toHaveAttribute('src', 'assets/logo.gif');
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Verify logo loads successfully (not broken)
    const logoNaturalWidth = await logo.evaluate((img) => img.naturalWidth);
    expect(logoNaturalWidth).toBeGreaterThan(0);
  });

  test('TC2: Headline contains MirDB: A Persistent Key-Value Store with Memcached Protocol', async ({ page }) => {
    const headline = page.locator('.hero-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('MirDB: A Persistent Key-Value Store with Memcached Protocol');

    // Verify it's an h1 element for proper semantic structure
    const tagName = await headline.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');
  });

  test('TC3: Tagline describes MirDB as simple, fast, durable key-value storage written in Rust', async ({ page }) => {
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Simple, fast, and durable key-value storage written in Rust');
  });

  test('TC4: Get Started button scrolls smoothly to Quick Start section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Find and click the Get Started button
    const ctaButton = page.locator('.hero-cta');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toHaveText('Get Started');
    await expect(ctaButton).toHaveAttribute('href', '#quickstart');

    // Click the button
    await ctaButton.click();

    // Wait for scroll to complete
    await page.waitForTimeout(1000);

    // Verify the page has scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify the quickstart section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('TC5: Visual indicators show Rust and Tokio technology stack', async ({ page }) => {
    // Verify tech stack container exists
    const techStack = page.locator('.hero-tech-stack');
    await expect(techStack).toBeVisible();

    // Verify Rust badge
    const rustBadge = page.locator('.tech-badge--rust');
    await expect(rustBadge).toBeVisible();
    await expect(rustBadge).toContainText('Rust');
    await expect(rustBadge).toHaveAttribute('aria-label', 'Built with Rust');

    // Verify Tokio badge
    const tokioBadge = page.locator('.tech-badge--tokio');
    await expect(tokioBadge).toBeVisible();
    await expect(tokioBadge).toContainText('Tokio');
    await expect(tokioBadge).toHaveAttribute('aria-label', 'Powered by Tokio');
  });

  test('Hero CTA button has accessible touch target size', async ({ page }) => {
    const ctaButton = page.locator('.hero-cta');
    const boundingBox = await ctaButton.boundingBox();

    // Verify minimum 44x44px touch target (WCAG 2.5.5)
    expect(boundingBox.width).toBeGreaterThanOrEqual(44);
    expect(boundingBox.height).toBeGreaterThanOrEqual(44);
  });

  test('Hero section is accessible with proper ARIA attributes', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Verify section has aria-labelledby pointing to the headline
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');

    // Verify tech stack has aria-label
    const techStack = page.locator('.hero-tech-stack');
    await expect(techStack).toHaveAttribute('aria-label', 'Technology stack');
  });
});
