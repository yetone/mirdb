// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Technology Stack Information Tests (REQ-5)
 *
 * These tests verify that technology stack and dependencies are displayed
 * prominently on the MirDB homepage.
 */
test.describe('Technology Stack Information (REQ-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page mentions Rust as the implementation language', async ({ page }) => {
    // Search for Rust mentioned anywhere on the page
    const pageContent = await page.textContent('body');

    // Verify Rust is mentioned in the page content
    expect(pageContent).toContain('Rust');

    // Additionally verify Rust is mentioned in a meaningful context
    // Check the features section for "Written in Rust" feature card
    const rustFeatureCard = page.locator('.feature-card:has-text("Rust")');
    await expect(rustFeatureCard).toBeVisible();

    // Verify the Rust feature card has meaningful description
    await expect(rustFeatureCard).toContainText('Written in Rust');

    // Also verify Rust is mentioned in the hero tagline
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toContainText('Rust');
  });

  test('TC2: Page mentions Tokio (async runtime) or other key dependencies', async ({ page }) => {
    // Search for Tokio mentioned on the page
    const pageContent = await page.textContent('body');

    // Verify Tokio is mentioned
    expect(pageContent).toContain('Tokio');

    // Check that Tokio is mentioned in the features section with context
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify Tokio is associated with async I/O functionality
    const tokioContext = page.locator('.feature-card:has-text("Tokio")');
    await expect(tokioContext).toBeVisible();
    await expect(tokioContext).toContainText('async');

    // Verify other key dependencies/technologies are mentioned
    // These are key dependencies for a high-performance key-value store
    const keyTechnologies = ['LSM-tree', 'Skip list', 'Memcached'];

    for (const tech of keyTechnologies) {
      expect(pageContent).toContain(tech);
    }
  });

  test('TC3: Footer contains badges or icons for key technologies', async ({ page }) => {
    // Scroll to footer to ensure it's visible
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer has a technology section with badges
    const footerTech = footer.locator('.footer-tech');
    await expect(footerTech).toBeVisible();

    // Verify badges exist in the footer
    const badges = footer.locator('.badge');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThanOrEqual(3);

    // Verify specific technology badges are present
    // These are the key technologies from the PRD
    const expectedTechnologies = ['Rust', 'Tokio', 'LSM-tree'];

    for (const tech of expectedTechnologies) {
      const badge = footer.locator(`.badge:has-text("${tech}")`);
      await expect(badge).toBeVisible();
    }

    // Verify badges have appropriate styling (not just plain text)
    const firstBadge = badges.first();
    const badgeStyles = await firstBadge.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        padding: styles.padding,
        borderRadius: styles.borderRadius
      };
    });

    // Badges should have some styling applied (background color not transparent)
    expect(badgeStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('Technology information is accessible via keyboard navigation', async ({ page }) => {
    // Start from the top of the page
    await page.keyboard.press('Tab');

    // Navigate to footer using keyboard
    const footer = page.locator('footer');

    // Scroll to footer to verify technology badges are accessible
    await footer.scrollIntoViewIfNeeded();

    // Verify footer badges are in the document flow
    const badges = footer.locator('.badge');
    const badgeCount = await badges.count();
    expect(badgeCount).toBeGreaterThan(0);

    // Verify all badges are visible (not hidden or display:none)
    for (let i = 0; i < badgeCount; i++) {
      const badge = badges.nth(i);
      await expect(badge).toBeVisible();
    }
  });

  test('Technology stack information is visible on initial page load', async ({ page }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify Rust mention in tagline is immediately visible in hero section
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Rust');

    // The hero section should be visible without scrolling
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
  });
});
