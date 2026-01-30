/**
 * MirDB Landing Page - Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests verify:
 * - Hero section is visible with logo
 * - Tagline text contains required keywords
 * - CTA buttons are present and functional
 * - Value proposition mentions drop-in replacement
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  // Test Case 1: Hero section is visible with logo.gif image displayed
  test('TC1: Hero section is visible with logo.gif image displayed', async ({ page }) => {
    // Verify hero section exists and is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify logo image is displayed
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Verify logo source contains logo.gif
    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');

    // Verify logo has proper alt text for accessibility
    const logoAlt = await logo.getAttribute('alt');
    expect(logoAlt).toBeTruthy();
  });

  // Test Case 2: Text contains 'Persistent Key-Value Store' and 'Memcached Protocol'
  test('TC2: Tagline contains required keywords', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Check for tagline text
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();

    // Verify tagline contains required keywords
    expect(taglineText).toContain('Persistent Key-Value Store');
    expect(taglineText).toContain('Memcached Protocol');
  });

  // Test Case 3: Two buttons present: 'Get Started' and 'View on GitHub'
  test('TC3: CTA buttons are present', async ({ page }) => {
    const heroSection = page.locator('#hero');
    const ctaSection = heroSection.locator('.hero-cta');

    // Verify CTA section exists
    await expect(ctaSection).toBeVisible();

    // Find Get Started button
    const getStartedBtn = ctaSection.locator('a', { hasText: 'Get Started' });
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveClass(/btn/);

    // Find View on GitHub button
    const githubBtn = ctaSection.locator('a', { hasText: 'View on GitHub' });
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveClass(/btn/);

    // Verify both buttons are clickable (have href)
    await expect(getStartedBtn).toHaveAttribute('href');
    await expect(githubBtn).toHaveAttribute('href');
  });

  // Test Case 4: View on GitHub button links to correct URL
  test('TC4: View on GitHub button links to correct URL', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Find the GitHub button
    const githubBtn = heroSection.locator('a', { hasText: 'View on GitHub' });
    await expect(githubBtn).toBeVisible();

    // Verify it links to the correct GitHub repository
    const href = await githubBtn.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in new tab (security best practice for external links)
    await expect(githubBtn).toHaveAttribute('target', '_blank');
    await expect(githubBtn).toHaveAttribute('rel', /noopener/);
  });

  // Test Case 5: Hero section mentions 'drop-in memcached replacement' capability
  test('TC5: Value proposition mentions drop-in memcached replacement', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Check for value proposition text
    const valueProposition = heroSection.locator('.hero-value-proposition');
    await expect(valueProposition).toBeVisible();

    const valueText = await valueProposition.textContent();

    // Verify it mentions drop-in memcached replacement
    expect(valueText.toLowerCase()).toContain('drop-in');
    expect(valueText.toLowerCase()).toContain('memcached');
    expect(valueText.toLowerCase()).toContain('replacement');
  });

  // Additional test: Hero section accessibility
  test('Hero section has proper accessibility attributes', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Verify section has aria-labelledby
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-title');

    // Verify heading exists
    const heroTitle = page.locator('#hero-title');
    await expect(heroTitle).toBeVisible();

    // Verify logo has alt text
    const logo = page.locator('.hero-logo');
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);
  });
});
