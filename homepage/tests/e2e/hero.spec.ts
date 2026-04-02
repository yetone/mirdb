/**
 * E2E tests for Hero Section Display
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test Cases:
 * 1. MirDB logo/branding element is visible
 * 2. Headline contains 'MirDB' and communicates key-value store purpose
 * 3. Tagline mentions 'Memcached protocol' and 'persistence'
 * 4. CTA button is clickable and navigates to documentation or GitHub
 */
import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: MirDB logo/branding element is visible', async ({ page }) => {
    // Verify the hero section exists
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify the logo is visible
    const logo = page.getByTestId('hero-logo');
    await expect(logo).toBeVisible();

    // Verify the logo contains branding (the "M" for MirDB)
    await expect(logo).toContainText('M');
  });

  test('TC2: Headline contains MirDB and communicates key-value store purpose', async ({ page }) => {
    const headline = page.getByTestId('hero-headline');
    await expect(headline).toBeVisible();

    // Get the headline text
    const headlineText = await headline.textContent();

    // Verify it contains 'MirDB'
    expect(headlineText).toContain('MirDB');

    // Verify it communicates key-value store purpose
    expect(headlineText?.toLowerCase()).toMatch(/key-value|key value/);
  });

  test('TC3: Tagline mentions Memcached protocol and persistence', async ({ page }) => {
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toBeVisible();

    // Get the tagline text
    const taglineText = await tagline.textContent();

    // Verify it mentions Memcached
    expect(taglineText?.toLowerCase()).toContain('memcached');

    // Verify it mentions persistence
    expect(taglineText?.toLowerCase()).toContain('persistence');
  });

  test('TC4: CTA button is clickable and navigates to GitHub', async ({ page, context }) => {
    const ctaButton = page.getByTestId('hero-cta');
    await expect(ctaButton).toBeVisible();

    // Verify the button text
    await expect(ctaButton).toContainText('Get Started');

    // Get the href attribute
    const href = await ctaButton.getAttribute('href');

    // Verify it links to GitHub (external link)
    expect(href).toContain('github.com');

    // Verify it opens in a new tab (has target="_blank")
    const target = await ctaButton.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has proper security attributes
    const rel = await ctaButton.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
