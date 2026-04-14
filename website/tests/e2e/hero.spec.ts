/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for:
 * - Hero section visibility
 * - Logo and branding display
 * - Tagline and value proposition
 * - CTA button functionality
 */
import { test, expect } from '@playwright/test';
import { SELECTORS, CONTENT } from '../fixtures/test-data';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section is visible with MirDB logo/name displayed prominently', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator(SELECTORS.hero);
    await expect(heroSection).toBeVisible();

    // Verify MirDB name/title is displayed
    const heroTitle = page.locator(SELECTORS.heroTitle);
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText(CONTENT.productName);

    // Verify logo area is visible
    const heroLogo = page.locator(SELECTORS.heroLogo);
    await expect(heroLogo).toBeVisible();
  });

  test('TC2: Value proposition tagline is visible', async ({ page }) => {
    // Verify tagline is displayed
    const tagline = page.locator(SELECTORS.heroTagline);
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText(CONTENT.tagline);
  });

  test('TC3: Get Started button scrolls to Quick Start section', async ({ page }) => {
    // Click Get Started button
    const ctaButton = page.locator(SELECTORS.ctaGetStarted);
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toContainText(CONTENT.ctaGetStartedText);

    // Click and verify scroll to quick-start section
    await ctaButton.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify quick-start section is in view
    const quickStartSection = page.locator(SELECTORS.quickStart);
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC4: View on GitHub button opens repository in new tab', async ({ page, context }) => {
    // Get the GitHub button
    const githubButton = page.locator(SELECTORS.ctaGitHub);
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toContainText(CONTENT.ctaGitHubText);

    // Verify target="_blank" attribute
    await expect(githubButton).toHaveAttribute('target', '_blank');

    // Verify rel="noopener noreferrer" for security
    await expect(githubButton).toHaveAttribute('rel', /noopener/);

    // Verify href points to GitHub
    await expect(githubButton).toHaveAttribute('href', CONTENT.githubUrl);
  });

  test('TC5: Hero section contains all required elements', async ({ page }) => {
    const heroSection = page.locator(SELECTORS.hero);
    await expect(heroSection).toBeVisible();

    // Check all required elements exist
    await expect(page.locator(SELECTORS.heroLogo)).toBeVisible();
    await expect(page.locator(SELECTORS.heroTitle)).toBeVisible();
    await expect(page.locator(SELECTORS.heroTagline)).toBeVisible();
    await expect(page.locator(SELECTORS.ctaGetStarted)).toBeVisible();
    await expect(page.locator(SELECTORS.ctaGitHub)).toBeVisible();

    // Verify hero is above the fold (visible without scrolling)
    await expect(heroSection).toBeInViewport();
  });
});
