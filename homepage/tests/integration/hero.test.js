/**
 * Hero Section Integration Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases:
 * - Hero renders with correct headline
 * - Subheadline contains key value proposition
 * - Get Started button scrolls to Quick Start
 * - View on GitHub button links to repository
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero renders with correct headline', async ({ page }) => {
    // Check hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check headline text
    const headline = page.locator('.hero-headline');
    await expect(headline).toContainText('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('TC2: Subheadline contains key value proposition', async ({ page }) => {
    const subheadline = page.locator('.hero-subheadline');
    await expect(subheadline).toContainText('Drop-in memcached replacement with disk persistence. Built with Rust.');
  });

  test('TC3: Get Started button scrolls to Quick Start section', async ({ page }) => {
    // Click Get Started button
    const getStartedBtn = page.locator('#cta-get-started');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Click and verify smooth scroll to quickstart section
    await getStartedBtn.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Check that the quickstart section is now in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('TC4: View on GitHub button opens repository in new tab', async ({ page }) => {
    const githubBtn = page.locator('#cta-github');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveText('View on GitHub');

    // Check that it has target="_blank" for new tab
    await expect(githubBtn).toHaveAttribute('target', '_blank');

    // Check that it has the correct href pointing to GitHub
    await expect(githubBtn).toHaveAttribute('href', /github\.com/);

    // Check rel attribute for security
    await expect(githubBtn).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Hero section is visible above the fold', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();
  });

  test('MirDB project name is prominently displayed', async ({ page }) => {
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toContainText('MirDB');
  });

  test('CTA buttons are styled correctly', async ({ page }) => {
    // Primary button (Get Started)
    const primaryBtn = page.locator('#cta-get-started');
    await expect(primaryBtn).toHaveClass(/btn-primary/);

    // Secondary button (View on GitHub)
    const secondaryBtn = page.locator('#cta-github');
    await expect(secondaryBtn).toHaveClass(/btn-secondary/);
  });
});
