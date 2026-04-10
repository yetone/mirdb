/**
 * E2E tests for Hero Section and Branding Display
 *
 * Owner: Scenario 1 - Hero Section and Branding Display
 *
 * Tests:
 * - Homepage loads with hero section
 * - MirDB branding and tagline are displayed
 * - Navigation links work correctly
 * - CTA buttons are functional
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section and Branding Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('displays MirDB branding and tagline', async ({ page }) => {
    // Check page title
    await expect(page).toHaveTitle(/MirDB/);

    // Check hero section title
    const heroTitle = page.locator('h1');
    await expect(heroTitle).toContainText('MirDB');

    // Check tagline
    const tagline = page.getByText('Persistent Key-Value Store with Memcached Protocol');
    await expect(tagline).toBeVisible();
  });

  test('displays value proposition and features', async ({ page }) => {
    // Check value proposition text
    const description = page.getByText(/combines the simplicity and speed of memcached/i);
    await expect(description).toBeVisible();

    // Check feature highlights
    await expect(page.getByText('Lightning Fast')).toBeVisible();
    await expect(page.getByText('Persistent Storage')).toBeVisible();
    await expect(page.getByText('Drop-in Replacement')).toBeVisible();
  });

  test('navigation links are present and functional', async ({ page }) => {
    // Check all navigation links are visible
    await expect(page.getByRole('link', { name: 'Dashboard' })).toBeVisible();
    await expect(page.getByRole('link', { name: 'Browser' })).toBeVisible();
    await expect(page.getByRole('link', { name: 'Config' })).toBeVisible();
    await expect(page.getByRole('link', { name: 'Docs' })).toBeVisible();

    // Test Dashboard navigation
    await page.getByRole('link', { name: 'Dashboard' }).click();
    await expect(page).toHaveURL('/dashboard');
    await expect(page.locator('h1')).toContainText('Dashboard');

    // Navigate back and test Browser
    await page.goto('/');
    await page.getByRole('link', { name: 'Browser' }).click();
    await expect(page).toHaveURL('/browser');
    await expect(page.locator('h1')).toContainText('Key-Value Browser');

    // Navigate back and test Config
    await page.goto('/');
    await page.getByRole('link', { name: 'Config' }).click();
    await expect(page).toHaveURL('/config');
    await expect(page.locator('h1')).toContainText('Configuration');

    // Navigate back and test Docs
    await page.goto('/');
    await page.getByRole('link', { name: 'Docs' }).click();
    await expect(page).toHaveURL('/docs');
    await expect(page.locator('h1')).toContainText('Documentation');
  });

  test('Quick Start Guide button navigates to docs', async ({ page }) => {
    const quickStartButton = page.getByRole('link', { name: /quick start guide/i });
    await expect(quickStartButton).toBeVisible();

    await quickStartButton.click();
    await expect(page).toHaveURL('/docs');
  });

  test('GitHub Repository button has correct link', async ({ page }) => {
    const githubButton = page.getByRole('link', { name: /github repository/i });
    await expect(githubButton).toBeVisible();
    await expect(githubButton).toHaveAttribute('href', 'https://github.com/mirdb/mirdb');
    await expect(githubButton).toHaveAttribute('target', '_blank');
  });

  test('logo link navigates to homepage', async ({ page }) => {
    // First navigate away from homepage
    await page.getByRole('link', { name: 'Dashboard' }).click();
    await expect(page).toHaveURL('/dashboard');

    // Click logo to go back home
    await page.getByRole('link', { name: /mirdb home/i }).click();
    await expect(page).toHaveURL('/');
  });
});
