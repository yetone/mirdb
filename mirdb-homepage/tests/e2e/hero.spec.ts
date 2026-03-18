/**
 * Hero Section E2E Tests.
 * Owner: Scenario 1 - Hero Section and Branding
 *
 * Tests:
 * - Logo presence and visibility
 * - H1 with MirDB name
 * - Tagline content
 * - Semantic HTML structure
 */

import { test, expect } from '@playwright/test';

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Logo element is present with appropriate alt text', async ({ page }) => {
    // Query for logo element
    const logo = page.locator('img[alt="MirDB Logo"]');

    // Verify logo is visible
    await expect(logo).toBeVisible();

    // Verify it has the correct src
    await expect(logo).toHaveAttribute('src', '/assets/logo.svg');
  });

  test('TC2: H1 heading contains MirDB text', async ({ page }) => {
    // Query for h1 heading
    const h1 = page.locator('h1');

    // Verify h1 is visible
    await expect(h1).toBeVisible();

    // Verify h1 contains 'MirDB' text
    await expect(h1).toContainText('MirDB');
  });

  test('TC3: Hero section contains tagline about Persistent Key-Value Store and Memcached Protocol', async ({ page }) => {
    // Query the hero section
    const hero = page.locator('#hero');

    // Verify hero section exists
    await expect(hero).toBeVisible();

    // Verify tagline contains 'Persistent Key-Value Store'
    const tagline = hero.locator('p').first();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify tagline contains 'Memcached Protocol'
    await expect(tagline).toContainText('Memcached Protocol');
  });

  test('TC4: Hero section uses proper semantic HTML elements', async ({ page }) => {
    // Verify hero section is wrapped in a header element
    const header = page.locator('header#hero');
    await expect(header).toBeVisible();

    // Verify h1 exists within the header
    const h1 = header.locator('h1');
    await expect(h1).toBeVisible();

    // Verify paragraph elements exist for tagline
    const paragraphs = header.locator('p');
    await expect(paragraphs.first()).toBeVisible();
  });
});
