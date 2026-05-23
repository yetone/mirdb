/**
 * E2E tests for homepage content and structure.
 * Owner: All content scenarios (1-8)
 *
 * Tests:
 * - Each section renders correctly
 * - Content matches PRD requirements
 * - Links are valid and point to correct URLs
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepagePath = path.resolve(__dirname, '../../index.html');

// Scenario 1: Hero Section Tests

test('hero section displays product name "MirDB" in h1', async ({ page }) => {
  await page.goto('file://' + homepagePath);
  const h1 = await page.locator('#hero h1');
  await expect(h1).toBeVisible();
  await expect(h1).toHaveText('MirDB');
});

test('hero section displays tagline', async ({ page }) => {
  await page.goto('file://' + homepagePath);
  const tagline = await page.locator('#hero .tagline');
  await expect(tagline).toBeVisible();
  await expect(tagline).toHaveText('A Persistent Key-Value Store with Memcached Protocol');
});

test('hero section has "Get Started" CTA button linking to #quick-start', async ({ page }) => {
  await page.goto('file://' + homepagePath);
  const cta = await page.locator('#hero .cta-button');
  await expect(cta).toBeVisible();
  await expect(cta).toHaveText('Get Started');
  await expect(cta).toHaveAttribute('href', '#quick-start');
});
