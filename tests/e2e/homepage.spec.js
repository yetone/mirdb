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

// Scenario 2: About Section Tests

test('about section has id="about" with semantic section element', async ({ page }) => {
  await page.goto('file://' + homepagePath);
  const about = await page.locator('section#about');
  await expect(about).toBeVisible();
  const tagName = await about.evaluate(el => el.tagName.toLowerCase());
  expect(tagName).toBe('section');
});

test('about section has h2 heading containing "About" or "What is MirDB"', async ({ page }) => {
  await page.goto('file://' + homepagePath);
  const h2 = await page.locator('#about h2');
  await expect(h2).toBeVisible();
  const text = await h2.textContent();
  expect(text.toLowerCase()).toMatch(/about|what is mirdb/);
});

test('about section description contains "persistent key-value store written in Rust"', async ({ page }) => {
  await page.goto('file://' + homepagePath);
  const about = await page.locator('#about');
  const text = await about.textContent();
  expect(text.toLowerCase()).toContain('persistent key-value store written in rust');
});

test('about section description contains "Memcached protocol"', async ({ page }) => {
  await page.goto('file://' + homepagePath);
  const about = await page.locator('#about');
  const text = await about.textContent();
  expect(text).toContain('Memcached protocol');
});

test('about section mentions "drop-in replacement" or "persistence" advantage', async ({ page }) => {
  await page.goto('file://' + homepagePath);
  const about = await page.locator('#about');
  const text = await about.textContent();
  const hasValueProp =
    text.toLowerCase().includes('drop-in replacement') ||
    text.toLowerCase().includes('persistence');
  expect(hasValueProp).toBe(true);
});
