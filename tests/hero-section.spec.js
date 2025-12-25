// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Test Case 1: Product name 'MirDB' is visible and prominently styled
test('TC1: Product name MirDB is visible and prominently styled', async ({ page }) => {
  // Navigate to homepage
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  // Check that the hero section exists
  const heroSection = page.locator('[data-testid="hero-section"]');
  await expect(heroSection).toBeVisible();

  // Check product name is visible
  const productName = page.locator('[data-testid="product-name"]');
  await expect(productName).toBeVisible();
  await expect(productName).toHaveText('MirDB');

  // Verify product name is prominently styled (h1 tag with large font)
  const tagName = await productName.evaluate(el => el.tagName.toLowerCase());
  expect(tagName).toBe('h1');

  // Check that the product name has the hero-logo class indicating prominent styling
  await expect(productName).toHaveClass(/hero-logo/);
});

// Test Case 2: Tagline text matches 'Persistent Key-Value Store with Memcached Protocol'
test('TC2: Tagline text matches expected value', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  const tagline = page.locator('[data-testid="hero-tagline"]');
  await expect(tagline).toBeVisible();
  await expect(tagline).toHaveText('Persistent Key-Value Store with Memcached Protocol');
});

// Test Case 3: 1-2 sentence description explaining MirDB's purpose is present
test('TC3: Brief description explaining MirDB purpose is present', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  const description = page.locator('[data-testid="hero-description"]');
  await expect(description).toBeVisible();

  // Get the description text and verify it's not empty
  const descriptionText = await description.textContent();
  expect(descriptionText).toBeTruthy();
  expect(descriptionText.trim().length).toBeGreaterThan(0);

  // Verify description contains relevant keywords about MirDB's purpose
  const normalizedText = descriptionText.toLowerCase();
  expect(
    normalizedText.includes('key-value') ||
    normalizedText.includes('persistent') ||
    normalizedText.includes('memcached') ||
    normalizedText.includes('rust')
  ).toBeTruthy();

  // Verify it's a reasonable length (1-2 sentences, roughly 50-300 characters)
  expect(descriptionText.trim().length).toBeGreaterThanOrEqual(50);
  expect(descriptionText.trim().length).toBeLessThanOrEqual(500);
});

// Test Case 4: Primary CTA navigates to Get Started section or GitHub repository
test('TC4: Primary CTA button navigates to GitHub repository', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  const primaryCTA = page.locator('[data-testid="primary-cta"]');
  await expect(primaryCTA).toBeVisible();

  // Check button text contains expected content
  const buttonText = await primaryCTA.textContent();
  expect(
    buttonText.toLowerCase().includes('github') ||
    buttonText.toLowerCase().includes('get started')
  ).toBeTruthy();

  // Check that the button has the correct href to GitHub
  const href = await primaryCTA.getAttribute('href');
  expect(href).toContain('github.com');
  expect(href).toContain('mirdb');

  // Verify it's a link element
  const tagName = await primaryCTA.evaluate(el => el.tagName.toLowerCase());
  expect(tagName).toBe('a');

  // Verify the button has primary styling
  await expect(primaryCTA).toHaveClass(/btn-primary/);
});

// Test Case 5: Secondary CTA navigates to documentation link
test('TC5: Secondary CTA button navigates to documentation', async ({ page }) => {
  await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

  const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
  await expect(secondaryCTA).toBeVisible();

  // Check button text contains documentation reference
  const buttonText = await secondaryCTA.textContent();
  expect(buttonText.toLowerCase()).toContain('documentation');

  // Check that the button has a valid href
  const href = await secondaryCTA.getAttribute('href');
  expect(href).toBeTruthy();
  expect(href.length).toBeGreaterThan(0);

  // Verify it's a link element
  const tagName = await secondaryCTA.evaluate(el => el.tagName.toLowerCase());
  expect(tagName).toBe('a');

  // Verify the button has secondary styling
  await expect(secondaryCTA).toHaveClass(/btn-secondary/);
});
