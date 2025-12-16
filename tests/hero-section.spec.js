// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const INDEX_PATH = 'file://' + path.join(process.cwd(), 'index.html');

/**
 * Test Case 1: Hero section contains h1 with MirDB name and tagline mentioning 'key-value store' or 'Memcached'
 */
test('hero section contains MirDB branding and value proposition tagline', async ({ page }) => {
  await page.goto(INDEX_PATH);

  // Verify hero section exists
  const heroSection = page.locator('[data-testid="hero-section"], .hero, #hero, section.hero');
  await expect(heroSection).toBeVisible();

  // Verify h1 contains MirDB name
  const h1 = heroSection.locator('h1');
  await expect(h1).toBeVisible();
  await expect(h1).toContainText('MirDB');

  // Verify tagline mentions 'key-value store' or 'Memcached'
  const heroText = await heroSection.textContent();
  const hasKeyValueStore = heroText.toLowerCase().includes('key-value store');
  const hasMemcached = heroText.toLowerCase().includes('memcached');
  expect(hasKeyValueStore || hasMemcached).toBeTruthy();
});

/**
 * Test Case 2: Primary CTA button is present with valid href
 */
test('primary CTA button (Get Started/GitHub) is present with valid href', async ({ page }) => {
  await page.goto(INDEX_PATH);

  const heroSection = page.locator('[data-testid="hero-section"], .hero, #hero, section.hero');
  await expect(heroSection).toBeVisible();

  // Look for primary CTA button with "Get Started" or "View on GitHub" text
  const primaryCta = heroSection.locator('a.btn-primary, a.cta-primary, a[data-testid="primary-cta"]').first();
  await expect(primaryCta).toBeVisible();

  // Verify button text
  const buttonText = await primaryCta.textContent();
  const hasGetStarted = buttonText.toLowerCase().includes('get started');
  const hasGitHub = buttonText.toLowerCase().includes('github');
  expect(hasGetStarted || hasGitHub).toBeTruthy();

  // Verify href is present and valid
  const href = await primaryCta.getAttribute('href');
  expect(href).toBeTruthy();
  expect(href.length).toBeGreaterThan(0);
});

/**
 * Test Case 3: Secondary CTA button (Documentation) is present with valid href
 */
test('secondary CTA button (Documentation) is present with valid href', async ({ page }) => {
  await page.goto(INDEX_PATH);

  const heroSection = page.locator('[data-testid="hero-section"], .hero, #hero, section.hero');
  await expect(heroSection).toBeVisible();

  // Look for secondary CTA button with "Documentation" text
  const secondaryCta = heroSection.locator('a.btn-secondary, a.cta-secondary, a[data-testid="secondary-cta"]').first();
  await expect(secondaryCta).toBeVisible();

  // Verify button text contains documentation-related text
  const buttonText = await secondaryCta.textContent();
  const hasDocumentation = buttonText.toLowerCase().includes('documentation') ||
                           buttonText.toLowerCase().includes('docs') ||
                           buttonText.toLowerCase().includes('read');
  expect(hasDocumentation).toBeTruthy();

  // Verify href is present and valid
  const href = await secondaryCta.getAttribute('href');
  expect(href).toBeTruthy();
  expect(href.length).toBeGreaterThan(0);
});

/**
 * Test Case 4: Hero section value proposition text mentions Memcached compatibility, persistence, and/or Rust performance
 */
test('hero section contains value proposition text about features', async ({ page }) => {
  await page.goto(INDEX_PATH);

  const heroSection = page.locator('[data-testid="hero-section"], .hero, #hero, section.hero');
  await expect(heroSection).toBeVisible();

  // Get all text content from hero section
  const heroText = await heroSection.textContent();
  const heroTextLower = heroText.toLowerCase();

  // Check for value proposition keywords
  const hasMemcachedCompatibility = heroTextLower.includes('memcached');
  const hasPersistence = heroTextLower.includes('persistent') || heroTextLower.includes('persistence');
  const hasRustPerformance = heroTextLower.includes('rust') || heroTextLower.includes('performance') || heroTextLower.includes('high-performance');

  // At least one of these value propositions should be present
  expect(hasMemcachedCompatibility || hasPersistence || hasRustPerformance).toBeTruthy();
});
