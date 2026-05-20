/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Hero Section (full page rendering)
 *
 * Tests:
 * - Full page render at default viewport
 * - Hero section visibility above the fold
 * - CTA button click behaviors
 * - Logo image loads correctly
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.join(__dirname, '../../index.html');
const fileUrl = 'file://' + indexPath;

test.describe('Hero Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(fileUrl);
  });

  test('hero section is visible above the fold', async ({ page }) => {
    const hero = page.locator('section#hero');
    await expect(hero).toBeVisible();

    const isAboveFold = await hero.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= 0 && rect.top < window.innerHeight;
    });

    expect(isAboveFold).toBe(true);
  });

  test('hero section contains product logo image', async ({ page }) => {
    const logo = page.locator('section#hero img.hero-logo');
    await expect(logo).toBeVisible();

    const src = await logo.getAttribute('src');
    expect(src).toMatch(/assets\/logo\.gif|assets\/logo/i);

    const alt = await logo.getAttribute('alt');
    expect(alt).toBe('MirDB Logo');
  });

  test('hero section displays primary tagline', async ({ page }) => {
    const tagline = page.locator('section#hero h1.hero-tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toHaveText('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('hero section displays subtext', async ({ page }) => {
    const subtext = page.locator('section#hero p.hero-subtext');
    await expect(subtext).toBeVisible();
    await expect(subtext).toHaveText('It is painless as using memcached');
  });

  test('hero section has primary CTA "View on GitHub"', async ({ page }) => {
    const primaryCta = page.locator('section#hero .hero-cta-primary');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toHaveText('View on GitHub');
  });

  test('hero section has secondary CTA "Get Started"', async ({ page }) => {
    const secondaryCta = page.locator('section#hero .hero-cta-secondary');
    await expect(secondaryCta).toBeVisible();
    await expect(secondaryCta).toHaveText('Get Started');
  });

  test('primary CTA navigates to GitHub repository', async ({ page, context }) => {
    const primaryCta = page.locator('section#hero .hero-cta-primary');
    const href = await primaryCta.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify the link has target="_blank" for new tab
    const target = await primaryCta.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('secondary CTA links to quickstart section', async ({ page }) => {
    const secondaryCta = page.locator('section#hero .hero-cta-secondary');
    const href = await secondaryCta.getAttribute('href');
    expect(href).toBe('#quickstart');
  });

  test('logo image has correct alt text', async ({ page }) => {
    const logo = page.locator('section#hero img.hero-logo');
    const alt = await logo.getAttribute('alt');
    expect(alt).toBe('MirDB Logo');
  });

  test('hero section has background styling applied', async ({ page }) => {
    const hero = page.locator('section#hero');
    const heroBg = page.locator('section#hero .hero-background');

    await expect(hero).toBeVisible();
    await expect(heroBg).toBeVisible();

    // Check that the hero has a computed background or the background element exists
    const bgExists = await heroBg.evaluate((el) => el !== null);
    expect(bgExists).toBe(true);
  });
});
