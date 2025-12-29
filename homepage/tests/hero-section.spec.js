// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.join(__dirname, '..', 'index.html');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test('TC1: Product name MirDB is displayed prominently', async ({ page }) => {
    // Load homepage and inspect hero section
    const heroSection = page.locator('.hero, [class*="hero"], header, section').first();
    await expect(heroSection).toBeVisible();

    // Check that product name 'MirDB' is displayed prominently
    const productName = page.locator('h1, .product-name, [class*="title"]').filter({ hasText: 'MirDB' });
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');
  });

  test('TC2: Tagline contains Persistent Key-Value Store with Memcached Protocol', async ({ page }) => {
    // Check hero tagline text content
    const tagline = page.locator('.tagline, .subtitle, h2, p').filter({
      hasText: /Persistent Key-Value Store with Memcached Protocol/i
    });
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store with Memcached Protocol');
  });

  test('TC3: Get Started button is visible and has correct styling', async ({ page }) => {
    // Locate and verify primary CTA button
    const getStartedBtn = page.locator('a, button').filter({ hasText: /Get Started/i });
    await expect(getStartedBtn).toBeVisible();

    // Verify button has appropriate styling (primary button characteristics)
    const btnClasses = await getStartedBtn.getAttribute('class');
    const isStyledButton = btnClasses?.includes('primary') ||
                           btnClasses?.includes('btn') ||
                           btnClasses?.includes('cta') ||
                           await getStartedBtn.evaluate(el => {
                             const styles = window.getComputedStyle(el);
                             return styles.backgroundColor !== 'transparent' &&
                                    styles.backgroundColor !== 'rgba(0, 0, 0, 0)';
                           });
    expect(isStyledButton).toBeTruthy();
  });

  test('TC4: View Documentation button is visible and links to documentation', async ({ page }) => {
    // Locate and verify secondary CTA button in the hero section
    const heroSection = page.locator('.hero');
    const docBtn = heroSection.locator('a.btn').filter({ hasText: /View Documentation/i });
    await expect(docBtn).toBeVisible();

    // Verify it has an href attribute (links to documentation)
    const href = await docBtn.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href?.toLowerCase()).toMatch(/doc|readme|github/i);
  });

  test('TC5: Logo image is rendered with appropriate alt text', async ({ page }) => {
    // Check for product logo presence
    const logo = page.locator('img.logo, img[alt*="MirDB"], img[alt*="logo"], .logo img, header img').first();
    await expect(logo).toBeVisible();

    // Verify alt text is present and appropriate
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText?.toLowerCase()).toMatch(/mirdb|logo/i);
  });
});
