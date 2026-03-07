/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test Case 4: Visual inspection of hero section
 * - Hero section has full-width gradient background with centered content
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Hero Section Visual Tests (Scenario 1)', () => {
  test.beforeEach(async ({ page }) => {
    const indexPath = path.join(__dirname, '..', '..', 'index.html');
    await page.goto(`file://${indexPath}`);
  });

  test('Test Case 4: Hero section has full-width gradient background with centered content', async ({ page }) => {
    // Verify hero section exists
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check that hero section spans full width
    const heroBox = await hero.boundingBox();
    const viewportSize = page.viewportSize();
    expect(heroBox.width).toBeGreaterThanOrEqual(viewportSize.width * 0.99);

    // Check that hero has a background (gradient)
    const heroStyles = await hero.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        background: styles.background,
        backgroundImage: styles.backgroundImage,
        textAlign: styles.textAlign
      };
    });

    // Verify gradient background exists
    expect(heroStyles.backgroundImage).toContain('gradient');

    // Verify centered content
    expect(heroStyles.textAlign).toBe('center');

    // Verify hero container content is centered using flexbox
    const container = page.locator('.hero__container');
    await expect(container).toBeVisible();

    const containerStyles = await container.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        flexDirection: styles.flexDirection,
        alignItems: styles.alignItems
      };
    });

    expect(containerStyles.display).toBe('flex');
    expect(containerStyles.flexDirection).toBe('column');
    expect(containerStyles.alignItems).toBe('center');
  });

  test('Hero section displays all required elements', async ({ page }) => {
    // Logo
    const logo = page.locator('.hero__logo');
    await expect(logo).toBeVisible();

    // Title
    const title = page.locator('.hero__title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('MirDB');

    // Tagline
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('A Persistent Key-Value Store with Memcached Protocol');

    // Description
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();
  });

  test('Hero section has appropriate height', async ({ page }) => {
    const hero = page.locator('.hero');
    const heroBox = await hero.boundingBox();

    // Hero should have significant height (at least 200px)
    expect(heroBox.height).toBeGreaterThanOrEqual(200);
  });
});
