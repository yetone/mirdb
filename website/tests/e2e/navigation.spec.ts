import { test, expect } from '@playwright/test';

test.describe('Navigation and Smooth Scrolling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Click Quick Start navigation link and verify smooth scroll', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Find and click the Quick Start nav link
    const quickStartLink = page.locator('nav.nav a[href="#quickstart"]').first();
    await expect(quickStartLink).toBeVisible();
    await quickStartLink.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Verify scroll position changed (scrolled down to quickstart section)
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify the quickstart section is now in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify URL hash is updated
    await expect(page).toHaveURL(/#quickstart$/);
  });

  test('TC2: Click Features navigation link and verify smooth scroll', async ({ page }) => {
    // Scroll to bottom first to ensure we need to scroll up or that we clearly move
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(100);

    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Find and click the Features nav link
    const featuresLink = page.locator('nav.nav a[href="#features"]').first();
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500);

    // Verify the features section is now in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify URL hash is updated
    await expect(page).toHaveURL(/#features$/);
  });

  test('TC3: CTA button shows hover state visual feedback', async ({ page }) => {
    // Find the primary CTA button in hero section
    const ctaButton = page.locator('.hero__cta.btn--primary').first();
    await expect(ctaButton).toBeVisible();

    // Get initial computed styles
    const initialStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        boxShadow: styles.boxShadow,
        transform: styles.transform
      };
    });

    // Hover over the button
    await ctaButton.hover();

    // Wait for CSS transition
    await page.waitForTimeout(200);

    // Get styles after hover
    const hoverStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        boxShadow: styles.boxShadow,
        transform: styles.transform
      };
    });

    // Verify at least one visual property changed (color, shadow, or transform)
    const hasVisualChange =
      hoverStyles.backgroundColor !== initialStyles.backgroundColor ||
      hoverStyles.boxShadow !== initialStyles.boxShadow ||
      hoverStyles.transform !== initialStyles.transform;

    expect(hasVisualChange).toBe(true);
  });

  test('TC4: Navigation links show hover state visual feedback', async ({ page }) => {
    // Find a navigation link in the nav bar
    const navLink = page.locator('.nav__links a').first();
    await expect(navLink).toBeVisible();

    // Get initial color
    const initialColor = await navLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Hover over the link
    await navLink.hover();

    // Wait for CSS transition
    await page.waitForTimeout(200);

    // Get color after hover
    const hoverColor = await navLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Verify color changed (hover state)
    expect(hoverColor).not.toBe(initialColor);
  });
});
