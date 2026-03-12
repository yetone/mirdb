/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Logo (logo.gif) displays prominently
 * - Project title "MirDB: A Persistent Key-Value Store" visible
 * - Usage demo (usage.gif) loads and displays
 * - "Get Started" CTA button present and functional
 * - "View Source Code" button present and links to GitHub
 *
 * Traceability: REQ-1, REQ-2, REQ-3
 */

import { test, expect } from '@playwright/test';
import { VIEWPORTS, waitForPageLoad, getTestIdLocator, waitForImage, getByTestId } from './test-utils';

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/homepage/index.html');
    await waitForPageLoad(page);
  });

  test('TC1: Logo image (logo.gif) is visible and loads without errors', async ({ page }) => {
    const logo = getTestIdLocator(page, 'hero-logo');

    // Check logo is visible
    await expect(logo).toBeVisible();

    // Check logo has correct src attribute containing logo.gif
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Wait for image to fully load
    await waitForImage(page, getByTestId('hero-logo'));

    // Verify image loaded successfully (naturalHeight > 0)
    const isLoaded = await page.evaluate(() => {
      const img = document.querySelector('[data-testid="hero-logo"]') as HTMLImageElement;
      return img && img.complete && img.naturalHeight > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('TC2: H1 element contains MirDB and includes tagline about persistent key-value store', async ({ page }) => {
    const title = getTestIdLocator(page, 'hero-title');

    // Check title is visible
    await expect(title).toBeVisible();

    // Check title is an H1 element
    const tagName = await title.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h1');

    // Check title contains 'MirDB'
    await expect(title).toContainText('MirDB');

    // Check title includes tagline about persistent key-value store
    await expect(title).toContainText('Persistent Key-Value Store');
  });

  test('TC3: usage.gif image is present, has correct src attribute, and loads successfully', async ({ page }) => {
    const usageGif = getTestIdLocator(page, 'usage-gif');

    // Check usage gif is visible
    await expect(usageGif).toBeVisible();

    // Check usage gif has correct src attribute
    const src = await usageGif.getAttribute('src');
    expect(src).toContain('usage.gif');

    // Wait for image to fully load
    await waitForImage(page, getByTestId('usage-gif'));

    // Verify image loaded successfully
    const isLoaded = await page.evaluate(() => {
      const img = document.querySelector('[data-testid="usage-gif"]') as HTMLImageElement;
      return img && img.complete && img.naturalHeight > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('TC4: Get Started button is visible and links to README or documentation', async ({ page }) => {
    const getStartedBtn = getTestIdLocator(page, 'get-started-btn');

    // Check button is visible
    await expect(getStartedBtn).toBeVisible();

    // Check button text
    await expect(getStartedBtn).toContainText('Get Started');

    // Check button has appropriate link (README or getting started)
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href?.toLowerCase()).toContain('readme');

    // Check button has styling (btn class)
    const classes = await getStartedBtn.getAttribute('class');
    expect(classes).toContain('btn');
    expect(classes).toContain('btn-primary');

    // Check opens in new tab
    const target = await getStartedBtn.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC5: View Source Code button is visible and links to GitHub repository', async ({ page }) => {
    const viewSourceBtn = getTestIdLocator(page, 'view-source-btn');

    // Check button is visible
    await expect(viewSourceBtn).toBeVisible();

    // Check button text
    await expect(viewSourceBtn).toContainText('View Source Code');

    // Check button links to GitHub
    const href = await viewSourceBtn.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href?.toLowerCase()).toContain('github');

    // Check button has styling
    const classes = await viewSourceBtn.getAttribute('class');
    expect(classes).toContain('btn');

    // Check opens in new tab
    const target = await viewSourceBtn.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC6: Hero section is above the fold on 1920x1080 viewport', async ({ page }) => {
    // Set viewport to 1920x1080
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/homepage/index.html');
    await waitForPageLoad(page);

    // Check logo is in viewport without scrolling
    const logo = getTestIdLocator(page, 'hero-logo');
    await expect(logo).toBeInViewport();

    // Check title is in viewport without scrolling
    const title = getTestIdLocator(page, 'hero-title');
    await expect(title).toBeInViewport();

    // Check primary CTA (Get Started button) is in viewport without scrolling
    const getStartedBtn = getTestIdLocator(page, 'get-started-btn');
    await expect(getStartedBtn).toBeInViewport();

    // Verify we haven't scrolled
    const scrollPosition = await page.evaluate(() => window.scrollY);
    expect(scrollPosition).toBe(0);
  });

  test('Hero section has proper accessibility attributes', async ({ page }) => {
    // Check hero section exists with proper structure
    const heroSection = getTestIdLocator(page, 'hero-section');
    await expect(heroSection).toBeVisible();

    // Check logo has alt text
    const logo = getTestIdLocator(page, 'hero-logo');
    const logoAlt = await logo.getAttribute('alt');
    expect(logoAlt).toBeTruthy();
    expect(logoAlt!.length).toBeGreaterThan(0);

    // Check usage gif has alt text
    const usageGif = getTestIdLocator(page, 'usage-gif');
    const usageAlt = await usageGif.getAttribute('alt');
    expect(usageAlt).toBeTruthy();
    expect(usageAlt!.length).toBeGreaterThan(0);

    // Check buttons have proper accessibility
    const buttons = page.locator('.hero-cta .btn');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(2);

    // Check buttons have rel="noopener noreferrer" for security
    const getStartedBtn = getTestIdLocator(page, 'get-started-btn');
    const rel = await getStartedBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
