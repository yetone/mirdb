/**
 * Header and Navigation E2E Tests
 * Owner: Scenario 1 - Homepage Header and Navigation
 *
 * Test coverage:
 * - Logo display and dimensions
 * - Navigation links (Features, Quick Start, GitHub)
 * - Navigation functionality
 * - Header visibility and styling
 */
import { test, expect } from '@playwright/test';
import { waitForPageLoad, checkImageLoads } from './test-utils';

test.describe('Homepage Header and Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Logo element exists with src pointing to logo.gif and appropriate alt text', async ({ page }) => {
    // Find the logo image
    const logo = page.locator('.logo');

    // Verify logo exists and is visible
    await expect(logo).toBeVisible();

    // Verify src attribute points to logo.gif
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Verify alt text is present and appropriate
    const alt = await logo.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt).toContain('MirDB');
  });

  test('TC2: Navigation contains Features link with href=#features and is visible', async ({ page }) => {
    // Find the Features navigation link
    const featuresLink = page.locator('nav a[href="#features"]');

    // Verify the link exists and is visible
    await expect(featuresLink).toBeVisible();

    // Verify the link text
    await expect(featuresLink).toContainText('Features');
  });

  test('TC3: Navigation contains Quick Start link with href=#quick-start and is visible', async ({ page }) => {
    // Find the Quick Start navigation link
    const quickStartLink = page.locator('nav a[href="#quick-start"]');

    // Verify the link exists and is visible
    await expect(quickStartLink).toBeVisible();

    // Verify the link text
    await expect(quickStartLink).toContainText('Quick Start');
  });

  test('TC4: Navigation contains GitHub link with correct href and target=_blank', async ({ page }) => {
    // Find the GitHub navigation link
    const githubLink = page.locator('nav a[href="https://github.com/yetone/mirdb"]');

    // Verify the link exists and is visible
    await expect(githubLink).toBeVisible();

    // Verify the link text
    await expect(githubLink).toContainText('GitHub');

    // Verify target="_blank" attribute
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC5: Logo image loads successfully and has reasonable dimensions (width > 50px)', async ({ page }) => {
    const logo = page.locator('.logo');

    // Wait for the logo to be visible
    await expect(logo).toBeVisible();

    // Check that the image loaded successfully
    const imageLoaded = await checkImageLoads(page, '.logo');
    expect(imageLoaded).toBe(true);

    // Verify the image has reasonable dimensions
    const boundingBox = await logo.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox!.width).toBeGreaterThan(50);
  });

  test('Header is visible at the top of the page', async ({ page }) => {
    const header = page.locator('header.header');

    // Verify header exists and is visible
    await expect(header).toBeVisible();

    // Verify header is at the top of the page
    const boundingBox = await header.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox!.y).toBe(0);
  });

  test('Navigation links are functional - Features link scrolls to section', async ({ page }) => {
    const featuresLink = page.locator('nav a[href="#features"]');

    // Click the Features link
    await featuresLink.click();

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify URL hash changed
    const url = page.url();
    expect(url).toContain('#features');
  });

  test('Navigation links are functional - Quick Start link scrolls to section', async ({ page }) => {
    const quickStartLink = page.locator('nav a[href="#quick-start"]');

    // Click the Quick Start link
    await quickStartLink.click();

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify URL hash changed
    const url = page.url();
    expect(url).toContain('#quick-start');
  });

  test('Header has proper semantic structure', async ({ page }) => {
    // Verify header has role="banner"
    const header = page.locator('header[role="banner"]');
    await expect(header).toBeVisible();

    // Verify nav has role="navigation"
    const nav = page.locator('nav[role="navigation"]');
    await expect(nav).toBeVisible();

    // Verify nav has aria-label
    const ariaLabel = await nav.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
  });
});
