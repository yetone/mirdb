import { test, expect } from '@playwright/test';

test.describe('Navigation Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation menu with links to all major sections is present and visible', async ({ page }) => {
    // Check for presence of navigation menu
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Check nav-links container exists
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify all major section links are present
    const featuresLink = navLinks.locator('a[href="#features"]');
    const gettingStartedLink = navLinks.locator('a[href="#getting-started"]');
    const configurationLink = navLinks.locator('a[href="#configuration"]');

    await expect(featuresLink).toBeVisible();
    await expect(gettingStartedLink).toBeVisible();
    await expect(configurationLink).toBeVisible();

    // Verify link text content
    await expect(featuresLink).toHaveText('Features');
    await expect(gettingStartedLink).toHaveText('Getting Started');
    await expect(configurationLink).toHaveText('Configuration');
  });

  test('TC2: Click Features navigation link scrolls to Features section', async ({ page }) => {
    // Click the Features navigation link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(800);

    // Verify the Features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify URL hash changed
    await expect(page).toHaveURL(/#features/);
  });

  test('TC3: Click Getting Started navigation link scrolls to Getting Started section', async ({ page }) => {
    // Click the Getting Started navigation link
    const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
    await gettingStartedLink.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(800);

    // Verify the Getting Started section is in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify URL hash changed
    await expect(page).toHaveURL(/#getting-started/);
  });

  test('TC4: Click Configuration navigation link scrolls to Configuration section', async ({ page }) => {
    // Click the Configuration navigation link
    const configurationLink = page.locator('.nav-links a[href="#configuration"]');
    await configurationLink.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(800);

    // Verify the Configuration section is in viewport
    const configurationSection = page.locator('#configuration');
    await expect(configurationSection).toBeInViewport();

    // Verify URL hash changed
    await expect(page).toHaveURL(/#configuration/);
  });

  test('TC5: Current section is visually highlighted in the navigation menu', async ({ page }) => {
    // First, scroll to the Features section
    await page.locator('.nav-links a[href="#features"]').click();
    await page.waitForTimeout(800);

    // Check that Features link has active class
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toHaveClass(/active/);

    // Now scroll to Getting Started section
    await page.locator('.nav-links a[href="#getting-started"]').click();
    await page.waitForTimeout(800);

    // Check that Getting Started link has active class
    const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
    await expect(gettingStartedLink).toHaveClass(/active/);

    // Features link should no longer be active
    await expect(featuresLink).not.toHaveClass(/active/);
  });

  test('Navigation is fixed and remains visible when scrolling', async ({ page }) => {
    // First scroll down to make sure we're not at the top
    await page.evaluate(() => window.scrollTo(0, 1000));
    await page.waitForTimeout(300);

    // Verify navigation is still visible
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeInViewport();

    // Verify navigation has fixed positioning
    const position = await navbar.evaluate(el => getComputedStyle(el).position);
    expect(position).toBe('fixed');
  });

  test('Navigation brand logo links to top of page', async ({ page }) => {
    // First scroll down
    await page.evaluate(() => window.scrollTo(0, 1000));
    await page.waitForTimeout(300);

    // Click the brand link
    const brandLink = page.locator('.nav-brand');
    await brandLink.click();
    await page.waitForTimeout(800);

    // Verify page scrolled to top (hero section is in viewport)
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();
  });
});
