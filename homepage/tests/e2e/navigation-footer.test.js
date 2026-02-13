/**
 * E2E Tests for Navigation and Footer
 * Owner: Scenario 8 - Navigation and Footer
 *
 * Tests user interactions, smooth scrolling, and external link behavior
 * Test Cases: 3, 4, 8
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 3: Click GitHub link in header opens correct repository in new tab', async ({ page, context }) => {
    // Find the GitHub link in header navigation
    const headerGithubLink = page.locator('header .nav__link--github, header .nav__links a[href*="github.com/yetone/mirdb"]').first();
    await expect(headerGithubLink).toBeVisible();

    // Listen for new page (tab) to open
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      headerGithubLink.click()
    ]);

    // Wait for the new page to load
    await newPage.waitForLoadState('domcontentloaded');

    // Verify the new tab URL is the GitHub repository
    const newUrl = newPage.url();
    expect(newUrl).toContain('github.com/yetone/mirdb');

    // Close the new tab
    await newPage.close();
  });

  test('Test Case 4: Click navigation link to features smoothly scrolls to features section', async ({ page }) => {
    // Set viewport size
    await page.setViewportSize({ width: 1280, height: 720 });

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features navigation link
    const featuresLink = page.locator('.nav__links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000);

    // Verify we scrolled to the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify URL hash updated
    const url = page.url();
    expect(url).toContain('#features');
  });

  test('Navigation Quick Start link scrolls to quickstart section', async ({ page }) => {
    // Set viewport size
    await page.setViewportSize({ width: 1280, height: 720 });

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Quick Start navigation link
    const quickstartLink = page.locator('.nav__links a[href="#quickstart"]');
    await expect(quickstartLink).toBeVisible();
    await quickstartLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(1000);

    // Verify we scrolled to the quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify URL hash
    const url = page.url();
    expect(url).toContain('#quickstart');
  });

  test('Navigation links are keyboard accessible', async ({ page }) => {
    // Tab through navigation elements
    await page.keyboard.press('Tab'); // Logo
    await page.keyboard.press('Tab'); // First nav link (Features)

    // Verify Features link is focusable and can be activated
    const featuresLink = page.locator('.nav__links a[href="#features"]');
    await featuresLink.focus();
    await expect(featuresLink).toBeFocused();

    // Press Enter to activate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);

    // Should have scrolled
    const url = page.url();
    expect(url).toContain('#features');
  });
});

test.describe('Footer E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Footer GitHub link opens repository in new tab', async ({ page, context }) => {
    // Scroll to footer first
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Find GitHub link in footer
    const footerGithubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(footerGithubLink).toBeVisible();

    // Click and wait for new tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      footerGithubLink.click()
    ]);

    await newPage.waitForLoadState('domcontentloaded');

    // Verify URL
    const newUrl = newPage.url();
    expect(newUrl).toContain('github.com/yetone/mirdb');

    await newPage.close();
  });

  test('Footer license link opens in new tab', async ({ page, context }) => {
    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Find license link in footer (if it exists as a link)
    const licenseLink = footer.locator('a[href*="LICENSE"]');

    if (await licenseLink.count() > 0) {
      await expect(licenseLink).toBeVisible();

      // Click and wait for new tab
      const [newPage] = await Promise.all([
        context.waitForEvent('page'),
        licenseLink.click()
      ]);

      await newPage.waitForLoadState('domcontentloaded');

      // Verify URL contains LICENSE
      const newUrl = newPage.url();
      expect(newUrl).toContain('LICENSE');

      await newPage.close();
    }
  });

  test('Footer is accessible via keyboard navigation', async ({ page }) => {
    // Scroll to footer to ensure it's loaded
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Find footer links and verify they're focusable
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }
  });
});

test.describe('Smooth Scroll E2E Tests', () => {
  test('Test Case 8: Smooth scroll respects prefers-reduced-motion preference', async ({ page }) => {
    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click a navigation link
    const featuresLink = page.locator('.nav__links a[href="#features"]');
    await featuresLink.click();

    // With reduced motion, scroll should be instant (auto behavior)
    // Wait a short time to ensure scroll completed
    await page.waitForTimeout(100);

    // Verify scroll happened (position changed)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify we're at the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Smooth scroll works with normal motion preference', async ({ page }) => {
    // Emulate no motion preference (normal)
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click a navigation link
    const quickstartLink = page.locator('.nav__links a[href="#quickstart"]');
    await quickstartLink.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(1000);

    // Verify scroll happened
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify we're at the quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('URL hash updates on anchor link click', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Initial URL should not have hash
    let url = page.url();
    expect(url).not.toContain('#features');

    // Click features link
    await page.locator('.nav__links a[href="#features"]').click();
    await page.waitForTimeout(500);

    // URL should now have #features
    url = page.url();
    expect(url).toContain('#features');

    // Click quickstart link
    await page.locator('.nav__links a[href="#quickstart"]').click();
    await page.waitForTimeout(500);

    // URL should now have #quickstart
    url = page.url();
    expect(url).toContain('#quickstart');
  });

  test('Scrolling to non-existent section does not break page', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Try to scroll to a non-existent element via JavaScript
    const result = await page.evaluate(() => {
      try {
        window.scrollToElement('#non-existent-section');
        return true;
      } catch (e) {
        return false;
      }
    });

    // Should not throw an error
    expect(result).toBeTruthy();

    // Page should still be functional
    const header = page.locator('header.header');
    await expect(header).toBeVisible();
  });
});

test.describe('Navigation Mobile Tests', () => {
  test('Navigation displays correctly on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Header should still be visible
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Navigation links should be visible (may be in hamburger menu in future)
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();
  });

  test('Footer displays correctly on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Scroll to footer
    const footer = page.locator('footer.footer');
    await footer.scrollIntoViewIfNeeded();

    // Footer should be visible
    await expect(footer).toBeVisible();

    // License should be visible
    const license = footer.locator('.footer__license');
    await expect(license).toBeVisible();

    // GitHub link should be visible
    const githubLink = footer.locator('a[href*="github.com"]').first();
    await expect(githubLink).toBeVisible();
  });
});
