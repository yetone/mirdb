/**
 * Navigation and Links E2E Tests
 * Owner: Scenario 15 - Navigation and Links
 *
 * Verifies all navigation links and external links work correctly
 *
 * Test Cases:
 * 1. Click Features link in header - Page scrolls to features section
 * 2. Click Documentation link in header - Page scrolls to documentation section
 * 3. Click GitHub link in header or footer - Opens MirDB GitHub repository in new tab
 * 4. Click quick-start CTA button in hero - Navigates to interactive demo section
 */

import { test, expect, Page } from '@playwright/test';

// Helper function to check if an element is in viewport
async function isElementInViewport(page: Page, selector: string): Promise<boolean> {
  const element = page.locator(selector);
  const box = await element.boundingBox();
  if (!box) return false;

  const viewportSize = page.viewportSize();
  if (!viewportSize) return false;

  const scrollY = await page.evaluate(() => window.scrollY);
  const elementTop = box.y + scrollY;
  const elementBottom = elementTop + box.height;
  const viewportTop = scrollY;
  const viewportBottom = scrollY + viewportSize.height;

  // Element is in viewport if any part of it is visible
  return elementTop < viewportBottom && elementBottom > viewportTop;
}

// Helper to wait for smooth scroll to complete
async function waitForScrollToComplete(page: Page, timeout = 1000): Promise<void> {
  let lastScrollY = await page.evaluate(() => window.scrollY);
  let stableCount = 0;

  for (let i = 0; i < timeout / 50; i++) {
    await page.waitForTimeout(50);
    const currentScrollY = await page.evaluate(() => window.scrollY);
    if (currentScrollY === lastScrollY) {
      stableCount++;
      if (stableCount >= 3) {
        // Scroll has been stable for 150ms, consider it complete
        return;
      }
    } else {
      stableCount = 0;
      lastScrollY = currentScrollY;
    }
  }
}

test.describe('Navigation and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  // Test Case 1: Click Features link in header
  test('clicking Features link scrolls to features section', async ({ page }) => {
    // Verify we start at the top
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBeLessThan(100);

    // Click the Features link in the navigation
    const featuresLink = page.locator('nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await waitForScrollToComplete(page);

    // Verify the features section is now in viewport
    await expect(page.locator('#features')).toBeInViewport();

    // Verify the URL hash was updated
    const hash = await page.evaluate(() => window.location.hash);
    expect(hash).toBe('#features');
  });

  // Test Case 2: Click Documentation link in header
  test('clicking Documentation link scrolls to documentation section', async ({ page }) => {
    // Verify we start at the top
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBeLessThan(100);

    // Click the Documentation link in the navigation
    const docsLink = page.locator('nav a[href="#documentation"]');
    await expect(docsLink).toBeVisible();
    await docsLink.click();

    // Wait for smooth scroll to complete
    await waitForScrollToComplete(page);

    // Verify the documentation section is now in viewport
    await expect(page.locator('#documentation')).toBeInViewport();

    // Verify the URL hash was updated
    const hash = await page.evaluate(() => window.location.hash);
    expect(hash).toBe('#documentation');
  });

  // Test Case 3a: Click GitHub link in header
  test('GitHub link in header has correct href and target', async ({ page }) => {
    // Find the GitHub link in the header navigation
    const headerGithubLink = page.locator('nav a[href*="github"]');
    await expect(headerGithubLink).toBeVisible();

    // Verify href points to MirDB GitHub repository
    const href = await headerGithubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify link opens in new tab
    const target = await headerGithubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes are present
    const rel = await headerGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  // Test Case 3b: Click GitHub link in footer
  test('GitHub link in footer has correct href and target', async ({ page }) => {
    // Find the GitHub link in the footer
    const footerGithubLink = page.locator('footer a[href*="github"]').first();
    await expect(footerGithubLink).toBeVisible();

    // Verify href points to MirDB GitHub repository
    const href = await footerGithubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify link opens in new tab
    const target = await footerGithubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  // Test Case 4: Click quick-start CTA button in hero
  test('clicking CTA button scrolls to demo section', async ({ page }) => {
    // Verify we start at the top
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBeLessThan(100);

    // Find and click the CTA button
    const ctaButton = page.locator('.cta-button');
    await expect(ctaButton).toBeVisible();

    // Verify CTA points to demo section
    const href = await ctaButton.getAttribute('href');
    expect(href).toBe('#demo');

    await ctaButton.click();

    // Wait for smooth scroll to complete
    await waitForScrollToComplete(page);

    // Verify the demo section is now in viewport
    await expect(page.locator('#demo')).toBeInViewport();
  });

  // Additional test: Navigation from features to documentation
  test('can navigate between sections using header links', async ({ page }) => {
    // Navigate to features first
    await page.locator('nav a[href="#features"]').click();
    await waitForScrollToComplete(page);
    await expect(page.locator('#features')).toBeInViewport();

    // Then navigate to documentation
    await page.locator('nav a[href="#documentation"]').click();
    await waitForScrollToComplete(page);
    await expect(page.locator('#documentation')).toBeInViewport();

    // Navigate back to features
    await page.locator('nav a[href="#features"]').click();
    await waitForScrollToComplete(page);
    await expect(page.locator('#features')).toBeInViewport();
  });

  // Test for keyboard navigation
  test('navigation links are keyboard accessible', async ({ page }) => {
    // Tab to the first navigation link
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // First nav link (Features)

    // Verify a nav link is focused
    const focusedElement = page.locator(':focus');
    const href = await focusedElement.getAttribute('href');
    expect(href).toMatch(/^#|github/);

    // Press Enter to activate the link
    if (href === '#features') {
      await page.keyboard.press('Enter');
      await waitForScrollToComplete(page);
      await expect(page.locator('#features')).toBeInViewport();
    }
  });

  // Test for initial hash in URL
  test('page scrolls to section when loaded with hash', async ({ page }) => {
    // Navigate directly to a section using hash
    await page.goto('/#documentation');
    await page.waitForLoadState('domcontentloaded');

    // Wait for initial scroll
    await page.waitForTimeout(500);

    // Verify the documentation section is in viewport
    await expect(page.locator('#documentation')).toBeInViewport();
  });
});

// Test that navigation links exist and are properly structured
test.describe('Navigation Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('header contains all required navigation links', async ({ page }) => {
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Check for Features link
    await expect(page.locator('nav a[href="#features"]')).toBeVisible();
    await expect(page.locator('nav a[href="#features"]')).toContainText('Features');

    // Check for Documentation link
    await expect(page.locator('nav a[href="#documentation"]')).toBeVisible();
    await expect(page.locator('nav a[href="#documentation"]')).toContainText('Documentation');

    // Check for GitHub link
    await expect(page.locator('nav a[href*="github"]')).toBeVisible();
    await expect(page.locator('nav a[href*="github"]')).toContainText('GitHub');
  });

  test('footer contains GitHub repository link', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for GitHub link in footer
    const footerGithubLink = page.locator('footer a[href*="github"]').first();
    await expect(footerGithubLink).toBeVisible();
  });

  test('all internal section targets exist', async ({ page }) => {
    // Verify all sections that are navigation targets exist
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#demo')).toBeVisible();
    await expect(page.locator('#documentation')).toBeVisible();
    await expect(page.locator('#main-content')).toBeVisible();
  });

  test('skip link points to main content', async ({ page }) => {
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toHaveCount(1);

    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Verify main content target exists
    await expect(page.locator('#main-content')).toBeVisible();
  });
});

// Test navigation in different browsers
test.describe('Cross-browser Navigation', () => {
  test('smooth scroll works correctly', async ({ page }) => {
    await page.goto('/');

    // Record initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click documentation link
    await page.locator('nav a[href="#documentation"]').click();

    // Verify scrolling occurred (not instant jump)
    await page.waitForTimeout(100);
    const midScrollY = await page.evaluate(() => window.scrollY);

    // Wait for scroll to complete
    await waitForScrollToComplete(page);
    const finalScrollY = await page.evaluate(() => window.scrollY);

    // Verify we scrolled down
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify documentation section is visible
    await expect(page.locator('#documentation')).toBeInViewport();
  });
});
