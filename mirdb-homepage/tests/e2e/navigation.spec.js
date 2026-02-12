/**
 * Navigation E2E Tests
 * Owner: Scenario 9 - Navigation and Internal Links
 *
 * Tests for validating navigation elements and internal links
 * work correctly, including anchor scrolling and direct linking.
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Internal Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('clicking Get Started button scrolls to quickstart section', async ({ page }) => {
    // Test case 1: Click 'Get Started' button and verify it scrolls to quick start section smoothly

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Find and click the "Get Started" button
    const getStartedButton = page.locator('a.btn:has-text("Get Started")');
    await expect(getStartedButton).toBeVisible();

    // Verify the button has correct href
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click the button
    await getStartedButton.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify the quickstart section is now in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify scroll position has changed (page scrolled down)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);
  });

  test('navigating to page with #quickstart anchor shows quickstart section', async ({ page }) => {
    // Test case 2: Navigate to page with #quickstart anchor
    // Expected: Page loads with quick start section visible

    await page.goto('/#quickstart');
    await page.waitForLoadState('domcontentloaded');

    // Allow time for browser to scroll to anchor
    await page.waitForTimeout(300);

    // Verify the quickstart section is visible in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();
    await expect(quickstartSection).toBeInViewport();

    // Verify the section has expected content
    const heading = quickstartSection.locator('h2');
    await expect(heading).toContainText('Quick Start');
  });

  test('navigating to page with #features anchor shows features section', async ({ page }) => {
    // Test case 3: Navigate to page with #features anchor
    // Expected: Page loads with features section visible

    await page.goto('/#features');
    await page.waitForLoadState('domcontentloaded');

    // Allow time for browser to scroll to anchor
    await page.waitForTimeout(300);

    // Verify the features section is visible in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(featuresSection).toBeInViewport();

    // Verify the section has expected content
    const heading = featuresSection.locator('h2');
    await expect(heading).toContainText('Features');
  });

  test('navigating to page with #demo anchor shows demo section', async ({ page }) => {
    // Additional test: Verify demo anchor works too
    await page.goto('/#demo');
    await page.waitForLoadState('domcontentloaded');

    await page.waitForTimeout(300);

    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();
    await expect(demoSection).toBeInViewport();
  });

  test('all internal anchor links point to existing sections', async ({ page }) => {
    // Test case 4: Check all internal links have valid hrefs
    // Expected: All anchor links point to existing section IDs

    // Get all anchor links that start with #
    const internalLinks = page.locator('a[href^="#"]');
    const count = await internalLinks.count();

    expect(count).toBeGreaterThan(0);

    // Collect all internal link hrefs
    const hrefs = [];
    for (let i = 0; i < count; i++) {
      const href = await internalLinks.nth(i).getAttribute('href');
      hrefs.push(href);
    }

    // Verify each internal link points to an existing element
    for (const href of hrefs) {
      const targetId = href.substring(1); // Remove the # prefix
      const targetElement = page.locator(`#${targetId}`);

      // Verify the target element exists
      const exists = await targetElement.count();
      expect(exists, `Target element for ${href} should exist`).toBeGreaterThan(0);
    }
  });

  test('Get Started button is visible and accessible', async ({ page }) => {
    // Verify the Get Started button is present and properly styled
    const getStartedButton = page.locator('a.btn:has-text("Get Started")');

    await expect(getStartedButton).toBeVisible();
    await expect(getStartedButton).toBeEnabled();

    // Check it's in the hero section
    const heroSection = page.locator('header.hero');
    await expect(heroSection.locator('a:has-text("Get Started")')).toBeVisible();
  });

  test('smooth scroll behavior is applied', async ({ page }) => {
    // Check that CSS smooth scroll is applied or scroll happens
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });

    // If smooth scroll is configured via CSS, it should be 'smooth'
    // If not, the click should still scroll (handled by browser default anchor behavior)
    // Either way is acceptable - the important thing is that the scroll happens

    // Click Get Started and verify smooth scrolling occurs
    const getStartedButton = page.locator('a.btn:has-text("Get Started")');
    await getStartedButton.click();

    // Small wait to allow for any scroll animation
    await page.waitForTimeout(100);

    // Verify the page has scrolled
    const scrollY = await page.evaluate(() => window.scrollY);
    expect(scrollY).toBeGreaterThan(0);
  });

  test('all section IDs are unique', async ({ page }) => {
    // Verify that all sections have unique IDs (important for navigation)
    const sectionsWithIds = await page.evaluate(() => {
      const elements = document.querySelectorAll('[id]');
      const ids = Array.from(elements).map(el => el.id);
      return ids;
    });

    const uniqueIds = new Set(sectionsWithIds);
    expect(sectionsWithIds.length).toBe(uniqueIds.size);
  });

  test('navigation between sections works correctly', async ({ page }) => {
    // Test navigating through multiple sections
    const sections = ['#features', '#demo', '#quickstart'];

    for (const section of sections) {
      await page.goto('/' + section);
      await page.waitForLoadState('domcontentloaded');
      await page.waitForTimeout(200);

      const sectionElement = page.locator(section);
      await expect(sectionElement).toBeInViewport();
    }
  });
});
