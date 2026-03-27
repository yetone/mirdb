/**
 * Navigation E2E Tests
 * Owner: Scenario 5 - Navigation and GitHub Link
 *
 * Tests:
 * - GitHub link presence and target
 * - Section navigation (smooth scroll)
 * - Keyboard accessibility for navigation
 */
import { test, expect } from '@playwright/test';

test.describe('Navigation and GitHub Link', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: GitHub link exists in header with correct text', async ({ page }) => {
    // Check for GitHub link in the header navigation
    const header = page.locator('header');
    const githubLink = header.locator('a[href*="github.com"]').first();

    await expect(githubLink).toBeVisible();

    // Verify the link text contains "GitHub" or has GitHub-related content
    const linkText = await githubLink.textContent();
    expect(linkText.toLowerCase()).toContain('github');

    // Verify it links to the correct repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
  });

  test('TC2: GitHub link opens in new tab with security attributes', async ({ page }) => {
    // Find GitHub link in header
    const header = page.locator('header');
    const githubLink = header.locator('a[href*="github.com"]').first();

    // Verify target="_blank" attribute
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel="noopener noreferrer" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  test('TC3: Features nav link scrolls to features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features navigation link in header
    const featuresLink = page.locator('header a[href="#features"]').first();
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify scroll position has changed (scrolled down)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);
  });

  test('TC4: Quick Start nav link scrolls to quick start section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Quick Start navigation link in header
    const quickStartLink = page.locator('header a[href="#quick-start"]').first();
    await expect(quickStartLink).toBeVisible();
    await quickStartLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the quick start section is in view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();

    // Verify scroll position has changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);
  });

  test('TC5: Navigation links are keyboard accessible', async ({ page }) => {
    // Focus on the page body first
    await page.keyboard.press('Tab');

    // Collect all focusable navigation elements
    const navLinks = page.locator('header a, header button');
    const linkCount = await navLinks.count();

    // Ensure there are navigation links
    expect(linkCount).toBeGreaterThan(0);

    // Tab through all navigation links and verify they can receive focus
    const focusedElements = [];

    // Tab through the header navigation elements
    for (let i = 0; i < linkCount + 5; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tagName: el?.tagName,
          href: el?.getAttribute('href'),
          role: el?.getAttribute('role'),
          ariaLabel: el?.getAttribute('aria-label'),
          isInHeader: el?.closest('header') !== null
        };
      });

      if (activeElement.isInHeader && (activeElement.tagName === 'A' || activeElement.tagName === 'BUTTON')) {
        focusedElements.push(activeElement);
      }

      await page.keyboard.press('Tab');
    }

    // Verify that navigation links were reachable via Tab
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify specific nav links are present among focused elements
    const focusedHrefs = focusedElements.map(el => el.href);
    expect(focusedHrefs.some(href => href === '#features' || href?.includes('features'))).toBeTruthy();
    expect(focusedHrefs.some(href => href === '#quick-start' || href?.includes('quick-start'))).toBeTruthy();
    expect(focusedHrefs.some(href => href?.includes('github.com'))).toBeTruthy();
  });

  test('Mobile menu button is accessible', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 667 });

    // Find mobile menu button
    const mobileMenuBtn = page.locator('#mobile-menu-btn');
    await expect(mobileMenuBtn).toBeVisible();

    // Verify it has aria-label for accessibility
    const ariaLabel = await mobileMenuBtn.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();

    // Verify aria-expanded is initially false
    const ariaExpanded = await mobileMenuBtn.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('false');

    // Click the button
    await mobileMenuBtn.click();

    // Verify aria-expanded changes to true
    const ariaExpandedAfter = await mobileMenuBtn.getAttribute('aria-expanded');
    expect(ariaExpandedAfter).toBe('true');

    // Verify mobile menu is now visible
    const mobileMenu = page.locator('#mobile-menu');
    await expect(mobileMenu).toBeVisible();
  });
});
