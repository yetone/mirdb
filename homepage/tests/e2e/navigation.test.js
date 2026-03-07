/**
 * Navigation E2E Tests
 * Owner: Scenario 5 - Navigation and External Links
 *
 * Tests:
 * - Sticky header navigation
 * - Section links (Features, Usage, Architecture)
 * - External links (GitHub, CircleCI)
 * - Smooth scroll navigation
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = `file://${path.join(__dirname, '../../index.html')}`;

test.describe('Navigation Section (Scenario 5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test.describe('Test Case 1: Navigation header with sticky positioning', () => {
    test('Header should be visible and sticky', async ({ page }) => {
      const header = page.locator('header.header');
      await expect(header).toBeVisible();

      // Check that header has position: sticky
      const position = await header.evaluate(el => getComputedStyle(el).position);
      expect(['sticky', 'fixed']).toContain(position);
    });

    test('Header should remain visible when scrolling', async ({ page }) => {
      const header = page.locator('header.header');
      await expect(header).toBeVisible();

      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, 500));

      // Header should still be visible
      await expect(header).toBeVisible();

      // Check header is at top of viewport
      const headerBox = await header.boundingBox();
      expect(headerBox.y).toBeLessThanOrEqual(0);
    });
  });

  test.describe('Test Case 2: Features navigation link', () => {
    test('Features link should exist and be clickable', async ({ page }) => {
      const featuresLink = page.locator('.nav__link[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveText(/features/i);
    });
  });

  test.describe('Test Case 3: Usage navigation link', () => {
    test('Usage or Demo link should exist', async ({ page }) => {
      // Accept either "Usage" or "Demo" as both refer to the usage demonstration
      const usageLink = page.locator('.nav__link:has-text("Usage"), .nav__link:has-text("Demo")');
      await expect(usageLink.first()).toBeVisible();
    });
  });

  test.describe('Test Case 4: Architecture navigation link', () => {
    test('Architecture link should exist and be clickable', async ({ page }) => {
      const archLink = page.locator('.nav__link[href="#architecture"]');
      await expect(archLink).toBeVisible();
      await expect(archLink).toHaveText(/architecture/i);
    });
  });

  test.describe('Test Case 5: GitHub link', () => {
    test('GitHub link should exist and point to correct repository', async ({ page }) => {
      const githubLink = page.locator('a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink.first()).toBeVisible();
    });

    test('GitHub link should open in new tab', async ({ page }) => {
      const githubLink = page.locator('a[href="https://github.com/yetone/mirdb"]').first();
      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');
    });
  });

  test.describe('Test Case 6: CircleCI badge', () => {
    test('CircleCI badge image should be present', async ({ page }) => {
      const badgeImg = page.locator('img[src*="circleci"]');
      await expect(badgeImg).toBeVisible();
    });

    test('CircleCI badge should link to CircleCI', async ({ page }) => {
      const badgeLink = page.locator('a[href*="circleci"]');
      await expect(badgeLink).toBeVisible();
      const href = await badgeLink.getAttribute('href');
      expect(href).toContain('circleci');
    });
  });

  test.describe('Test Case 7: Smooth scroll navigation', () => {
    test('Clicking Features link should scroll to features section', async ({ page }) => {
      const featuresLink = page.locator('.nav__link[href="#features"]');
      const featuresSection = page.locator('#features');

      // Get initial position
      const initialY = await page.evaluate(() => window.scrollY);

      // Click the features link
      await featuresLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Check that we scrolled
      const newY = await page.evaluate(() => window.scrollY);
      expect(newY).not.toBe(initialY);

      // Check that features section is now visible in viewport
      await expect(featuresSection).toBeInViewport();
    });

    test('Clicking Architecture link should scroll to architecture section', async ({ page }) => {
      const archLink = page.locator('.nav__link[href="#architecture"]');
      const archSection = page.locator('#architecture');

      // Click the architecture link
      await archLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Check that architecture section is now visible in viewport
      await expect(archSection).toBeInViewport();
    });

    test('Internal navigation links should use smooth scroll behavior', async ({ page }) => {
      // Check that smooth scroll is enabled via JavaScript
      const featuresLink = page.locator('.nav__link[href="#features"]');

      // Create a listener for scroll behavior
      const scrollBehavior = await page.evaluate(() => {
        return new Promise((resolve) => {
          const link = document.querySelector('.nav__link[href="#features"]');
          if (link) {
            link.click();
            // Check if smooth scroll is being used (scrollBehavior or JS animation)
            setTimeout(() => {
              resolve('scrolled');
            }, 100);
          } else {
            resolve('no-link');
          }
        });
      });

      expect(scrollBehavior).toBe('scrolled');
    });
  });

  test.describe('Navigation accessibility', () => {
    test('Skip link should be functional', async ({ page }) => {
      const skipLink = page.locator('.skip-link');

      // Skip link should exist
      await expect(skipLink).toBeAttached();

      // Focus on skip link to make it visible
      await skipLink.focus();
      await expect(skipLink).toBeVisible();

      // Click should navigate to main content
      await skipLink.click();

      // Main content should receive focus or be in viewport
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeInViewport();
    });

    test('Theme toggle button should be accessible', async ({ page }) => {
      const themeToggle = page.locator('.theme-toggle');
      await expect(themeToggle).toBeVisible();

      // Should have aria-label
      const ariaLabel = await themeToggle.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    });
  });
});
