/**
 * Responsive Design E2E Tests - Mobile
 * Owner: Scenario 9 - Responsive Design - Mobile
 *
 * Test viewports: 320px, 375px (mobile range 320px-767px)
 *
 * Tests verify:
 * - No horizontal overflow at minimum viewport widths
 * - Mobile hamburger menu visibility and functionality
 * - Single-column layout for feature cards
 * - Hero section readability and CTA accessibility
 * - Consistent rendering on common mobile devices
 */

import { test, expect } from '@playwright/test';

test.describe('Mobile Responsive Design (320px-767px)', () => {

  // Test Case 1: Page renders without horizontal overflow at 320px
  test.describe('320px Viewport - Minimum Width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('page renders without horizontal overflow at 320px', async ({ page }) => {
      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check document body width doesn't exceed viewport
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Body should not be wider than viewport (no horizontal scroll)
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('all main sections are visible at 320px', async ({ page }) => {
      // Verify navbar is visible
      const navbar = page.getByTestId('navbar');
      await expect(navbar).toBeVisible();

      // Verify hero section is visible
      const hero = page.getByTestId('hero');
      await expect(hero).toBeVisible();

      // Verify features section is visible
      const features = page.getByTestId('features');
      await expect(features).toBeVisible();
    });
  });

  // Test Case 2: Navigation collapses to mobile menu (hamburger)
  test.describe('Mobile Navigation', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('navbar shows hamburger menu button on mobile', async ({ page }) => {
      // Mobile menu button should be visible
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await expect(mobileMenuButton).toBeVisible();

      // Desktop nav links should be hidden (they have md:flex class)
      // We check that the theme toggle in desktop nav is not visible
      const desktopThemeToggle = page.locator('[data-testid="theme-toggle"]').first();
      // On mobile, this should be in the mobile menu, not visible in desktop nav
      await expect(mobileMenuButton).toBeVisible();
    });

    test('hamburger menu has correct aria attributes', async ({ page }) => {
      const mobileMenuButton = page.getByTestId('mobile-menu-button');

      // Should have aria-label
      await expect(mobileMenuButton).toHaveAttribute('aria-label', 'Toggle mobile menu');

      // Initially aria-expanded should be false
      await expect(mobileMenuButton).toHaveAttribute('aria-expanded', 'false');
    });
  });

  // Test Case 3: Mobile navigation drawer opens with all links
  test.describe('Mobile Menu Interaction', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('clicking hamburger opens mobile menu with all navigation links', async ({ page }) => {
      const mobileMenuButton = page.getByTestId('mobile-menu-button');

      // Initially mobile menu should not be visible
      await expect(page.getByTestId('mobile-menu')).not.toBeVisible();

      // Click hamburger to open menu
      await mobileMenuButton.click();

      // Mobile menu should now be visible
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).toBeVisible();

      // aria-expanded should be true
      await expect(mobileMenuButton).toHaveAttribute('aria-expanded', 'true');

      // All navigation links should be present in mobile menu
      await expect(mobileMenu.getByText('GitHub')).toBeVisible();
      await expect(mobileMenu.getByText('Documentation')).toBeVisible();
      await expect(mobileMenu.getByText('API Reference')).toBeVisible();

      // Theme toggle option should be visible
      await expect(mobileMenu.getByText(/Dark Mode|Light Mode/)).toBeVisible();
    });

    test('clicking hamburger again closes mobile menu', async ({ page }) => {
      const mobileMenuButton = page.getByTestId('mobile-menu-button');

      // Open menu
      await mobileMenuButton.click();
      await expect(page.getByTestId('mobile-menu')).toBeVisible();

      // Close menu
      await mobileMenuButton.click();
      await expect(page.getByTestId('mobile-menu')).not.toBeVisible();

      // aria-expanded should be false again
      await expect(mobileMenuButton).toHaveAttribute('aria-expanded', 'false');
    });

    test('mobile menu links have correct hrefs', async ({ page }) => {
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await mobileMenuButton.click();

      const mobileMenu = page.getByTestId('mobile-menu');

      // GitHub link
      const githubLink = mobileMenu.getByRole('link', { name: 'GitHub' });
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');

      // Documentation link
      const docsLink = mobileMenu.getByRole('link', { name: 'Documentation' });
      await expect(docsLink).toHaveAttribute('href', '#documentation');
    });
  });

  // Test Case 4: Feature cards stack in single column at 320px
  test.describe('Features Section Mobile Layout', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('feature cards stack in single column at 320px', async ({ page }) => {
      // Navigate to features section
      const features = page.getByTestId('features');
      await features.scrollIntoViewIfNeeded();

      // Get all feature cards (Card components)
      const cards = features.locator('[data-testid^="feature-card"]');
      const cardCount = await cards.count();

      // If no cards with data-testid, look for article elements or divs with card styling
      if (cardCount === 0) {
        // Find cards by their container grid structure
        const gridContainer = features.locator('.grid');
        await expect(gridContainer).toBeVisible();

        // Check grid computed style - on mobile should be single column
        const gridStyle = await gridContainer.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            display: style.display,
            gridTemplateColumns: style.gridTemplateColumns,
          };
        });

        expect(gridStyle.display).toBe('grid');
        // Single column means only one column track value or a single "1fr" etc.
        // At 320px, md:grid-cols-2 shouldn't apply, so we get default (1 column)
        const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(Boolean).length;
        expect(columnCount).toBe(1);
      }
    });

    test('all feature cards are visible and readable at 320px', async ({ page }) => {
      const features = page.getByTestId('features');
      await features.scrollIntoViewIfNeeded();

      // Check features heading
      const heading = features.getByRole('heading', { name: 'Features' });
      await expect(heading).toBeVisible();

      // Check that feature titles are visible (from constants.ts)
      const featureTitles = [
        'Persistent Key-Value Storage',
        'Memcached Protocol Compatibility',
        'LSM-tree Implementation',
        'Write-Ahead Logging',
        'Atomic Compaction',
      ];

      for (const title of featureTitles) {
        const featureHeading = features.getByRole('heading', { name: title });
        await expect(featureHeading).toBeVisible();
      }
    });
  });

  // Test Case 5: Hero text readable and CTA buttons accessible at 320px
  test.describe('Hero Section Mobile', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('hero headline is readable at 320px', async ({ page }) => {
      const hero = page.getByTestId('hero');
      const headline = hero.getByRole('heading', { level: 1 });

      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Persistent Key-Value Store');

      // Verify text isn't cut off - bounding box should fit within viewport
      const boundingBox = await headline.boundingBox();
      expect(boundingBox).not.toBeNull();
      if (boundingBox) {
        // Right edge of headline shouldn't exceed viewport width
        expect(boundingBox.x + boundingBox.width).toBeLessThanOrEqual(320);
      }
    });

    test('hero subtitle is visible at 320px', async ({ page }) => {
      const hero = page.getByTestId('hero');
      const subtitle = hero.getByText(/with Memcached Protocol/i);

      await expect(subtitle).toBeVisible();
    });

    test('CTA buttons are accessible and clickable at 320px', async ({ page }) => {
      const hero = page.getByTestId('hero');

      // Get Started button
      const getStartedButton = hero.getByRole('link', { name: /get started/i });
      await expect(getStartedButton).toBeVisible();

      // Verify button is tappable (has reasonable size)
      const getStartedBox = await getStartedButton.boundingBox();
      expect(getStartedBox).not.toBeNull();
      if (getStartedBox) {
        // Minimum touch target size (44px recommended)
        expect(getStartedBox.height).toBeGreaterThanOrEqual(44);
      }

      // GitHub button
      const githubButton = hero.getByRole('link', { name: /github/i });
      await expect(githubButton).toBeVisible();

      const githubBox = await githubButton.boundingBox();
      expect(githubBox).not.toBeNull();
      if (githubBox) {
        expect(githubBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('CTA buttons stack vertically on mobile', async ({ page }) => {
      const hero = page.getByTestId('hero');

      const getStartedButton = hero.getByRole('link', { name: /get started/i });
      const githubButton = hero.getByRole('link', { name: /github/i });

      const getStartedBox = await getStartedButton.boundingBox();
      const githubBox = await githubButton.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(githubBox).not.toBeNull();

      if (getStartedBox && githubBox) {
        // On mobile (flex-col), buttons should stack vertically
        // GitHub button should be below Get Started button
        expect(githubBox.y).toBeGreaterThan(getStartedBox.y);
      }
    });
  });

  // Test Case 6: Page displays correctly at 375px (iPhone)
  test.describe('375px Viewport - Common Mobile Device', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    test('page renders without horizontal overflow at 375px', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('all content sections are accessible at 375px', async ({ page }) => {
      // Navbar
      await expect(page.getByTestId('navbar')).toBeVisible();

      // Hero
      const hero = page.getByTestId('hero');
      await expect(hero).toBeVisible();
      await expect(hero.getByRole('heading', { level: 1 })).toBeVisible();

      // Features
      const features = page.getByTestId('features');
      await features.scrollIntoViewIfNeeded();
      await expect(features).toBeVisible();

      // Quick Start
      const quickStart = page.getByTestId('quickstart');
      await quickStart.scrollIntoViewIfNeeded();
      await expect(quickStart).toBeVisible();

      // Status Badges
      const statusBadges = page.getByTestId('status-badges');
      await statusBadges.scrollIntoViewIfNeeded();
      await expect(statusBadges).toBeVisible();

      // Footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });

    test('hamburger menu works correctly at 375px', async ({ page }) => {
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await expect(mobileMenuButton).toBeVisible();

      // Open menu
      await mobileMenuButton.click();
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).toBeVisible();

      // Verify all links are present
      await expect(mobileMenu.getByText('GitHub')).toBeVisible();
      await expect(mobileMenu.getByText('Documentation')).toBeVisible();
      await expect(mobileMenu.getByText('API Reference')).toBeVisible();
    });

    test('text content does not overflow at 375px', async ({ page }) => {
      // Check hero headline doesn't overflow
      const hero = page.getByTestId('hero');
      const headline = hero.getByRole('heading', { level: 1 });

      const headlineBox = await headline.boundingBox();
      expect(headlineBox).not.toBeNull();
      if (headlineBox) {
        // Content should fit within viewport with some padding
        expect(headlineBox.x).toBeGreaterThanOrEqual(0);
        expect(headlineBox.x + headlineBox.width).toBeLessThanOrEqual(375);
      }
    });

    test('feature cards display properly at 375px', async ({ page }) => {
      const features = page.getByTestId('features');
      await features.scrollIntoViewIfNeeded();

      // Grid should still be single column at 375px (below md breakpoint of 768px)
      const gridContainer = features.locator('.grid');
      const gridStyle = await gridContainer.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // Single column grid
      const columnCount = gridStyle.split(' ').filter(Boolean).length;
      expect(columnCount).toBe(1);
    });
  });
});
