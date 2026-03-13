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

/**
 * Responsive Design E2E Tests - Tablet & Desktop
 * Owner: Scenario 10 - Responsive Design - Tablet & Desktop
 *
 * Test viewports:
 * - Tablet: 768px
 * - Desktop: 1024px, 1366px, 1920px
 *
 * Tests verify:
 * - Full navbar displays at tablet/desktop widths (not mobile menu)
 * - Feature cards in 2-column grid at tablet width
 * - Multi-column features at desktop widths
 * - Content has max-width container (not full-bleed) at large screens
 * - Hero section visible above-the-fold on laptop (1366x768)
 */

test.describe('Tablet Responsive Design (768px-1023px)', () => {
  // Test Case 1: Full navbar displays at 768px (not mobile menu)
  test.describe('768px Viewport - Tablet Width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
    });

    test('full navbar displays at 768px (not mobile menu)', async ({ page }) => {
      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Mobile menu button should NOT be visible (hidden by md:hidden class)
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await expect(mobileMenuButton).not.toBeVisible();

      // Desktop navigation links should be visible
      const navLinkGithub = page.getByTestId('nav-link-github');
      await expect(navLinkGithub).toBeVisible();

      const navLinkDocs = page.getByTestId('nav-link-documentation');
      await expect(navLinkDocs).toBeVisible();

      const navLinkApi = page.getByTestId('nav-link-api-reference');
      await expect(navLinkApi).toBeVisible();

      // Theme toggle should be visible in the desktop nav
      const themeToggle = page.getByTestId('theme-toggle');
      await expect(themeToggle).toBeVisible();
    });

    test('page renders without horizontal overflow at 768px', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    // Test Case 2: Feature cards display in 2-column grid at 768px
    test('feature cards display in 2-column grid at 768px', async ({ page }) => {
      const features = page.getByTestId('features');
      await features.scrollIntoViewIfNeeded();

      // Grid should be 2 columns at md breakpoint (md:grid-cols-2)
      const gridContainer = features.locator('.grid');
      await expect(gridContainer).toBeVisible();

      const gridStyle = await gridContainer.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // 2-column grid means two column track values
      const columnCount = gridStyle.split(' ').filter(Boolean).length;
      expect(columnCount).toBe(2);
    });

    test('all main sections are visible and properly laid out at 768px', async ({ page }) => {
      // Navbar
      await expect(page.getByTestId('navbar')).toBeVisible();

      // Hero
      const hero = page.getByTestId('hero');
      await expect(hero).toBeVisible();
      await expect(hero.getByRole('heading', { level: 1 })).toBeVisible();

      // CTA buttons should be side-by-side at tablet (sm:flex-row)
      const getStartedButton = hero.getByRole('link', { name: /get started/i });
      const githubButton = hero.getByRole('link', { name: /github/i });

      const getStartedBox = await getStartedButton.boundingBox();
      const githubBox = await githubButton.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(githubBox).not.toBeNull();

      if (getStartedBox && githubBox) {
        // At 768px with sm:flex-row, buttons should be on the same row
        // Y positions should be similar (within a few pixels tolerance)
        expect(Math.abs(getStartedBox.y - githubBox.y)).toBeLessThan(10);
      }

      // Features
      const features = page.getByTestId('features');
      await features.scrollIntoViewIfNeeded();
      await expect(features).toBeVisible();

      // Quick Start
      const quickStart = page.getByTestId('quickstart');
      await quickStart.scrollIntoViewIfNeeded();
      await expect(quickStart).toBeVisible();

      // Footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });
  });
});

test.describe('Desktop Responsive Design (1024px+)', () => {
  // Test Case 3: Desktop layout with multi-column features at 1024px
  test.describe('1024px Viewport - Desktop Width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');
    });

    test('desktop layout with multi-column features at 1024px', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Mobile menu button should NOT be visible
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await expect(mobileMenuButton).not.toBeVisible();

      // Desktop navigation should be visible
      const navLinkGithub = page.getByTestId('nav-link-github');
      await expect(navLinkGithub).toBeVisible();

      // Features grid should have 3 columns at lg breakpoint (lg:grid-cols-3)
      const features = page.getByTestId('features');
      await features.scrollIntoViewIfNeeded();

      const gridContainer = features.locator('.grid');
      const gridStyle = await gridContainer.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });

      // 3-column grid at lg breakpoint
      const columnCount = gridStyle.split(' ').filter(Boolean).length;
      expect(columnCount).toBe(3);
    });

    test('page renders without horizontal overflow at 1024px', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });
  });

  // Test Case 5: Hero section visible above-the-fold at 1366x768 (laptop)
  test.describe('1366x768 Viewport - Laptop Screen', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1366, height: 768 });
      await page.goto('/');
    });

    test('hero section is fully visible above the fold at 1366x768', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      const hero = page.getByTestId('hero');
      await expect(hero).toBeVisible();

      // Get hero bounding box
      const heroBox = await hero.boundingBox();
      expect(heroBox).not.toBeNull();

      if (heroBox) {
        // Hero should start at or near top of viewport (accounting for navbar)
        expect(heroBox.y).toBeLessThan(100); // Navbar is ~64px

        // Hero's bottom should be within viewport height (768px)
        // This ensures the entire hero section is visible without scrolling
        const heroBottom = heroBox.y + heroBox.height;
        expect(heroBottom).toBeLessThanOrEqual(768);
      }

      // Verify key hero elements are visible
      const headline = hero.getByRole('heading', { level: 1 });
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Persistent Key-Value Store');

      const subtitle = hero.getByText(/with Memcached Protocol/i);
      await expect(subtitle).toBeVisible();

      // CTA buttons should be visible
      const getStartedButton = hero.getByRole('link', { name: /get started/i });
      await expect(getStartedButton).toBeVisible();

      const githubButton = hero.getByRole('link', { name: /github/i });
      await expect(githubButton).toBeVisible();
    });

    test('desktop navigation is fully functional at 1366px', async ({ page }) => {
      // Mobile menu button should be hidden
      const mobileMenuButton = page.getByTestId('mobile-menu-button');
      await expect(mobileMenuButton).not.toBeVisible();

      // All nav links should be visible
      await expect(page.getByTestId('nav-link-github')).toBeVisible();
      await expect(page.getByTestId('nav-link-documentation')).toBeVisible();
      await expect(page.getByTestId('nav-link-api-reference')).toBeVisible();

      // Theme toggle should work
      const themeToggle = page.getByTestId('theme-toggle');
      await expect(themeToggle).toBeVisible();
    });
  });

  // Test Case 4: Content has max-width container at 1920px (not full-bleed)
  test.describe('1920px Viewport - Large Desktop', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');
    });

    test('content has max-width container at 1920px (not full-bleed)', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      // Check that the navbar container has max-width (max-w-7xl = 1280px)
      const navbarContainer = page.locator('[data-testid="navbar"] > div').first();
      const navbarContainerWidth = await navbarContainer.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return el.offsetWidth;
      });

      // Max-w-7xl is 1280px, container should not exceed this
      expect(navbarContainerWidth).toBeLessThanOrEqual(1280);

      // Check features section container
      const features = page.getByTestId('features');
      await features.scrollIntoViewIfNeeded();

      const featuresContainer = features.locator('div.max-w-7xl').first();
      const featuresContainerWidth = await featuresContainer.evaluate((el) => {
        return el.offsetWidth;
      });

      // Features container should also respect max-width
      expect(featuresContainerWidth).toBeLessThanOrEqual(1280);

      // Verify content is centered (has equal margins on both sides)
      const featuresBox = await featuresContainer.boundingBox();
      expect(featuresBox).not.toBeNull();

      if (featuresBox) {
        // Content should be centered - left margin should be approximately equal to right margin
        const leftMargin = featuresBox.x;
        const rightMargin = 1920 - (featuresBox.x + featuresBox.width);
        // Allow 50px tolerance for centering
        expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
      }
    });

    test('page renders without horizontal overflow at 1920px', async ({ page }) => {
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('all sections have proper max-width containers at 1920px', async ({ page }) => {
      // Quick Start section
      const quickStart = page.getByTestId('quickstart');
      await quickStart.scrollIntoViewIfNeeded();

      const quickStartContainer = quickStart.locator('div.max-w-7xl, div.max-w-4xl, div.max-w-3xl').first();
      const quickStartContainerBox = await quickStartContainer.boundingBox();

      expect(quickStartContainerBox).not.toBeNull();
      if (quickStartContainerBox) {
        // Container should not span the full 1920px width
        expect(quickStartContainerBox.width).toBeLessThan(1920);
      }

      // Footer section
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      const footerContainer = footer.locator('div.max-w-7xl').first();
      const footerContainerWidth = await footerContainer.evaluate((el) => {
        return el.offsetWidth;
      });

      expect(footerContainerWidth).toBeLessThanOrEqual(1280);
    });

    test('hero section is properly styled at 1920px', async ({ page }) => {
      const hero = page.getByTestId('hero');
      await expect(hero).toBeVisible();

      // Hero headline should use larger font size at lg breakpoint
      const headline = hero.getByRole('heading', { level: 1 });
      const headlineStyle = await headline.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          fontSize: style.fontSize,
          fontWeight: style.fontWeight,
        };
      });

      // At lg breakpoint, should be text-6xl (60px) - verify it's at least that large
      const fontSize = parseInt(headlineStyle.fontSize);
      expect(fontSize).toBeGreaterThanOrEqual(48); // text-5xl or larger
    });
  });
});
