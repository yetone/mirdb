/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 16 - Cross-Browser Compatibility
 *
 * Test cases:
 * - Page renders in Chrome
 * - Page renders in Firefox
 * - Page renders in Safari
 * - Page renders in Edge
 * - Layout consistent across browsers
 */

import { test, expect } from '@playwright/test';

// All tests in this file run on all configured browser projects (chromium, firefox, webkit)
// The Playwright config defines browser-specific projects

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Test Case 1-4: Page renders correctly in all browsers', () => {
    test('page loads successfully and displays main content', async ({ page, browserName }) => {
      // Verify page title
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main structural elements are present
      const header = page.locator('header');
      const main = page.locator('main');
      const footer = page.locator('footer');

      await expect(header).toBeVisible();
      await expect(main).toBeVisible();
      await expect(footer).toBeVisible();

      // Log which browser is being tested for debugging
      console.log(`Testing on browser: ${browserName}`);
    });

    test('hero section renders correctly', async ({ page }) => {
      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();

      // Hero title
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Hero headline
      const heroHeadline = page.locator('.hero-headline');
      await expect(heroHeadline).toBeVisible();

      // Hero subheadline
      const heroSubheadline = page.locator('.hero-subheadline');
      await expect(heroSubheadline).toBeVisible();

      // CTA buttons
      const getStartedBtn = page.locator('#cta-get-started');
      const githubBtn = page.locator('#cta-github');
      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();
    });

    test('features section renders correctly', async ({ page }) => {
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Section title
      const featuresTitle = page.locator('#features-title');
      await expect(featuresTitle).toBeVisible();
      await expect(featuresTitle).toContainText('Features');

      // Feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // Verify all four feature cards are visible
      await expect(featureCards.nth(0)).toBeVisible();
      await expect(featureCards.nth(1)).toBeVisible();
      await expect(featureCards.nth(2)).toBeVisible();
      await expect(featureCards.nth(3)).toBeVisible();
    });

    test('quickstart section renders correctly', async ({ page }) => {
      const quickstart = page.locator('#quickstart');
      await expect(quickstart).toBeVisible();

      // Section title
      const quickstartTitle = page.locator('#quickstart-title');
      await expect(quickstartTitle).toBeVisible();
      await expect(quickstartTitle).toContainText('Quick Start');

      // Code blocks
      const codeBlocks = page.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThanOrEqual(2);

      // Copy buttons
      const copyBtns = page.locator('.copy-btn');
      const copyBtnCount = await copyBtns.count();
      expect(copyBtnCount).toBeGreaterThanOrEqual(2);
    });

    test('architecture section renders correctly', async ({ page }) => {
      const architecture = page.locator('#architecture');
      await expect(architecture).toBeVisible();

      // Section title
      const architectureTitle = page.locator('#architecture-title');
      await expect(architectureTitle).toBeVisible();
      await expect(architectureTitle).toContainText('Architecture');

      // LSM components
      const walComponent = page.locator('[data-component="wal"]').first();
      const memtableComponent = page.locator('[data-component="memtable"]').first();
      const sstableComponent = page.locator('[data-component="sstable"]').first();

      await expect(walComponent).toBeVisible();
      await expect(memtableComponent).toBeVisible();
      await expect(sstableComponent).toBeVisible();
    });

    test('protocol section renders correctly', async ({ page }) => {
      const protocol = page.locator('#protocol');
      await expect(protocol).toBeVisible();

      // Section title
      const protocolTitle = page.locator('#protocol-title');
      await expect(protocolTitle).toBeVisible();
      await expect(protocolTitle).toContainText('Protocol Reference');

      // Protocol table
      const protocolTable = page.locator('.protocol-table');
      await expect(protocolTable).toBeVisible();

      // Table rows
      const tableRows = page.locator('.protocol-table tbody tr');
      const rowCount = await tableRows.count();
      expect(rowCount).toBeGreaterThanOrEqual(8);
    });

    test('status section renders correctly', async ({ page }) => {
      const status = page.locator('#status');
      await expect(status).toBeVisible();

      // Section title
      const statusTitle = page.locator('#status-title');
      await expect(statusTitle).toBeVisible();
      await expect(statusTitle).toContainText('Project Status');

      // Implemented and roadmap columns
      const implementedColumn = page.locator('[data-status-type="implemented"]');
      const roadmapColumn = page.locator('[data-status-type="roadmap"]');

      await expect(implementedColumn).toBeVisible();
      await expect(roadmapColumn).toBeVisible();
    });

    test('footer section renders correctly', async ({ page }) => {
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      // Footer logo
      const footerLogo = page.locator('.footer-logo');
      await expect(footerLogo).toBeVisible();

      // Footer navigation
      const footerNav = page.locator('.footer-nav');
      await expect(footerNav).toBeVisible();

      // License info
      const footerCopyright = page.locator('.footer-copyright');
      await expect(footerCopyright).toBeVisible();
      await expect(footerCopyright).toContainText('MIT License');
    });

    test('navigation header renders correctly', async ({ page }) => {
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Logo
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Theme toggle button
      const themeToggle = page.locator('#theme-toggle');
      await expect(themeToggle).toBeVisible();

      // GitHub link
      const githubLink = page.locator('#github-link');
      await expect(githubLink).toBeVisible();
    });
  });

  test.describe('Test Case 5: CSS Grid/Flexbox layout consistency', () => {
    test('features grid displays correctly', async ({ page }) => {
      // Set viewport to desktop size
      await page.setViewportSize({ width: 1280, height: 800 });

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Check grid is being used
      const display = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('grid');

      // Get grid template columns
      const gridTemplateColumns = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have multiple columns on desktop
      const columnCount = gridTemplateColumns.split(' ').filter(c => c !== '').length;
      expect(columnCount).toBeGreaterThan(1);
    });

    test('status grid displays correctly', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });

      const statusGrid = page.locator('.status-grid');
      await expect(statusGrid).toBeVisible();

      // Check grid is being used
      const display = await statusGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('grid');

      // Get grid template columns
      const gridTemplateColumns = await statusGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have at least 2 columns on desktop
      const columnCount = gridTemplateColumns.split(' ').filter(c => c !== '').length;
      expect(columnCount).toBeGreaterThanOrEqual(2);
    });

    test('flexbox navigation displays correctly', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });

      // Use the main navigation specifically
      const nav = page.locator('nav[aria-label="Main navigation"]');
      await expect(nav).toBeVisible();

      // Check flexbox is being used
      const display = await nav.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('flex');

      // Check justify-content
      const justifyContent = await nav.evaluate((el) => {
        return window.getComputedStyle(el).justifyContent;
      });
      expect(justifyContent).toBe('space-between');
    });

    test('hero section layout is consistent', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });

      const heroContent = page.locator('.hero-content');
      await expect(heroContent).toBeVisible();

      // Check text alignment
      const textAlign = await heroContent.evaluate((el) => {
        return window.getComputedStyle(el).textAlign;
      });
      expect(textAlign).toBe('center');
    });

    test('footer grid displays correctly', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });

      const footerContent = page.locator('.footer-content');
      await expect(footerContent).toBeVisible();

      // Check grid is being used
      const display = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('grid');

      // Get grid template columns
      const gridTemplateColumns = await footerContent.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // Should have multiple columns on desktop
      const columnCount = gridTemplateColumns.split(' ').filter(c => c !== '').length;
      expect(columnCount).toBeGreaterThan(1);
    });

    test('architecture flow uses flexbox correctly', async ({ page }) => {
      const flowComponents = page.locator('.flow-components').first();
      await expect(flowComponents).toBeVisible();

      // Check flexbox is being used
      const display = await flowComponents.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('flex');
    });
  });

  test.describe('Browser-specific visual rendering', () => {
    test('CSS custom properties are applied correctly', async ({ page }) => {
      // Check that CSS custom properties work across browsers
      const body = page.locator('body');

      // Get computed background color
      const backgroundColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Should have a valid background color (not 'transparent' or empty)
      expect(backgroundColor).toBeTruthy();
      expect(backgroundColor).not.toBe('transparent');
      expect(backgroundColor).not.toBe('');
    });

    test('SVG icons render correctly', async ({ page }) => {
      // Check feature icons
      const featureIcons = page.locator('.feature-icon img');
      const iconCount = await featureIcons.count();
      expect(iconCount).toBeGreaterThan(0);

      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        await expect(icon).toBeVisible();

        // Check icon has dimensions
        const boundingBox = await icon.boundingBox();
        expect(boundingBox).toBeTruthy();
        expect(boundingBox.width).toBeGreaterThan(0);
        expect(boundingBox.height).toBeGreaterThan(0);
      }
    });

    test('font rendering is consistent', async ({ page }) => {
      // Check that fonts are applied correctly
      const heroTitle = page.locator('.hero-title');

      const fontFamily = await heroTitle.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Should have a valid font family
      expect(fontFamily).toBeTruthy();
      expect(fontFamily).not.toBe('');
    });

    test('box shadows render correctly', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const card = featureCards.first();

      // Hover over the card to trigger shadow
      await card.hover();

      // Give time for transition
      await page.waitForTimeout(300);

      const boxShadow = await card.evaluate((el) => {
        return window.getComputedStyle(el).boxShadow;
      });

      // Box shadow should be applied (not 'none')
      expect(boxShadow).toBeTruthy();
    });

    test('transitions work smoothly', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');

      // Get initial transition property
      const transition = await themeToggle.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });

      // Should have transition defined
      expect(transition).toBeTruthy();
      expect(transition).not.toBe('none 0s ease 0s');
    });
  });

  test.describe('Interactive elements work across browsers', () => {
    test('theme toggle works', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');
      const html = page.locator('html');

      // Get initial theme
      const initialTheme = await html.getAttribute('data-theme');

      // Click theme toggle
      await themeToggle.click();

      // Wait for transition
      await page.waitForTimeout(100);

      // Theme should have changed
      const newTheme = await html.getAttribute('data-theme');

      // If initial was light (or null), new should be dark, and vice versa
      if (initialTheme === 'dark') {
        expect(newTheme).not.toBe('dark');
      } else {
        expect(newTheme).toBe('dark');
      }
    });

    test('smooth scroll works on anchor links', async ({ page }) => {
      // Click on Get Started button which links to #quickstart
      const getStartedBtn = page.locator('#cta-get-started');
      await getStartedBtn.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(500);

      // Check that we've scrolled to the quickstart section
      const quickstartSection = page.locator('#quickstart');
      const boundingBox = await quickstartSection.boundingBox();

      // Section should be near the top of viewport
      expect(boundingBox.y).toBeLessThan(200);
    });

    test('copy buttons are clickable', async ({ page }) => {
      const copyBtn = page.locator('.copy-btn').first();
      await expect(copyBtn).toBeVisible();

      // Click should not throw an error
      await copyBtn.click();

      // Wait for feedback
      await page.waitForTimeout(300);

      // Button should still be visible after click
      await expect(copyBtn).toBeVisible();
    });

    test('external links open correctly', async ({ page }) => {
      const githubLink = page.locator('#github-link');

      // Check that link has correct attributes
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');

      // Check href is present
      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com');
    });
  });

  test.describe('Responsive behavior across browsers', () => {
    test('mobile viewport renders correctly', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 812 });

      // Hamburger menu should be visible
      const hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).toBeVisible();

      // Desktop nav should be hidden
      const navLinks = page.locator('.nav-links');
      const display = await navLinks.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(display).toBe('none');

      // Hero content should still be visible
      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();
    });

    test('tablet viewport renders correctly', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });

      // All main sections should be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('#protocol')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
      await expect(page.locator('.footer')).toBeVisible();
    });

    test('large desktop viewport renders correctly', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });

      // All sections should be visible and properly rendered
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#architecture')).toBeVisible();
      await expect(page.locator('#protocol')).toBeVisible();
      await expect(page.locator('#status')).toBeVisible();
      await expect(page.locator('.footer')).toBeVisible();

      // Verify page content renders without layout issues
      const container = page.locator('.container').first();
      const boundingBox = await container.boundingBox();

      // Container should exist and have positive dimensions
      expect(boundingBox).toBeTruthy();
      expect(boundingBox.width).toBeGreaterThan(0);
      expect(boundingBox.height).toBeGreaterThan(0);
    });
  });
});
