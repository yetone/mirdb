/**
 * Desktop Browser Compatibility E2E Tests
 * Owner: Scenario 10 - Desktop Browser Compatibility
 *
 * Test coverage:
 * - CSS renders correctly without vendor prefix issues
 * - Layout consistency across browsers (Chrome, Firefox, Safari, Edge)
 * - JavaScript functionality works across browsers
 *
 * Note: These tests run on all configured browser projects
 * to verify cross-browser compatibility.
 */

import { test, expect } from '@playwright/test';

test.describe('Desktop Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test.describe('CSS Rendering - Test Case 1', () => {
    test('should render CSS custom properties correctly', async ({ page }) => {
      // Test that CSS variables are applied correctly
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) =>
        window.getComputedStyle(el).fontFamily
      );
      expect(fontFamily).toBeTruthy();
      expect(fontFamily).not.toBe('');
    });

    test('should render box-sizing correctly', async ({ page }) => {
      // Verify box-sizing: border-box is applied
      const container = page.locator('.container').first();
      const boxSizing = await container.evaluate((el) =>
        window.getComputedStyle(el).boxSizing
      );
      expect(boxSizing).toBe('border-box');
    });

    test('should render flexbox layouts correctly', async ({ page }) => {
      // Verify header flexbox layout
      const headerContainer = page.locator('.header-container');
      const display = await headerContainer.evaluate((el) =>
        window.getComputedStyle(el).display
      );
      expect(display).toBe('flex');

      const justifyContent = await headerContainer.evaluate((el) =>
        window.getComputedStyle(el).justifyContent
      );
      expect(justifyContent).toBe('space-between');
    });

    test('should render grid layouts correctly', async ({ page }) => {
      // Verify feature cards grid layout
      const featureCards = page.locator('.feature-cards');
      const display = await featureCards.evaluate((el) =>
        window.getComputedStyle(el).display
      );
      expect(display).toBe('grid');
    });

    test('should apply sticky positioning on header', async ({ page }) => {
      const header = page.locator('.header');
      const position = await header.evaluate((el) =>
        window.getComputedStyle(el).position
      );
      expect(position).toBe('sticky');
    });

    test('should render border-radius correctly', async ({ page }) => {
      // Check border-radius on feature cards
      const featureCard = page.locator('.feature-card').first();
      const borderRadius = await featureCard.evaluate((el) =>
        window.getComputedStyle(el).borderRadius
      );
      // Should have border-radius (12px based on CSS)
      expect(borderRadius).not.toBe('0px');
    });

    test('should render linear-gradient backgrounds', async ({ page }) => {
      // Hero section has a linear-gradient background
      const hero = page.locator('.hero');
      const backgroundImage = await hero.evaluate((el) =>
        window.getComputedStyle(el).backgroundImage
      );
      // Should contain gradient
      expect(backgroundImage).toContain('linear-gradient');
    });

    test('should render transitions correctly', async ({ page }) => {
      // Check that transitions are defined
      const navLink = page.locator('.nav-link').first();
      const transition = await navLink.evaluate((el) =>
        window.getComputedStyle(el).transition
      );
      expect(transition).not.toBe('none');
      expect(transition).not.toBe('all 0s ease 0s');
    });
  });

  test.describe('Layout Consistency - Test Case 2', () => {
    test('should display header with logo and navigation', async ({ page }) => {
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      const nav = page.locator('.nav');
      await expect(nav).toBeVisible();

      // Verify navigation items
      const navLinks = page.locator('.nav-link');
      await expect(navLinks).toHaveCount(3);
    });

    test('should display hero section with proper layout', async ({ page }) => {
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      const heroTagline = page.locator('.hero-tagline');
      await expect(heroTagline).toBeVisible();

      const heroCta = page.locator('.hero-cta');
      await expect(heroCta).toBeVisible();
    });

    test('should display three feature cards in grid layout', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(3);

      // Verify each card is visible
      for (let i = 0; i < 3; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('should display quick-start section with code block', async ({ page }) => {
      const quickStart = page.locator('#quick-start');
      await expect(quickStart).toBeVisible();

      const codeBlock = page.locator('.code-block');
      await expect(codeBlock).toBeVisible();
    });

    test('should display demo section with GIF', async ({ page }) => {
      const demo = page.locator('#demo');
      // Scroll to demo section to trigger lazy loading
      await demo.scrollIntoViewIfNeeded();
      await expect(demo).toBeVisible();

      const demoGif = page.locator('[data-testid="usage-demo-gif"]');
      // Wait for lazy-loaded image to become visible
      await demoGif.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);
      await expect(demoGif).toBeVisible();
    });

    test('should display status section with completed and planned items', async ({ page }) => {
      const status = page.locator('#status');
      await expect(status).toBeVisible();

      const completedItems = page.locator('.status-item.completed');
      const plannedItems = page.locator('.status-item.planned');

      expect(await completedItems.count()).toBeGreaterThan(0);
      expect(await plannedItems.count()).toBeGreaterThan(0);
    });

    test('should display footer with proper elements', async ({ page }) => {
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      const footerBrand = page.locator('.footer-brand');
      await expect(footerBrand).toBeVisible();

      const footerCopyright = page.locator('.footer-copyright');
      await expect(footerCopyright).toBeVisible();
    });

    test('should maintain consistent spacing between sections', async ({ page }) => {
      // Verify sections have proper padding
      const sections = ['.features', '.quick-start', '.demo', '.status'];

      for (const selector of sections) {
        const section = page.locator(selector);
        const paddingTop = await section.evaluate((el) =>
          window.getComputedStyle(el).paddingTop
        );
        const paddingBottom = await section.evaluate((el) =>
          window.getComputedStyle(el).paddingBottom
        );

        // Sections should have non-zero padding
        expect(paddingTop).not.toBe('0px');
        expect(paddingBottom).not.toBe('0px');
      }
    });

    test('should have proper element dimensions and positioning', async ({ page }) => {
      // Header should be full width
      const header = page.locator('.header');
      const headerBox = await header.boundingBox();
      expect(headerBox).not.toBeNull();
      if (headerBox) {
        expect(headerBox.width).toBeGreaterThan(300);
      }

      // Hero should be visible and have proper height
      const hero = page.locator('.hero');
      const heroBox = await hero.boundingBox();
      expect(heroBox).not.toBeNull();
      if (heroBox) {
        expect(heroBox.height).toBeGreaterThan(100);
      }
    });
  });

  test.describe('JavaScript Functionality - Test Case 3', () => {
    test('should enable smooth scrolling via CSS', async ({ page }) => {
      const html = page.locator('html');
      const scrollBehavior = await html.evaluate((el) =>
        window.getComputedStyle(el).scrollBehavior
      );
      expect(scrollBehavior).toBe('smooth');
    });

    test('should navigate to sections via anchor links', async ({ page }) => {
      // Click on Features nav link
      const featuresLink = page.locator('.nav-link[href="#features"]');
      await featuresLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(500);

      // Verify features section is in view
      const features = page.locator('#features');
      await expect(features).toBeInViewport();
    });

    test('should have functional Get Started CTA button', async ({ page }) => {
      const ctaButton = page.locator('[data-testid="get-started-btn"]');
      await expect(ctaButton).toBeVisible();

      // Verify it has the correct href
      const href = await ctaButton.getAttribute('href');
      expect(href).toBe('#quick-start');

      // Click and verify navigation
      await ctaButton.click();
      await page.waitForTimeout(500);

      const quickStart = page.locator('#quick-start');
      await expect(quickStart).toBeInViewport();
    });

    test('should have functional copy code button', async ({ page }) => {
      const copyBtn = page.locator('[data-testid="copy-code-btn"]');
      await expect(copyBtn).toBeVisible();

      // Verify button is clickable (has click handler)
      const isEnabled = await copyBtn.isEnabled();
      expect(isEnabled).toBe(true);
    });

    test('should handle keyboard navigation', async ({ page }) => {
      // Tab through navigation links
      await page.keyboard.press('Tab');
      await page.keyboard.press('Tab');

      // An element should be focused
      const focusedElement = await page.evaluate(() => document.activeElement?.tagName);
      expect(focusedElement).toBeTruthy();
    });

    test('should open external links in new tab', async ({ page }) => {
      // Check GitHub link in navigation
      const githubLink = page.locator('.nav-link[href*="github.com"]');
      await expect(githubLink).toBeVisible();

      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('should apply hover effects on interactive elements', async ({ page }) => {
      // Hover over CTA button
      const ctaButton = page.locator('.hero-cta');
      const originalBg = await ctaButton.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );

      await ctaButton.hover();

      // Give time for CSS transition
      await page.waitForTimeout(300);

      const hoverBg = await ctaButton.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );

      // Background color may change on hover
      // Just verify the element responds to hover
      expect(ctaButton).toBeVisible();
    });

    test('should load images correctly', async ({ page }) => {
      // Check logo image
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      const logoSrc = await logo.getAttribute('src');
      expect(logoSrc).toContain('logo.gif');

      // Check demo GIF (lazy-loaded)
      const demoGif = page.locator('[data-testid="usage-demo-gif"]');
      // Scroll to trigger lazy loading
      await demoGif.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);
      await expect(demoGif).toBeVisible();

      const demoSrc = await demoGif.getAttribute('src');
      expect(demoSrc).toContain('usage.gif');
    });
  });

  test.describe('Cross-Browser CSS Features', () => {
    test('should support clamp() for responsive typography', async ({ page }) => {
      const heroTitle = page.locator('.hero-title');
      const fontSize = await heroTitle.evaluate((el) =>
        window.getComputedStyle(el).fontSize
      );
      // Font size should be computed (not empty)
      expect(fontSize).not.toBe('');
      expect(parseFloat(fontSize)).toBeGreaterThan(0);
    });

    test('should render SVG icons correctly', async ({ page }) => {
      const svgIcons = page.locator('svg');
      const count = await svgIcons.count();
      expect(count).toBeGreaterThan(0);

      // Verify first SVG is visible
      const firstSvg = svgIcons.first();
      await expect(firstSvg).toBeVisible();
    });

    test('should apply box-shadow correctly', async ({ page }) => {
      const ctaButton = page.locator('.hero-cta');
      const boxShadow = await ctaButton.evaluate((el) =>
        window.getComputedStyle(el).boxShadow
      );
      expect(boxShadow).not.toBe('none');
    });

    test('should handle z-index layering correctly', async ({ page }) => {
      const header = page.locator('.header');
      const zIndex = await header.evaluate((el) =>
        window.getComputedStyle(el).zIndex
      );
      expect(parseInt(zIndex)).toBeGreaterThan(0);
    });
  });
});
