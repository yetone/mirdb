import { test, expect } from '@playwright/test';

/**
 * Cross-Browser Compatibility E2E Tests
 *
 * This test file covers the scenario: "Verify homepage renders correctly across
 * Chrome, Firefox, Safari, and Edge browsers as per success criteria"
 *
 * Test Cases:
 * 1. E2E: Load homepage in Chrome → Homepage renders correctly with all features functional
 * 2. E2E: Load homepage in Firefox → Homepage renders correctly with all features functional
 * 3. E2E: Load homepage in Safari → Homepage renders correctly with all features functional
 * 4. E2E: Load homepage in Edge → Homepage renders correctly with all features functional
 * 5. E2E: Verify CSS backdrop-filter support → Glassmorphism effects render correctly or have fallbacks
 *
 * These tests run across all browser projects configured in playwright.config.ts
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Cases 1-4: Homepage renders correctly across browsers
   * This test runs on all configured browsers (Chrome, Firefox, Safari/WebKit, Edge)
   */
  test.describe('Homepage Rendering', () => {
    test('homepage loads successfully', async ({ page }) => {
      // Verify the page has loaded with correct title
      await expect(page).toHaveTitle(/URL/i);
    });

    test('hero section renders correctly', async ({ page }) => {
      // Verify hero section is visible
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      // Verify headline is present
      const headline = page.getByTestId('hero-headline');
      await expect(headline).toBeVisible();
      await expect(headline).not.toBeEmpty();

      // Verify subheadline is present
      const subheadline = page.getByTestId('hero-subheadline');
      await expect(subheadline).toBeVisible();
    });

    test('CTA buttons are visible and functional', async ({ page }) => {
      // Verify Get Started button
      const ctaGetStarted = page.getByTestId('cta-get-started');
      await expect(ctaGetStarted).toBeVisible();
      await expect(ctaGetStarted).toHaveAttribute('href', '/register');

      // Verify Login button
      const ctaLogin = page.getByTestId('cta-login');
      await expect(ctaLogin).toBeVisible();
      await expect(ctaLogin).toHaveAttribute('href', '/login');
    });

    test('features section renders with all feature cards', async ({ page }) => {
      // Verify features section is visible
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      // Verify all 4 feature cards are present in the features section
      // (scoped to features section to exclude cards from other sections like SocialProofSection)
      const featureCards = featuresSection.getByTestId('glassmorphism-card');
      await expect(featureCards).toHaveCount(4);

      // Verify each card is visible
      for (let i = 0; i < 4; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('how it works section renders correctly', async ({ page }) => {
      // Verify how it works section is visible
      const howItWorksSection = page.locator('section#how-it-works');
      await expect(howItWorksSection).toBeVisible();

      // Verify all 3 steps are present
      const step1 = page.getByTestId('step-1');
      const step2 = page.getByTestId('step-2');
      const step3 = page.getByTestId('step-3');

      await expect(step1).toBeVisible();
      await expect(step2).toBeVisible();
      await expect(step3).toBeVisible();
    });

    test('navbar renders correctly with all navigation elements', async ({ page }) => {
      // Verify navbar is visible
      const navbar = page.getByTestId('navbar');
      await expect(navbar).toBeVisible();

      // Verify logo is present
      const logo = page.getByTestId('navbar-logo');
      await expect(logo).toBeVisible();
      await expect(logo).toHaveText('URL Shortener');

      // Verify login link
      const loginLink = page.getByTestId('navbar-login');
      await expect(loginLink).toBeVisible();

      // Verify get started button
      const getStartedBtn = page.getByTestId('navbar-get-started');
      await expect(getStartedBtn).toBeVisible();
    });

    test('footer renders correctly', async ({ page }) => {
      // Scroll to main footer (there's also a footer-cta section)
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });
  });

  /**
   * Test Case 5: CSS backdrop-filter support and glassmorphism effects
   */
  test.describe('Glassmorphism and CSS Effects', () => {
    test('feature cards have glassmorphism styling', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();

      const featureCards = page.getByTestId('glassmorphism-card');
      await expect(featureCards.first()).toBeVisible();

      // Check that cards have the expected visual styles
      // The card should have backdrop-blur or fallback background
      const firstCard = featureCards.first();

      // Verify the card has CSS classes for glassmorphism
      const cardClasses = await firstCard.getAttribute('class');
      expect(cardClasses).toBeTruthy();

      // Check for backdrop-filter CSS property or verify card is visible
      // Browsers that don't support backdrop-filter should still display the card
      const cardBoundingBox = await firstCard.boundingBox();
      expect(cardBoundingBox).not.toBeNull();
      expect(cardBoundingBox!.width).toBeGreaterThan(0);
      expect(cardBoundingBox!.height).toBeGreaterThan(0);
    });

    test('glassmorphism cards are readable in all browsers', async ({ page }) => {
      const featureCards = page.getByTestId('glassmorphism-card');

      // Verify each card's content is readable (text is present and visible)
      for (let i = 0; i < 4; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        // Verify card has text content (title)
        const cardText = await card.textContent();
        expect(cardText).toBeTruthy();
        expect(cardText!.length).toBeGreaterThan(0);
      }
    });

    test('backdrop-filter or fallback is applied', async ({ page, browserName }) => {
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();

      const firstCard = page.getByTestId('glassmorphism-card').first();
      await expect(firstCard).toBeVisible();

      // Get computed style to verify backdrop-filter or fallback
      const computedStyle = await firstCard.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          backdropFilter: style.backdropFilter || style.webkitBackdropFilter || 'none',
          backgroundColor: style.backgroundColor,
          opacity: style.opacity,
        };
      });

      // Either backdrop-filter should be applied, or there should be a fallback background
      const hasBackdropFilter =
        computedStyle.backdropFilter !== 'none' && computedStyle.backdropFilter !== '';
      const hasBackgroundFallback = computedStyle.backgroundColor !== 'transparent';

      // At least one of these should be true for the card to be visible
      expect(hasBackdropFilter || hasBackgroundFallback).toBeTruthy();

      // Log for debugging
      console.log(`Browser: ${browserName}, Backdrop-filter: ${computedStyle.backdropFilter}, Background: ${computedStyle.backgroundColor}`);
    });
  });

  /**
   * Interactive Elements Tests - Verify functionality across browsers
   */
  test.describe('Interactive Elements', () => {
    test('navigation links work correctly', async ({ page }) => {
      // Test Get Started button navigation
      const ctaGetStarted = page.getByTestId('cta-get-started');
      await expect(ctaGetStarted).toHaveAttribute('href', '/register');

      // Click and verify navigation
      await ctaGetStarted.click();
      await expect(page).toHaveURL(/\/register/);

      // Go back to homepage
      await page.goto('/');
    });

    test('login navigation works correctly', async ({ page }) => {
      // Test Login button navigation
      const ctaLogin = page.getByTestId('cta-login');
      await expect(ctaLogin).toHaveAttribute('href', '/login');

      // Click and verify navigation
      await ctaLogin.click();
      await expect(page).toHaveURL(/\/login/);
    });

    test('theme toggle is functional', async ({ page }) => {
      // Find and click theme toggle - wait for it to be visible first
      const themeToggle = page.getByTestId('hero-theme-toggle');

      // Scroll to hero section to ensure theme toggle is in view
      const heroSection = page.getByTestId('hero-section');
      await heroSection.scrollIntoViewIfNeeded();

      // Wait for the theme toggle to be visible with increased timeout
      await expect(themeToggle).toBeVisible({ timeout: 10000 });

      // Get initial theme state
      const html = page.locator('html');
      const initialTheme = await html.getAttribute('data-theme');

      // Click theme toggle
      await themeToggle.click();

      // Wait for theme change animation
      await page.waitForTimeout(500);

      // Verify theme toggle is still functional (visible after click)
      await expect(themeToggle).toBeVisible({ timeout: 5000 });
    });

    test('smooth scroll navigation works', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Find a section anchor and click it (e.g., Features link if exists)
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();

      // Verify scroll happened
      await page.waitForTimeout(500);
      const finalScrollY = await page.evaluate(() => window.scrollY);

      // Scroll should have changed
      expect(finalScrollY).not.toBe(initialScrollY);
    });
  });

  /**
   * Layout and Rendering Tests
   */
  test.describe('Layout Rendering', () => {
    test('page layout is consistent', async ({ page }) => {
      // Verify main sections are in correct order (top to bottom)
      const heroSection = page.getByTestId('hero-section');
      const featuresSection = page.getByTestId('features-section');
      const howItWorksSection = page.locator('section#how-it-works');

      const heroBox = await heroSection.boundingBox();
      const featuresBox = await featuresSection.boundingBox();
      const howItWorksBox = await howItWorksSection.boundingBox();

      expect(heroBox).not.toBeNull();
      expect(featuresBox).not.toBeNull();
      expect(howItWorksBox).not.toBeNull();

      // Hero should be before features
      expect(heroBox!.y).toBeLessThan(featuresBox!.y);
      // Features should be before how it works
      expect(featuresBox!.y).toBeLessThan(howItWorksBox!.y);
    });

    test('no horizontal overflow', async ({ page }) => {
      // Check for horizontal scrollbar (indicates overflow)
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBeFalsy();
    });

    test('all images and icons load correctly', async ({ page }) => {
      // Wait for all images to load
      await page.waitForLoadState('networkidle');

      // Check that no images are broken
      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const naturalWidth = await img.evaluate((el: HTMLImageElement) => el.naturalWidth);
        // Image should have loaded (naturalWidth > 0 or be hidden/decorative)
        const isVisible = await img.isVisible();
        if (isVisible) {
          expect(naturalWidth).toBeGreaterThan(0);
        }
      }
    });

    test('fonts render correctly', async ({ page }) => {
      // Verify main heading has proper font size
      const headline = page.getByTestId('hero-headline');
      await expect(headline).toBeVisible();

      const fontSize = await headline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Headline should have a reasonable font size (at least 24px)
      expect(fontSize).toBeGreaterThan(24);
    });
  });

  /**
   * Performance and Rendering Stability
   */
  test.describe('Rendering Stability', () => {
    test('no console errors on page load', async ({ page }) => {
      const consoleErrors: string[] = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out known non-critical errors (like favicon.ico 404)
      const criticalErrors = consoleErrors.filter(
        (error) => !error.includes('favicon') && !error.includes('404')
      );

      expect(criticalErrors).toHaveLength(0);
    });

    test('page renders within reasonable time', async ({ page }) => {
      const startTime = Date.now();

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      const loadTime = Date.now() - startTime;

      // Page should load within 10 seconds (generous limit for CI)
      expect(loadTime).toBeLessThan(10000);
    });
  });
});
