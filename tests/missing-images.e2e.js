/**
 * Error Handling - Missing Images E2E Tests
 * Scenario: Verify page handles missing images gracefully
 *
 * Test Cases:
 * - TC1: Load page with blocked image requests - page remains functional with alt text visible
 * - TC2: Check layout with missing images - layout does not break when images fail to load
 */

import { test, expect } from '@playwright/test';

test.describe('Error Handling - Missing Images', () => {
  test.describe('Test Case 1: Page with Blocked Image Requests', () => {
    test('page should remain functional when images are blocked', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Verify page loads successfully (no crash)
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main navigation is still functional
      const header = page.locator('[data-testid="header"]');
      await expect(header).toBeVisible();

      // Verify nav links are clickable
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // Verify CTA buttons are functional
      const primaryCta = page.locator('[data-testid="hero-cta-primary"]');
      await expect(primaryCta).toBeVisible();
      await expect(primaryCta).toBeEnabled();
    });

    test('alt text should be visible when images fail to load', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Wait for page to stabilize
      await page.waitForLoadState('networkidle');

      // Check logo image has alt text
      const logoImage = page.locator('.logo-image');
      const logoAlt = await logoImage.getAttribute('alt');
      expect(logoAlt).toBeTruthy();
      expect(logoAlt.length).toBeGreaterThan(0);

      // Check hero image has alt text
      const heroImage = page.locator('[data-testid="hero-image"]');
      const heroAlt = await heroImage.getAttribute('alt');
      expect(heroAlt).toBeTruthy();
      expect(heroAlt.length).toBeGreaterThan(0);

      // Check showcase image has alt text
      const showcaseImage = page.locator('[data-testid="showcase-image"]');
      const showcaseAlt = await showcaseImage.getAttribute('alt');
      expect(showcaseAlt).toBeTruthy();
      expect(showcaseAlt.length).toBeGreaterThan(0);
    });

    test('all interactive elements should remain functional when images are blocked', async ({
      page,
    }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Test smooth scroll navigation
      const featuresLink = page.locator('a[href="#features"]').first();
      await featuresLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(500);

      // Verify features section is now in view
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeInViewport();

      // Verify footer is reachable by scrolling
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();
    });

    test('page should be scrollable when images are blocked', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Scroll to features section to test scrolling
      const featuresSection = page.locator('[data-testid="features-section"]');
      await featuresSection.scrollIntoViewIfNeeded();

      // Wait for scroll animation
      await page.waitForTimeout(300);

      // Verify the features section is now in the viewport
      await expect(featuresSection).toBeInViewport();

      // Scroll to footer to test full page scrolling
      const footer = page.locator('[data-testid="footer"]');
      await footer.scrollIntoViewIfNeeded();

      // Wait for scroll animation
      await page.waitForTimeout(300);

      // Verify footer is visible
      await expect(footer).toBeVisible();
    });
  });

  test.describe('Test Case 2: Layout with Missing Images', () => {
    test('layout should not break when images fail to load', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Verify hero section maintains its grid layout
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();

      // Hero section should still have reasonable dimensions
      expect(heroBox.width).toBeGreaterThan(300);
      expect(heroBox.height).toBeGreaterThan(100);
    });

    test('hero content should remain accessible when hero image is missing', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Verify headline is still visible and readable
      const headline = page.locator('[data-testid="hero-headline"]');
      await expect(headline).toBeVisible();

      const headlineText = await headline.textContent();
      expect(headlineText.trim()).not.toBe('');

      // Verify subheadline is still visible
      const subheadline = page.locator('[data-testid="hero-subheadline"]');
      await expect(subheadline).toBeVisible();
    });

    test('features grid should maintain structure when images are missing', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Scroll to features section
      await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

      // Verify features grid is visible
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Verify feature cards are still displayed
      const featureCards = page.locator('[data-testid="feature-card"]');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(3);

      // Verify each feature card has content
      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();

        // Check feature title exists
        const title = card.locator('[data-testid="feature-title"]');
        await expect(title).toBeVisible();

        // Check feature description exists
        const description = card.locator('[data-testid="feature-description"]');
        await expect(description).toBeVisible();
      }
    });

    test('footer should maintain proper layout when images are missing', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Scroll to footer
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      // Verify footer is visible
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // Verify footer sections are present
      const footerContact = page.locator('[data-testid="footer-contact"]');
      await expect(footerContact).toBeVisible();

      const footerSocial = page.locator('[data-testid="footer-social"]');
      await expect(footerSocial).toBeVisible();

      // Verify copyright is visible
      const copyright = page.locator('[data-testid="footer-copyright"]');
      await expect(copyright).toBeVisible();
    });

    test('no horizontal scrolling should occur when images fail to load', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Wait for page to stabilize
      await page.waitForLoadState('networkidle');

      // Check for horizontal overflow
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalOverflow).toBe(false);
    });

    test('testimonials section should display properly without images', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Scroll to testimonials section
      await page.locator('[data-testid="testimonials-section"]').scrollIntoViewIfNeeded();

      // Verify testimonials grid is visible
      const testimonialsGrid = page.locator('[data-testid="testimonials-grid"]');
      await expect(testimonialsGrid).toBeVisible();

      // Verify testimonial cards are displayed
      const testimonialCards = page.locator('[data-testid="testimonial-card"]');
      const cardCount = await testimonialCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(2);
    });

    test('pricing section should maintain structure when images are missing', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Scroll to pricing section
      await page.locator('[data-testid="pricing-section"]').scrollIntoViewIfNeeded();

      // Verify pricing grid is visible
      const pricingGrid = page.locator('[data-testid="pricing-grid"]');
      await expect(pricingGrid).toBeVisible();

      // Verify pricing cards are displayed
      const pricingCards = page.locator('[data-testid="pricing-card"]');
      const cardCount = await pricingCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(2);

      // Verify CTA buttons in pricing cards are still functional
      for (let i = 0; i < cardCount; i++) {
        const card = pricingCards.nth(i);
        const ctaButton = card.locator('.cta-button');
        await expect(ctaButton).toBeVisible();
      }
    });
  });

  test.describe('Image Element Dimensions with Missing Images', () => {
    test('images should have defined dimensions to prevent layout shift', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', (route) => {
        route.abort();
      });

      // Navigate to landing page
      await page.goto('/');

      // Check that img elements have max-width defined (from CSS)
      const heroImage = page.locator('[data-testid="hero-image"]');
      const maxWidth = await heroImage.evaluate((el) => {
        return window.getComputedStyle(el).maxWidth;
      });

      // CSS has max-width: 100% for images
      expect(maxWidth).toBe('100%');
    });
  });
});
