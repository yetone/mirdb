/**
 * E2E Tests for JavaScript Disabled Fallback
 *
 * This test suite verifies that the landing page works correctly when JavaScript
 * is disabled, following the progressive enhancement principle from the PRD.
 *
 * Key technical constraint from PRD:
 * "Must work without JavaScript for core content (progressive enhancement)"
 */
import { test, expect } from '@playwright/test';

test.describe('JavaScript Disabled Fallback', () => {
  // Configure all tests in this file to run with JavaScript disabled
  test.use({ javaScriptEnabled: false });

  test.describe('Test Case 1: Core content visibility without JavaScript', () => {
    test('hero section is visible and contains key elements', async ({ page }) => {
      await page.goto('/');

      // Wait for page to load (no JS, so just wait for content)
      await page.waitForLoadState('domcontentloaded');

      // Hero section should be visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Hero headline should be visible
      const headline = page.locator('.hero-headline');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Lightning-Fast Persistent Key-Value Storage');

      // Hero subheadline should be visible
      const subheadline = page.locator('.hero-subheadline');
      await expect(subheadline).toBeVisible();

      // CTA buttons should be visible
      const ctaPrimary = page.locator('.cta-primary').first();
      await expect(ctaPrimary).toBeVisible();

      const ctaSecondary = page.locator('.cta-secondary').first();
      await expect(ctaSecondary).toBeVisible();
    });

    test('features section is visible and displays all feature cards', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Features section should be visible
      const featuresSection = page.locator('.features');
      await expect(featuresSection).toBeVisible();

      // Section heading should be visible
      const featuresHeading = page.locator('.features h2');
      await expect(featuresHeading).toBeVisible();
      await expect(featuresHeading).toContainText('Why Choose MirDB?');

      // All feature cards should be visible (3 cards expected)
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(3);

      // Each feature card should have icon, title, and description
      for (let i = 0; i < 3; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();
        await expect(card.locator('.feature-icon')).toBeVisible();
        await expect(card.locator('h3')).toBeVisible();
        await expect(card.locator('p')).toBeVisible();
      }
    });

    test('footer section is visible with all content', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Footer should be visible
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      // Footer sections should be present
      const footerSections = page.locator('.footer-section');
      await expect(footerSections).toHaveCount(3);

      // Footer bottom with copyright should be visible
      const footerBottom = page.locator('.footer-bottom');
      await expect(footerBottom).toBeVisible();
      await expect(footerBottom).toContainText('MirDB');
    });

    test('social proof/testimonials section is visible', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Social proof section should be visible
      const socialProof = page.locator('.social-proof');
      await expect(socialProof).toBeVisible();

      // Testimonials should be present
      const testimonials = page.locator('.testimonial');
      await expect(testimonials).toHaveCount(3);

      // Each testimonial should have image, quote, and attribution
      for (let i = 0; i < 3; i++) {
        const testimonial = testimonials.nth(i);
        await expect(testimonial).toBeVisible();
        await expect(testimonial.locator('.testimonial-quote')).toBeVisible();
        await expect(testimonial.locator('.testimonial-name')).toBeVisible();
      }
    });

    test('product showcase section is visible', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Product showcase should be visible
      const showcase = page.locator('.product-showcase');
      await expect(showcase).toBeVisible();

      // Showcase heading should be visible
      const showcaseHeading = page.locator('.product-showcase h2');
      await expect(showcaseHeading).toBeVisible();
    });

    test('CTA section is visible', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // CTA section should be visible
      const ctaSection = page.locator('.cta-section');
      await expect(ctaSection).toBeVisible();

      // CTA heading should be visible
      const ctaHeading = page.locator('.cta-section h2');
      await expect(ctaHeading).toBeVisible();

      // CTA buttons should be visible
      const ctaButtons = page.locator('.cta-section .cta-primary, .cta-section .cta-secondary');
      await expect(ctaButtons.first()).toBeVisible();
    });
  });

  test.describe('Test Case 2: Navigation functionality without JavaScript', () => {
    test('header navigation is visible', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Header should be visible
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Logo should be visible
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Navigation links should be visible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Nav CTA should be visible
      const navCta = page.locator('.nav-cta');
      await expect(navCta).toBeVisible();
    });

    test('navigation links have correct href attributes', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check Features link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveAttribute('href', '#features');

      // Check Documentation link
      const docsLink = page.locator('.nav-links a[href="#docs"]');
      await expect(docsLink).toBeVisible();
      await expect(docsLink).toHaveAttribute('href', '#docs');

      // Check Community link
      const communityLink = page.locator('.nav-links a[href="#community"]');
      await expect(communityLink).toBeVisible();
      await expect(communityLink).toHaveAttribute('href', '#community');

      // Check Get Started CTA link
      const getStartedLink = page.locator('.nav-cta');
      await expect(getStartedLink).toBeVisible();
      await expect(getStartedLink).toHaveAttribute('href', '#get-started');
    });

    test('anchor links navigate to correct sections without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Navigate to features section using anchor link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await featuresLink.click();

      // The URL should now contain #features
      await expect(page).toHaveURL(/#features/);

      // The features section should be in the viewport (browser native anchor behavior)
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    });

    test('footer navigation links are functional', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Footer documentation link should work
      const docsLink = page.locator('.footer a[href="#docs"]');
      await expect(docsLink).toBeVisible();

      // GitHub link (external) should have correct href
      const githubLink = page.locator('.footer a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Contact email link should work
      const emailLink = page.locator('.footer a[href="mailto:contact@mirdb.io"]');
      await expect(emailLink).toBeVisible();
      await expect(emailLink).toHaveAttribute('href', 'mailto:contact@mirdb.io');
    });

    test('CTA buttons in hero section are accessible without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // The secondary CTA (View Demo) should be an anchor link that works
      const viewDemoLink = page.locator('.hero .cta-secondary');
      await expect(viewDemoLink).toBeVisible();
      await expect(viewDemoLink).toHaveAttribute('href', '#demo');
    });

    test('CTA section links work without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Primary CTA in CTA section links to GitHub
      const primaryCta = page.locator('.cta-section .cta-primary');
      await expect(primaryCta).toBeVisible();
      await expect(primaryCta).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Secondary CTA links to docs
      const secondaryCta = page.locator('.cta-section .cta-secondary');
      await expect(secondaryCta).toBeVisible();
      await expect(secondaryCta).toHaveAttribute('href', '#docs');
    });
  });

  test.describe('Page structure and semantic HTML without JavaScript', () => {
    test('page has proper semantic structure', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Header element exists
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Main element exists
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Footer element exists
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Nav element exists
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('heading hierarchy is maintained without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // H1 should be present (only one)
      const h1Elements = page.locator('h1');
      await expect(h1Elements).toHaveCount(1);

      // H2 elements should be present for sections
      const h2Elements = page.locator('h2');
      expect(await h2Elements.count()).toBeGreaterThanOrEqual(3);

      // H3 elements should be present for feature cards
      const h3Elements = page.locator('h3');
      expect(await h3Elements.count()).toBeGreaterThanOrEqual(3);
    });

    test('images have alt text and are visible without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check all images have alt attributes
      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const altText = await img.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.length).toBeGreaterThan(0);
      }
    });
  });

  test.describe('CSS styling without JavaScript', () => {
    test('CSS styles are applied correctly without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Header should have fixed positioning (sticky header)
      const header = page.locator('.header');
      const headerPosition = await header.evaluate(el =>
        window.getComputedStyle(el).position
      );
      expect(headerPosition).toBe('fixed');

      // Hero section should have proper styling
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // CTA buttons should have distinct styling
      const primaryCta = page.locator('.cta-primary').first();
      const primaryBgColor = await primaryCta.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      expect(primaryBgColor).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('responsive layout is functional without JavaScript', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Check that features grid uses CSS Grid or Flexbox
      const featuresGrid = page.locator('.features-grid');
      const gridDisplay = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).display
      );
      expect(['grid', 'flex']).toContain(gridDisplay);
    });
  });
});
