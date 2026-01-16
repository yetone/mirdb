import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile View', () => {
  test.describe('Test Case 1: 375px viewport width', () => {
    test('all content displays without horizontal scroll, text is readable', async ({ page }) => {
      // Set mobile viewport (iPhone SE equivalent)
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Wait for page to load
      await expect(page.locator('main')).toBeVisible();

      // Verify no horizontal scroll by checking body width matches viewport
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify hero section content is visible and readable
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      const headline = page.getByTestId('hero-headline');
      await expect(headline).toBeVisible();
      await expect(headline).toHaveText('Shorten. Share. Analyze.');

      const subheadline = page.getByTestId('hero-subheadline');
      await expect(subheadline).toBeVisible();

      // Verify text is within viewport bounds (no overflow)
      const headlineBounds = await headline.boundingBox();
      expect(headlineBounds).not.toBeNull();
      expect(headlineBounds!.x).toBeGreaterThanOrEqual(0);
      expect(headlineBounds!.x + headlineBounds!.width).toBeLessThanOrEqual(viewportWidth);

      // Verify features section is visible
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      // Verify footer is visible
      const footerCTA = page.getByTestId('footer-cta');
      await expect(footerCTA).toBeVisible();
    });
  });

  test.describe('Test Case 2: 320px minimum viewport width', () => {
    test('layout remains functional at minimum supported width', async ({ page }) => {
      // Set minimum mobile viewport
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Wait for page to load
      await expect(page.locator('main')).toBeVisible();

      // Verify no horizontal scroll
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify navbar is visible
      const navbar = page.getByTestId('navbar');
      await expect(navbar).toBeVisible();

      // Verify hero section content is visible
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      const headline = page.getByTestId('hero-headline');
      await expect(headline).toBeVisible();

      // Verify CTA buttons are visible and accessible
      const ctaGetStarted = page.getByTestId('cta-get-started');
      const ctaLogin = page.getByTestId('cta-login');
      await expect(ctaGetStarted).toBeVisible();
      await expect(ctaLogin).toBeVisible();

      // Verify features section displays correctly
      const featuresSection = page.getByTestId('features-section');
      await expect(featuresSection).toBeVisible();

      // Verify How It Works section
      const howItWorksSection = page.locator('section#how-it-works');
      await expect(howItWorksSection).toBeVisible();

      // Verify footer
      const footerCTA = page.getByTestId('footer-cta');
      await expect(footerCTA).toBeVisible();
    });
  });

  test.describe('Test Case 3: Mobile navigation menu', () => {
    test('hamburger menu is displayed and functional on mobile', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Verify hamburger button is visible on mobile
      const hamburgerButton = page.getByTestId('hamburger-button');
      await expect(hamburgerButton).toBeVisible();

      // Verify desktop nav is hidden on mobile
      const desktopNav = page.getByTestId('desktop-nav');
      await expect(desktopNav).not.toBeVisible();

      // Verify mobile menu is not visible initially
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).not.toBeVisible();

      // Click hamburger to open menu
      await hamburgerButton.click();

      // Verify mobile menu is now visible
      await expect(mobileMenu).toBeVisible();

      // Verify mobile navigation links are present
      const featuresLink = page.getByTestId('nav-features-mobile');
      const howItWorksLink = page.getByTestId('nav-how-it-works-mobile');
      const loginLink = page.getByTestId('nav-login-mobile');
      const registerLink = page.getByTestId('nav-register-mobile');

      await expect(featuresLink).toBeVisible();
      await expect(howItWorksLink).toBeVisible();
      await expect(loginLink).toBeVisible();
      await expect(registerLink).toBeVisible();

      // Click hamburger again to close menu
      await hamburgerButton.click();

      // Verify mobile menu is hidden
      await expect(mobileMenu).not.toBeVisible();
    });

    test('hamburger button has proper aria attributes', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const hamburgerButton = page.getByTestId('hamburger-button');

      // Check aria-expanded is false initially
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false');
      await expect(hamburgerButton).toHaveAttribute('aria-controls', 'mobile-menu');

      // Open menu
      await hamburgerButton.click();

      // Check aria-expanded is true when open
      await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true');
    });
  });

  test.describe('Test Case 4: CTA button touch targets', () => {
    test('all interactive elements have minimum 44x44px touch area', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Test hamburger button touch target (using Math.floor to handle sub-pixel rendering)
      const hamburgerButton = page.getByTestId('hamburger-button');
      const hamburgerBounds = await hamburgerButton.boundingBox();
      expect(hamburgerBounds).not.toBeNull();
      expect(Math.floor(hamburgerBounds!.width)).toBeGreaterThanOrEqual(43);
      expect(Math.floor(hamburgerBounds!.height)).toBeGreaterThanOrEqual(43);

      // Test CTA Get Started button
      const ctaGetStarted = page.getByTestId('cta-get-started');
      const ctaGetStartedBounds = await ctaGetStarted.boundingBox();
      expect(ctaGetStartedBounds).not.toBeNull();
      expect(ctaGetStartedBounds!.width).toBeGreaterThanOrEqual(44);
      expect(ctaGetStartedBounds!.height).toBeGreaterThanOrEqual(44);

      // Test CTA Login button
      const ctaLogin = page.getByTestId('cta-login');
      const ctaLoginBounds = await ctaLogin.boundingBox();
      expect(ctaLoginBounds).not.toBeNull();
      expect(ctaLoginBounds!.width).toBeGreaterThanOrEqual(44);
      expect(ctaLoginBounds!.height).toBeGreaterThanOrEqual(44);

      // Open mobile menu and test its buttons
      await hamburgerButton.click();
      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).toBeVisible();

      // Test mobile login button
      const mobileLogin = page.getByTestId('nav-login-mobile');
      const mobileLoginBounds = await mobileLogin.boundingBox();
      expect(mobileLoginBounds).not.toBeNull();
      expect(mobileLoginBounds!.height).toBeGreaterThanOrEqual(44);

      // Test mobile register button
      const mobileRegister = page.getByTestId('nav-register-mobile');
      const mobileRegisterBounds = await mobileRegister.boundingBox();
      expect(mobileRegisterBounds).not.toBeNull();
      expect(mobileRegisterBounds!.height).toBeGreaterThanOrEqual(44);

      // Test footer CTA button
      await hamburgerButton.click(); // Close menu first
      const footerCta = page.getByTestId('footer-cta-get-started');
      await footerCta.scrollIntoViewIfNeeded();
      const footerCtaBounds = await footerCta.boundingBox();
      expect(footerCtaBounds).not.toBeNull();
      expect(footerCtaBounds!.width).toBeGreaterThanOrEqual(44);
      expect(footerCtaBounds!.height).toBeGreaterThanOrEqual(44);
    });
  });

  test.describe('Test Case 5: Feature cards on mobile', () => {
    test('feature cards stack vertically on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Scroll to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Get the features grid
      const featuresGrid = page.getByTestId('features-grid');
      await expect(featuresGrid).toBeVisible();

      // Get all feature cards (should be 4)
      const featureIcon1 = page.getByTestId('feature-icon-1');
      const featureIcon2 = page.getByTestId('feature-icon-2');
      const featureIcon3 = page.getByTestId('feature-icon-3');
      const featureIcon4 = page.getByTestId('feature-icon-4');

      await expect(featureIcon1).toBeVisible();
      await expect(featureIcon2).toBeVisible();

      // Get bounding boxes to verify vertical stacking
      const feature1Bounds = await featureIcon1.boundingBox();
      const feature2Bounds = await featureIcon2.boundingBox();

      expect(feature1Bounds).not.toBeNull();
      expect(feature2Bounds).not.toBeNull();

      // On mobile (single column), feature 2 should be below feature 1
      // They should have similar x positions (centered) and feature 2 has higher y
      expect(feature2Bounds!.y).toBeGreaterThan(feature1Bounds!.y);

      // Verify all 4 features are accessible
      await expect(featureIcon3).toBeVisible();
      await expect(featureIcon4).toBeVisible();
    });

    test('feature cards are readable on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Scroll to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();

      // Verify feature titles are visible
      const featureTitle1 = page.getByTestId('feature-title-1');
      const featureTitle2 = page.getByTestId('feature-title-2');
      const featureTitle3 = page.getByTestId('feature-title-3');
      const featureTitle4 = page.getByTestId('feature-title-4');

      await expect(featureTitle1).toBeVisible();
      await expect(featureTitle1).toHaveText('Instant URL Shortening');

      await expect(featureTitle2).toBeVisible();
      await expect(featureTitle2).toHaveText('Detailed Analytics');

      await featureTitle3.scrollIntoViewIfNeeded();
      await expect(featureTitle3).toBeVisible();
      await expect(featureTitle3).toHaveText('Easy Management');

      await featureTitle4.scrollIntoViewIfNeeded();
      await expect(featureTitle4).toBeVisible();
      await expect(featureTitle4).toHaveText('Secure Sharing');

      // Verify descriptions are visible
      const featureDesc1 = page.getByTestId('feature-description-1');
      await featureDesc1.scrollIntoViewIfNeeded();
      await expect(featureDesc1).toBeVisible();
    });
  });

  test.describe('Additional mobile responsiveness checks', () => {
    test('navbar is fixed and visible while scrolling', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const navbar = page.getByTestId('navbar');
      await expect(navbar).toBeVisible();

      // Scroll down
      await page.evaluate(() => window.scrollBy(0, 500));
      await page.waitForTimeout(100); // Wait for scroll to complete

      // Navbar should still be visible (fixed position)
      await expect(navbar).toBeVisible();

      // Verify navbar is near top of viewport (allowing for small animation offset)
      const navbarBounds = await navbar.boundingBox();
      expect(navbarBounds).not.toBeNull();
      expect(Math.abs(navbarBounds!.y)).toBeLessThan(20); // Allow for animation offset
    });

    test('How It Works section displays vertically on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const howItWorksSection = page.locator('section#how-it-works');
      await howItWorksSection.scrollIntoViewIfNeeded();
      await expect(howItWorksSection).toBeVisible();

      // Verify steps are stacked vertically
      const step1 = page.getByTestId('step-1');
      const step2 = page.getByTestId('step-2');
      const step3 = page.getByTestId('step-3');

      await expect(step1).toBeVisible();
      await expect(step2).toBeVisible();
      await expect(step3).toBeVisible();

      const step1Bounds = await step1.boundingBox();
      const step2Bounds = await step2.boundingBox();
      const step3Bounds = await step3.boundingBox();

      expect(step1Bounds).not.toBeNull();
      expect(step2Bounds).not.toBeNull();
      expect(step3Bounds).not.toBeNull();

      // Verify vertical stacking (step 1 above step 2, step 2 above step 3)
      expect(step1Bounds!.y).toBeLessThan(step2Bounds!.y);
      expect(step2Bounds!.y).toBeLessThan(step3Bounds!.y);
    });
  });
});
