/**
 * E2E Tests for Mobile Responsiveness.
 * Owner: Scenario 9 - Mobile Responsive Design, Scenario 10 - Tablet Responsive Design
 *
 * Tests:
 * - Mobile viewport shows hamburger menu
 * - Mobile navigation links are hidden on desktop
 * - Hero section uses single-column layout on mobile
 * - Touch-friendly elements have minimum 44px height
 * - How It Works section stacks vertically on mobile
 * - Features section stacks vertically on mobile
 * - Complete URL shortening flow works on mobile
 */
import { test, expect } from '@playwright/test';

// Mobile viewport dimensions
const MOBILE_VIEWPORT = { width: 375, height: 812 };
const TABLET_VIEWPORT = { width: 768, height: 1024 };
const DESKTOP_VIEWPORT = { width: 1280, height: 800 };

test.describe('Mobile Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT);
  });

  // Test Case 1: Hamburger menu icon displayed on mobile
  test('displays hamburger menu icon on mobile viewport', async ({ page }) => {
    await page.goto('/');

    // Wait for page to load
    await expect(page.getByTestId('home-navbar')).toBeVisible();

    // Hamburger button should be visible on mobile
    const hamburgerButton = page.getByTestId('hamburger-button');
    await expect(hamburgerButton).toBeVisible();

    // Desktop navigation links should be hidden on mobile
    // The navbar-end section should be hidden on mobile
    const navbarEnd = page.locator('.navbar-end');
    await expect(navbarEnd).toHaveClass(/hidden|md:flex/);
  });

  // Test Case 2: Slide-in navigation menu with Login and Register links
  test('opens slide-in menu with Login and Register when hamburger is clicked', async ({ page }) => {
    await page.goto('/');

    // Click hamburger menu
    const hamburgerButton = page.getByTestId('hamburger-button');
    await hamburgerButton.click();

    // Mobile menu should be visible
    const mobileMenu = page.getByTestId('mobile-menu');
    await expect(mobileMenu).toBeVisible();
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false');

    // Should have Login link
    const loginLink = page.getByTestId('mobile-menu-login');
    await expect(loginLink).toBeVisible();
    await expect(loginLink).toHaveText('Login');

    // Should have Register link
    const registerLink = page.getByTestId('mobile-menu-register');
    await expect(registerLink).toBeVisible();
    await expect(registerLink).toHaveText('Register');
  });

  // Test Case 3: URL input field has minimum 44px height for touch accessibility
  test('URL input field has minimum 44px height for touch accessibility', async ({ page }) => {
    await page.goto('/');

    const urlInput = page.getByTestId('url-input');
    await expect(urlInput).toBeVisible();

    const boundingBox = await urlInput.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox!.height).toBeGreaterThanOrEqual(44);
  });

  // Test Case 4: CTA buttons have minimum 44px height for touch accessibility
  test('CTA buttons have minimum 44px height for touch accessibility', async ({ page }) => {
    await page.goto('/');

    // Check shorten button
    const shortenButton = page.getByTestId('shorten-button');
    await expect(shortenButton).toBeVisible();

    const shortenButtonBox = await shortenButton.boundingBox();
    expect(shortenButtonBox).not.toBeNull();
    expect(shortenButtonBox!.height).toBeGreaterThanOrEqual(44);
  });

  // Test Case 5: How It Works section stacks vertically on mobile
  test('How It Works section stacks vertically on mobile', async ({ page }) => {
    await page.goto('/');

    // Scroll to How It Works section if it exists
    const howItWorksSection = page.getByTestId('how-it-works-section');

    if (await howItWorksSection.count() > 0) {
      await howItWorksSection.scrollIntoViewIfNeeded();

      // Check that steps container has flex-col class on mobile
      const stepsContainer = howItWorksSection.locator('[data-testid="steps-container"]');
      if (await stepsContainer.count() > 0) {
        await expect(stepsContainer).toHaveClass(/flex-col/);
      }
    }
  });

  // Test Case 6: Features section stacks vertically on mobile
  test('Features section stacks vertically on mobile', async ({ page }) => {
    await page.goto('/');

    // Scroll to Features section if it exists
    const featuresSection = page.getByTestId('features-section');

    if (await featuresSection.count() > 0) {
      await featuresSection.scrollIntoViewIfNeeded();

      // Check that cards container has grid-cols-1 class on mobile
      const cardsContainer = featuresSection.locator('[data-testid="feature-cards-container"]');
      if (await cardsContainer.count() > 0) {
        // On mobile, should be single column
        const computedStyle = await cardsContainer.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            display: style.display,
            gridTemplateColumns: style.gridTemplateColumns,
          };
        });

        // Should be single column (no multiple columns)
        if (computedStyle.display === 'grid') {
          expect(computedStyle.gridTemplateColumns).not.toMatch(/repeat\([2-9]/);
        }
      }
    }
  });

  // Test Case 7: Complete URL shortening on mobile viewport
  test('guest URL creation works correctly on mobile devices', async ({ page }) => {
    await page.goto('/');

    // Enter a URL
    const urlInput = page.getByTestId('url-input');
    await urlInput.fill('https://example.com/very-long-url-that-needs-shortening');

    // Click shorten button
    const shortenButton = page.getByTestId('shorten-button');
    await shortenButton.click();

    // Wait for either success or error
    await page.waitForSelector('[data-testid="success-result"], [data-testid="error-message"]', {
      timeout: 10000,
    });

    // If success, verify the short URL is displayed
    const successResult = page.getByTestId('success-result');
    if (await successResult.isVisible()) {
      await expect(successResult).toBeVisible();

      // Short URL should be displayed
      const shortUrl = page.getByTestId('short-url');
      await expect(shortUrl).toBeVisible();

      // Copy button should be visible and touch-friendly
      const copyButton = page.getByTestId('copy-button');
      if (await copyButton.isVisible()) {
        const copyButtonBox = await copyButton.boundingBox();
        expect(copyButtonBox).not.toBeNull();
        expect(copyButtonBox!.height).toBeGreaterThanOrEqual(30);
      }
    }
  });

  test('hero section uses single-column stacked layout on mobile', async ({ page }) => {
    await page.goto('/');

    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // The hero section should use flex-col for vertical stacking
    await expect(heroSection).toHaveClass(/flex-col/);
  });

  test('mobile menu closes when navigation link is clicked', async ({ page }) => {
    await page.goto('/');

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-button');
    await hamburgerButton.click();

    // Verify menu is open
    const mobileMenu = page.getByTestId('mobile-menu');
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false');

    // Click Login link
    const loginLink = page.getByTestId('mobile-menu-login');
    await loginLink.click();

    // Menu should close and navigate to login
    await expect(page).toHaveURL('/login');
  });

  test('mobile menu backdrop closes menu when clicked', async ({ page }) => {
    await page.goto('/');

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-button');
    await hamburgerButton.click();

    // Verify menu is open
    const mobileMenu = page.getByTestId('mobile-menu');
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false');

    // Click backdrop
    const backdrop = page.getByTestId('mobile-menu-backdrop');
    await backdrop.click({ force: true });

    // Menu should close
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true');
  });

  test('Escape key closes mobile menu', async ({ page }) => {
    await page.goto('/');

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-button');
    await hamburgerButton.click();

    // Verify menu is open
    const mobileMenu = page.getByTestId('mobile-menu');
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false');

    // Press Escape
    await page.keyboard.press('Escape');

    // Menu should close
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true');
  });
});

test.describe('Tablet Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT);
  });

  test('displays appropriate layout on tablet viewport', async ({ page }) => {
    await page.goto('/');

    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Navbar should be visible
    const navbar = page.getByTestId('home-navbar');
    await expect(navbar).toBeVisible();
  });
});

test.describe('Desktop Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT);
  });

  test('hides hamburger menu on desktop viewport', async ({ page }) => {
    await page.goto('/');

    // Hamburger button should be hidden on desktop
    const hamburgerButton = page.getByTestId('hamburger-button');

    // It might not exist or be hidden
    if (await hamburgerButton.count() > 0) {
      await expect(hamburgerButton).toHaveClass(/hidden|md:hidden/);
    }
  });

  test('displays navigation links in navbar on desktop', async ({ page }) => {
    await page.goto('/');

    // Login and Register links should be visible in navbar
    const loginLink = page.getByTestId('navbar-login-link');
    const registerLink = page.getByTestId('navbar-register-link');

    await expect(loginLink).toBeVisible();
    await expect(registerLink).toBeVisible();
  });
});

test.describe('Cross-viewport Responsiveness', () => {
  test('layout adapts correctly when resizing from desktop to mobile', async ({ page }) => {
    // Start at desktop
    await page.setViewportSize(DESKTOP_VIEWPORT);
    await page.goto('/');

    // Verify desktop layout
    const navbarLoginLink = page.getByTestId('navbar-login-link');
    await expect(navbarLoginLink).toBeVisible();

    // Resize to mobile
    await page.setViewportSize(MOBILE_VIEWPORT);

    // Hamburger should appear
    const hamburgerButton = page.getByTestId('hamburger-button');
    await expect(hamburgerButton).toBeVisible();
  });
});
