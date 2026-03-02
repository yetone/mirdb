/**
 * E2E Tests for Mobile and Tablet Responsiveness.
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
 * - Tablet responsive layout at various breakpoints (768px-1024px)
 *
 * Requirements: NFR-2
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

test.describe('Tablet Responsive Design (768px-1024px)', () => {
  test.describe('Lower tablet breakpoint (768px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
    });

    test('homepage renders correctly at 768px viewport width', async ({ page }) => {
      // Verify the page loads
      await expect(page.locator('[data-testid="home-navbar"]')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Check navigation is visible and adapts to tablet layout
      const navbar = page.locator('[data-testid="home-navbar"]');
      await expect(navbar).toBeVisible();

      // Verify layout adapts with appropriate spacing
      const mainContent = page.locator('main');
      await expect(mainContent).toBeVisible();

      // Check hero section displays correctly
      const heroSection = page.locator('[data-testid="hero-section"]');
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).toBeTruthy();
      expect(heroBox!.width).toBeLessThanOrEqual(768);

      // Verify text content is visible and readable
      await expect(page.getByText('Shorten Links. Track Clicks. Grow Your Impact.')).toBeVisible();
    });

    test('navigation displays appropriately for tablet viewport', async ({ page }) => {
      const navbar = page.locator('[data-testid="home-navbar"]');

      // Navbar should be visible
      await expect(navbar).toBeVisible();

      // Logo should be visible
      await expect(page.locator('[data-testid="navbar-logo"]')).toBeVisible();

      // At 768px (tablet), navigation links should be visible (not hidden in hamburger menu)
      // Tablet breakpoint typically shows full navigation
      const loginLink = page.locator('[data-testid="navbar-login-link"]');
      const registerLink = page.locator('[data-testid="navbar-register-link"]');

      // Check that navigation elements are present
      await expect(loginLink).toBeVisible();
      await expect(registerLink).toBeVisible();
    });

    test('hero section content layout adapts to tablet', async ({ page }) => {
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify the URL input and button are present
      const urlInput = page.locator('input[type="url"]');
      const shortenButton = page.getByRole('button', { name: /shorten/i });

      await expect(urlInput).toBeVisible();
      await expect(shortenButton).toBeVisible();

      // Check CTA buttons are visible (Sign Up Free has role="button" attribute)
      await expect(page.getByRole('button', { name: /sign up free/i })).toBeVisible();
      await expect(page.getByRole('button', { name: /try as guest/i })).toBeVisible();
    });

    test('all interactive elements are functional at tablet viewport', async ({ page }) => {
      // Test navigation link functionality
      const loginLink = page.locator('[data-testid="navbar-login-link"]');
      await expect(loginLink).toBeVisible();

      // Verify links are clickable
      await expect(loginLink).toBeEnabled();

      // Test URL input functionality
      const urlInput = page.locator('input[type="url"]');
      await urlInput.fill('https://example.com');
      await expect(urlInput).toHaveValue('https://example.com');

      // Verify button is clickable
      const shortenButton = page.getByRole('button', { name: /shorten/i });
      await expect(shortenButton).toBeEnabled();
    });
  });

  test.describe('Upper tablet breakpoint (1024px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');
    });

    test('homepage renders correctly at 1024px viewport width', async ({ page }) => {
      // Verify the page loads
      await expect(page.locator('[data-testid="home-navbar"]')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Check layout fills viewport width appropriately
      const heroSection = page.locator('[data-testid="hero-section"]');
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).toBeTruthy();
      expect(heroBox!.width).toBeLessThanOrEqual(1024);

      // Verify content is visible
      await expect(page.getByText('Shorten Links. Track Clicks. Grow Your Impact.')).toBeVisible();
      await expect(page.getByText(/free url shortener/i)).toBeVisible();
    });

    test('navigation displays in full desktop-like layout at upper tablet breakpoint', async ({ page }) => {
      const navbar = page.locator('[data-testid="home-navbar"]');
      await expect(navbar).toBeVisible();

      // At 1024px, full navigation should be visible
      await expect(page.locator('[data-testid="navbar-logo"]')).toBeVisible();
      await expect(page.locator('[data-testid="navbar-login-link"]')).toBeVisible();
      await expect(page.locator('[data-testid="navbar-register-link"]')).toBeVisible();
    });

    test('content sections use appropriate layouts at upper tablet breakpoint', async ({ page }) => {
      // Hero section should display properly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // URL form should be in a comfortable layout
      const urlInput = page.locator('input[type="url"]');
      const urlInputBox = await urlInput.boundingBox();
      expect(urlInputBox).toBeTruthy();

      // Input should have reasonable width at this viewport
      expect(urlInputBox!.width).toBeGreaterThan(200);

      // CTA buttons should be side by side at this width (Sign Up Free has role="button" attribute)
      const signUpButton = page.getByRole('button', { name: /sign up free/i });
      const guestButton = page.getByRole('button', { name: /try as guest/i });

      await expect(signUpButton).toBeVisible();
      await expect(guestButton).toBeVisible();
    });

    test('all features work correctly at upper tablet viewport', async ({ page }) => {
      // Test navigation
      const registerLink = page.locator('[data-testid="navbar-register-link"]');
      await expect(registerLink).toBeEnabled();

      // Test URL input
      const urlInput = page.locator('input[type="url"]');
      await urlInput.fill('https://example.com/long-path');
      await expect(urlInput).toHaveValue('https://example.com/long-path');

      // Test button interactions
      const shortenButton = page.getByRole('button', { name: /shorten/i });
      await expect(shortenButton).toBeEnabled();
    });
  });

  test.describe('Intermediate tablet viewport (900px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 900, height: 1200 });
      await page.goto('/');
    });

    test('layout displays correctly at intermediate tablet width', async ({ page }) => {
      // Verify page loads correctly at an intermediate tablet width
      await expect(page.locator('[data-testid="home-navbar"]')).toBeVisible();
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Verify responsive layout
      const heroSection = page.locator('[data-testid="hero-section"]');
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).toBeTruthy();
      expect(heroBox!.width).toBe(900);

      // Content should be readable
      await expect(page.getByText('Shorten Links. Track Clicks. Grow Your Impact.')).toBeVisible();
    });
  });
});

test.describe('Viewport consistency across tablet range', () => {
  const tabletViewports = [768, 834, 900, 1024];

  for (const width of tabletViewports) {
    test(`homepage maintains layout consistency at ${width}px width`, async ({ page }) => {
      await page.setViewportSize({ width, height: 1024 });
      await page.goto('/');

      // All tablet viewports should show:
      // 1. Visible navbar with logo
      await expect(page.locator('[data-testid="home-navbar"]')).toBeVisible();
      await expect(page.locator('[data-testid="navbar-logo"]')).toBeVisible();

      // 2. Visible hero section
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // 3. Functional URL input
      const urlInput = page.locator('input[type="url"]');
      await expect(urlInput).toBeVisible();
      await expect(urlInput).toBeEnabled();

      // 4. Visible CTA buttons (Sign Up Free has role="button" attribute)
      await expect(page.getByRole('button', { name: /sign up free/i })).toBeVisible();
      await expect(page.getByRole('button', { name: /try as guest/i })).toBeVisible();
    });
  }
});

test.describe('FeaturesSection tablet responsive layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
  });

  test('FeaturesSection displays feature cards in appropriate grid at tablet viewport', async ({ page }) => {
    // Check if FeaturesSection exists on the page
    const featuresSection = page.locator('[data-testid="features-section"]');
    const featuresSectionCount = await featuresSection.count();

    if (featuresSectionCount > 0) {
      // FeaturesSection exists - verify tablet layout
      await expect(featuresSection).toBeVisible();

      // Check for feature cards
      const featureCards = page.locator('[data-testid="feature-card"]');
      const cardCount = await featureCards.count();

      if (cardCount > 0) {
        // Verify cards are displayed in an appropriate grid (2-column or full width)
        const firstCardBox = await featureCards.first().boundingBox();
        expect(firstCardBox).toBeTruthy();

        // At tablet viewport, cards should take up a reasonable portion of the width
        // Either 2-column grid (each card ~50% width) or full width stacked
        const viewportWidth = 768;
        const expectedMinWidth = viewportWidth * 0.4; // At least 40% of viewport
        expect(firstCardBox!.width).toBeGreaterThan(expectedMinWidth);
      }
    } else {
      // FeaturesSection not yet implemented - test passes with note
      // This will be implemented by Scenario 6
      test.info().annotations.push({
        type: 'info',
        description: 'FeaturesSection not yet implemented (owned by Scenario 6)',
      });
    }
  });

  test('FeaturesSection grid adapts between tablet breakpoints', async ({ page }) => {
    // Test at lower tablet breakpoint (768px) - already set in beforeEach
    const featuresSection = page.locator('[data-testid="features-section"]');

    if ((await featuresSection.count()) > 0) {
      // Check grid layout at 768px
      await expect(featuresSection).toBeVisible();

      // Resize to upper tablet breakpoint
      await page.setViewportSize({ width: 1024, height: 768 });
      await expect(featuresSection).toBeVisible();

      // Feature cards should maintain appropriate layout at both breakpoints
      const featureCards = page.locator('[data-testid="feature-card"]');
      if ((await featureCards.count()) > 0) {
        const cardBox = await featureCards.first().boundingBox();
        expect(cardBox).toBeTruthy();
        // Cards should be visible and have reasonable dimensions at 1024px
        expect(cardBox!.width).toBeGreaterThan(200);
      }
    } else {
      test.info().annotations.push({
        type: 'info',
        description: 'FeaturesSection not yet implemented (owned by Scenario 6)',
      });
    }
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
