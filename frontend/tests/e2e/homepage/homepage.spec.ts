/**
 * Homepage Integration E2E Tests
 * Owner: Scenario 8 - Homepage Integration and Assembly
 *
 * Verifies the complete homepage integrates all sections correctly and functions as expected.
 * Tests all homepage requirements (All REQ, All US).
 *
 * Test Cases:
 * 1. Navigate to '/' route - Homepage component is rendered
 * 2. Check page structure has all sections - Navbar, Hero, Features, CTA, and Footer
 * 3. Verify BackgroundEffect component is rendered
 * 4. Click 'Get Started' CTA - Navigate to /register
 * 5. Click 'Login' in navbar - Navigate to /login
 * 6. Verify proper heading hierarchy (h1, h2, h3)
 * 7. Check keyboard navigation through interactive elements
 * 8. Verify page load completes within performance budget
 */

import { test, expect, type Page } from '@playwright/test';

// Performance budget for page load (in milliseconds)
const PERFORMANCE_BUDGET_MS = 5000;

/**
 * Helper to verify heading hierarchy follows semantic order
 */
async function verifyHeadingHierarchy(page: Page): Promise<boolean> {
  const headings = await page.evaluate(() => {
    const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const levels: number[] = [];
    headingElements.forEach((h) => {
      const level = parseInt(h.tagName.charAt(1), 10);
      levels.push(level);
    });
    return levels;
  });

  // Check that headings don't skip levels (e.g., h1 -> h3 without h2)
  // Allow h1 to be followed by h2 or h3 for flexibility in design
  if (headings.length === 0) return false;
  if (headings[0] !== 1) return false; // Must start with h1

  // Check that we don't skip more than one level
  for (let i = 1; i < headings.length; i++) {
    const diff = headings[i] - headings[i - 1];
    // Allow going down (positive) by max 1 level, or going up (negative) any amount
    if (diff > 1) {
      return false;
    }
  }

  return true;
}

/**
 * Helper to check if element is keyboard focusable
 */
async function isKeyboardFocusable(page: Page, selector: string): Promise<boolean> {
  return page.evaluate((sel) => {
    const element = document.querySelector(sel);
    if (!element) return false;

    const tabIndex = element.getAttribute('tabindex');
    const tagName = element.tagName.toLowerCase();
    const isNativelyFocusable = ['a', 'button', 'input', 'select', 'textarea'].includes(tagName);

    // Element is focusable if it's natively focusable or has tabindex >= 0
    return isNativelyFocusable || (tabIndex !== null && parseInt(tabIndex) >= 0);
  }, selector);
}

test.describe('Homepage Integration - Page Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Navigate to '/' route
   * Expected: Homepage component is rendered
   */
  test('TC1: should render homepage when navigating to root URL', async ({ page }) => {
    // Verify homepage container is present
    await expect(page.locator('[data-testid="homepage"]')).toBeVisible();

    // Verify page title or main content is present
    await expect(page.locator('h1')).toBeVisible();

    // Check that the main headline text is displayed
    await expect(
      page.getByText(/shorten urls\. track every click/i)
    ).toBeVisible();
  });

  /**
   * Test Case 2: Check page structure has all sections
   * Expected: Navbar, Hero, Features, CTA, and Footer sections are all present
   */
  test('TC2: should display all required homepage sections', async ({ page }) => {
    // Verify Navbar is present (main nav with logo, not footer nav)
    const navbar = page.locator('nav.navbar');
    await expect(navbar).toBeVisible();

    // Check Navbar has logo, login, register links
    await expect(page.locator('[data-testid="logo-link"]')).toBeVisible();
    await expect(page.locator('[data-testid="login-link"]')).toBeVisible();
    await expect(page.locator('[data-testid="register-button"]')).toBeVisible();

    // Verify Hero Section is present
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-headline"]')).toBeVisible();
    await expect(page.locator('[data-testid="hero-subheadline"]')).toBeVisible();

    // Verify Features Section is present
    await expect(page.locator('[data-testid="features-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-cards-grid"]')).toBeVisible();

    // Verify CTA Section is present
    await expect(page.locator('[data-testid="cta-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="cta-headline"]')).toBeVisible();

    // Verify Footer is present
    await expect(page.locator('[data-testid="footer-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-copyright"]')).toBeVisible();
  });

  /**
   * Test Case 3: Verify BackgroundEffect component is rendered
   * Expected: Background visual effects are visible
   */
  test('TC3: should render BackgroundEffect component', async ({ page }) => {
    // BackgroundEffect renders a canvas element
    const canvas = page.locator('canvas');
    await expect(canvas).toBeVisible();

    // Canvas should be positioned as a background element
    const canvasClasses = await canvas.getAttribute('class');
    expect(canvasClasses).toContain('fixed');
    expect(canvasClasses).toContain('inset-0');

    // Canvas should be behind other content (negative z-index)
    expect(canvasClasses).toContain('-z-10');
  });
});

test.describe('Homepage Integration - Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 4: Click 'Get Started' CTA on homepage
   * Expected: User is navigated to /register page
   */
  test('TC4: should navigate to register page when clicking Get Started CTA', async ({
    page,
  }) => {
    // Find and click the primary CTA in hero section
    const primaryCTA = page.locator('[data-testid="hero-primary-cta"]');
    await expect(primaryCTA).toBeVisible();
    await primaryCTA.click();

    // Verify navigation to register page
    await expect(page).toHaveURL(/\/register/);

    // Verify register page content is displayed - form should be present
    await expect(page.locator('form')).toBeVisible();
    await expect(page.getByText('Create your free account')).toBeVisible();
  });

  /**
   * Test Case 4b: Click 'Create Account' CTA in secondary CTA section
   * Expected: User is navigated to /register page
   */
  test('TC4b: should navigate to register page when clicking secondary CTA', async ({
    page,
  }) => {
    // Scroll to CTA section
    await page.locator('[data-testid="cta-section"]').scrollIntoViewIfNeeded();

    // Find and click the CTA button in the CTA section
    const ctaButton = page.locator('[data-testid="cta-button"]');
    await expect(ctaButton).toBeVisible();
    await ctaButton.click();

    // Verify navigation to register page
    await expect(page).toHaveURL(/\/register/);
  });

  /**
   * Test Case 5: Click 'Login' in navbar from homepage
   * Expected: User is navigated to /login page
   */
  test('TC5: should navigate to login page when clicking Login link', async ({
    page,
  }) => {
    // Find and click the login link in navbar
    const loginLink = page.locator('[data-testid="login-link"]');
    await expect(loginLink).toBeVisible();
    await loginLink.click();

    // Verify navigation to login page
    await expect(page).toHaveURL(/\/login/);

    // Verify login page content is displayed - form should be present
    await expect(page.locator('form')).toBeVisible();
    await expect(page.getByText('Sign in to your account')).toBeVisible();
  });

  /**
   * Test Case 5b: Click 'Sign In' secondary CTA in hero section
   * Expected: User is navigated to /login page
   */
  test('TC5b: should navigate to login page when clicking Sign In CTA in hero', async ({
    page,
  }) => {
    // Find and click the secondary CTA in hero section
    const secondaryCTA = page.locator('[data-testid="hero-secondary-cta"]');
    await expect(secondaryCTA).toBeVisible();
    await secondaryCTA.click();

    // Verify navigation to login page
    await expect(page).toHaveURL(/\/login/);
  });
});

test.describe('Homepage Integration - Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 6: Verify proper heading hierarchy (h1, h2, h3)
   * Expected: Headings follow semantic hierarchy for accessibility
   */
  test('TC6: should have proper heading hierarchy for accessibility', async ({
    page,
  }) => {
    // Wait for page content to load
    await page.waitForSelector('h1');

    // Verify there is exactly one h1 (main headline)
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    // Verify heading hierarchy is valid
    const hierarchyValid = await verifyHeadingHierarchy(page);
    expect(hierarchyValid).toBe(true);

    // Verify h2 elements exist for section headings
    const h2Count = await page.locator('h2').count();
    expect(h2Count).toBeGreaterThan(0);

    // Verify h1 text is the main headline
    const h1Text = await page.locator('h1').textContent();
    expect(h1Text?.toLowerCase()).toContain('shorten');
  });

  /**
   * Test Case 7: Check keyboard navigation through interactive elements
   * Expected: All CTAs and links are keyboard accessible
   */
  test('TC7: should support keyboard navigation for all interactive elements', async ({
    page,
  }) => {
    // Wait for page to be interactive
    await page.waitForSelector('[data-testid="homepage"]');

    // Get all interactive elements (links and buttons)
    const interactiveElements = page.locator(
      'a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );

    const count = await interactiveElements.count();
    expect(count).toBeGreaterThan(5); // Should have multiple interactive elements

    // Test keyboard focus by tabbing through elements
    // Start with the first focusable element after body
    await page.keyboard.press('Tab');

    // Verify some element received focus
    const focusedElement = page.locator(':focus');
    await expect(focusedElement).toBeVisible();

    // Continue tabbing and verify focus moves
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
      const currentFocused = page.locator(':focus');
      await expect(currentFocused).toBeVisible();
    }

    // Test that pressing Enter on a focused link activates it
    await page.goto('/');
    await page.waitForSelector('[data-testid="login-link"]');

    // Focus the login link and press Enter
    await page.locator('[data-testid="login-link"]').focus();
    await page.keyboard.press('Enter');

    // Should navigate to login page
    await expect(page).toHaveURL(/\/login/);
  });

  /**
   * Test Case 7b: All interactive elements should be focusable
   */
  test('TC7b: should have all CTAs and nav links keyboard focusable', async ({
    page,
  }) => {
    // Check primary CTA is focusable
    const primaryCTAFocusable = await isKeyboardFocusable(
      page,
      '[data-testid="hero-primary-cta"] button, [data-testid="hero-primary-cta"]'
    );
    expect(primaryCTAFocusable).toBe(true);

    // Check login link is focusable
    const loginFocusable = await isKeyboardFocusable(page, '[data-testid="login-link"]');
    expect(loginFocusable).toBe(true);

    // Check register button is focusable
    const registerFocusable = await isKeyboardFocusable(
      page,
      '[data-testid="register-button"] button, [data-testid="register-button"]'
    );
    expect(registerFocusable).toBe(true);
  });
});

test.describe('Homepage Integration - Performance', () => {
  /**
   * Test Case 8: Verify page load completes within performance budget
   * Expected: Page loads and becomes interactive within reasonable time
   */
  test('TC8: should load and become interactive within performance budget', async ({
    page,
  }) => {
    // Measure page load time
    const startTime = Date.now();

    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for main content to be visible
    await page.waitForSelector('[data-testid="homepage"]');
    await page.waitForSelector('h1');
    await page.waitForSelector('[data-testid="hero-section"]');

    const loadTime = Date.now() - startTime;

    // Page should load within performance budget
    expect(loadTime).toBeLessThan(PERFORMANCE_BUDGET_MS);

    // Verify page is interactive by checking button responds
    const primaryCTA = page.locator('[data-testid="hero-primary-cta"]');
    await expect(primaryCTA).toBeEnabled();
  });

  /**
   * Test Case 8b: Verify all sections load without errors
   */
  test('TC8b: should load all sections without console errors', async ({ page }) => {
    const consoleErrors: string[] = [];

    // Listen for console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    await page.goto('/');
    await page.waitForSelector('[data-testid="homepage"]');

    // Allow animations to complete
    await page.waitForTimeout(500);

    // Filter out known non-critical errors (e.g., favicon)
    const criticalErrors = consoleErrors.filter(
      (error) => !error.includes('favicon') && !error.includes('404')
    );

    expect(criticalErrors.length).toBe(0);
  });
});

test.describe('Homepage Integration - End-to-End User Journey', () => {
  /**
   * Complete user journey: visitor lands on homepage and registers
   */
  test('should complete visitor to registration user journey', async ({ page }) => {
    // Step 1: Visitor lands on homepage
    await page.goto('/');
    await expect(page.locator('[data-testid="homepage"]')).toBeVisible();

    // Step 2: Scans hero section to understand the product
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();
    const headlineText = await headline.textContent();
    expect(headlineText?.toLowerCase()).toContain('shorten');

    // Step 3: Reviews feature highlights
    const featuresSection = page.locator('[data-testid="features-section"]');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('[data-testid="feature-cards-grid"] > div');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Step 4: Clicks primary CTA to begin registration
    await page.locator('[data-testid="hero-primary-cta"]').click();

    // Step 5: Arrives at registration page
    await expect(page).toHaveURL(/\/register/);
    await expect(page.locator('form')).toBeVisible();
    await expect(page.getByText('Create your free account')).toBeVisible();
  });

  /**
   * Alternative path: returning user logs in
   */
  test('should allow returning user to navigate to login', async ({ page }) => {
    // Visitor lands on homepage
    await page.goto('/');
    await expect(page.locator('[data-testid="homepage"]')).toBeVisible();

    // Returning user clicks Login link in navbar
    await page.locator('[data-testid="login-link"]').click();

    // Arrives at login page
    await expect(page).toHaveURL(/\/login/);
    await expect(page.locator('form')).toBeVisible();
    await expect(page.getByText('Sign in to your account')).toBeVisible();
  });
});

test.describe('Homepage Integration - Theme Toggle Presence', () => {
  test('should have theme toggle visible on homepage', async ({ page }) => {
    await page.goto('/');

    // Theme toggle should be present (either desktop or mobile version)
    const themeToggle = page.locator('[data-testid="theme-toggle"]').first();
    await expect(themeToggle).toBeVisible();
  });
});
