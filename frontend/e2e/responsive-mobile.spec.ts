import { test, expect } from '@playwright/test';

/**
 * Responsive Design - Mobile View E2E Tests
 *
 * This test file covers the scenario: "Verify homepage displays correctly on
 * mobile devices (< 768px) as specified in REQ-8 and US-7"
 *
 * Test Cases:
 * 1. E2E: Render homepage at 375px viewport width - all content displays without horizontal scroll
 * 2. E2E: Render homepage at 320px viewport width - layout remains functional
 * 3. E2E: Check mobile navigation menu - hamburger menu is displayed and functional
 * 4. E2E: Measure CTA button touch targets - all interactive elements have min 44x44px
 * 5. E2E: Test feature cards on mobile - cards stack vertically
 */

test.describe('Responsive Design - Mobile View', () => {
  /**
   * Test Case 1: E2E Test
   * Input: Render homepage at 375px viewport width
   * Expected: All content displays without horizontal scroll, text is readable
   */
  test.describe('Test Case 1: 375px Viewport Width', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    test('page has no horizontal scrollbar at 375px width', async ({ page }) => {
      // Check that the document width doesn't exceed viewport width
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(documentWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('hero section content is visible and readable', async ({ page }) => {
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      const headline = page.getByTestId('hero-headline');
      await expect(headline).toBeVisible();

      const subheadline = page.getByTestId('hero-subheadline');
      await expect(subheadline).toBeVisible();

      // Verify text doesn't overflow its container
      const headlineBox = await headline.boundingBox();
      expect(headlineBox).not.toBeNull();
      expect(headlineBox!.width).toBeLessThanOrEqual(375);
    });

    test('CTA buttons are visible and properly sized', async ({ page }) => {
      const getStartedBtn = page.getByTestId('cta-get-started');
      const loginBtn = page.getByTestId('cta-login');

      await expect(getStartedBtn).toBeVisible();
      await expect(loginBtn).toBeVisible();

      // Buttons should fit within viewport
      const getStartedBox = await getStartedBtn.boundingBox();
      const loginBox = await loginBtn.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(loginBox).not.toBeNull();
      expect(getStartedBox!.x + getStartedBox!.width).toBeLessThanOrEqual(375);
      expect(loginBox!.x + loginBox!.width).toBeLessThanOrEqual(375);
    });

    test('features section is readable without horizontal scroll', async ({ page }) => {
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Check document width hasn't changed after scrolling
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      expect(documentWidth).toBeLessThanOrEqual(375);
    });

    test('footer section is visible and readable', async ({ page }) => {
      const footer = page.getByTestId('footer-cta');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      const footerHeadline = page.getByTestId('footer-headline');
      await expect(footerHeadline).toBeVisible();
    });
  });

  /**
   * Test Case 2: E2E Test
   * Input: Render homepage at 320px viewport width
   * Expected: Layout remains functional at minimum supported width
   */
  test.describe('Test Case 2: 320px Viewport Width (Minimum)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');
    });

    test('page has no horizontal scrollbar at 320px width', async ({ page }) => {
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      expect(documentWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('all main sections are visible at 320px', async ({ page }) => {
      // Hero section
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      // Features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // How It Works section
      const howItWorks = page.locator('section#how-it-works');
      await howItWorks.scrollIntoViewIfNeeded();
      await expect(howItWorks).toBeVisible();

      // Footer
      const footer = page.getByTestId('footer-cta');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });

    test('text content wraps properly at 320px', async ({ page }) => {
      const headline = page.getByTestId('hero-headline');
      const headlineBox = await headline.boundingBox();

      expect(headlineBox).not.toBeNull();
      // Headline should fit within viewport with some padding
      expect(headlineBox!.x + headlineBox!.width).toBeLessThanOrEqual(320);
    });

    test('buttons do not overflow at 320px', async ({ page }) => {
      const getStartedBtn = page.getByTestId('cta-get-started');
      const loginBtn = page.getByTestId('cta-login');

      await expect(getStartedBtn).toBeVisible();
      await expect(loginBtn).toBeVisible();

      const getStartedBox = await getStartedBtn.boundingBox();
      const loginBox = await loginBtn.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(loginBox).not.toBeNull();

      // Buttons should not extend beyond viewport
      expect(getStartedBox!.x).toBeGreaterThanOrEqual(0);
      expect(getStartedBox!.x + getStartedBox!.width).toBeLessThanOrEqual(320);
      expect(loginBox!.x).toBeGreaterThanOrEqual(0);
      expect(loginBox!.x + loginBox!.width).toBeLessThanOrEqual(320);
    });
  });

  /**
   * Test Case 3: E2E Test
   * Input: Check mobile navigation menu
   * Expected: Hamburger menu is displayed and functional on mobile
   */
  test.describe('Test Case 3: Mobile Navigation Menu', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    test('hamburger menu button is visible on mobile', async ({ page }) => {
      const hamburgerBtn = page.getByTestId('mobile-menu-button');
      await expect(hamburgerBtn).toBeVisible();
    });

    test('desktop navigation links are hidden on mobile', async ({ page }) => {
      const desktopNav = page.getByTestId('desktop-nav');
      await expect(desktopNav).toBeHidden();
    });

    test('clicking hamburger opens mobile menu', async ({ page }) => {
      const hamburgerBtn = page.getByTestId('mobile-menu-button');
      await hamburgerBtn.click();

      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).toBeVisible();
    });

    test('mobile menu contains navigation links', async ({ page }) => {
      const hamburgerBtn = page.getByTestId('mobile-menu-button');
      await hamburgerBtn.click();

      const mobileLoginLink = page.getByTestId('mobile-nav-login');
      const mobileGetStartedLink = page.getByTestId('mobile-nav-get-started');

      await expect(mobileLoginLink).toBeVisible();
      await expect(mobileGetStartedLink).toBeVisible();
    });

    test('mobile menu links navigate correctly', async ({ page }) => {
      const hamburgerBtn = page.getByTestId('mobile-menu-button');
      await hamburgerBtn.click();

      const mobileLoginLink = page.getByTestId('mobile-nav-login');
      await mobileLoginLink.click();

      await expect(page).toHaveURL(/\/login$/);
    });

    test('mobile menu can be closed', async ({ page }) => {
      const hamburgerBtn = page.getByTestId('mobile-menu-button');
      await hamburgerBtn.click();

      const mobileMenu = page.getByTestId('mobile-menu');
      await expect(mobileMenu).toBeVisible();

      // Click hamburger again to close
      await hamburgerBtn.click();
      await expect(mobileMenu).toBeHidden();
    });
  });

  /**
   * Test Case 4: E2E Test
   * Input: Measure CTA button touch targets
   * Expected: All interactive elements have minimum 44x44px touch area
   */
  test.describe('Test Case 4: Touch Target Sizes', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    test('Get Started button has minimum 44x44px touch target', async ({ page }) => {
      const getStartedBtn = page.getByTestId('cta-get-started');
      const box = await getStartedBtn.boundingBox();

      expect(box).not.toBeNull();
      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    });

    test('Login button has minimum 44x44px touch target', async ({ page }) => {
      const loginBtn = page.getByTestId('cta-login');
      const box = await loginBtn.boundingBox();

      expect(box).not.toBeNull();
      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    });

    test('hamburger menu button has minimum 44x44px touch target', async ({ page }) => {
      const hamburgerBtn = page.getByTestId('mobile-menu-button');
      const box = await hamburgerBtn.boundingBox();

      expect(box).not.toBeNull();
      // Allow small floating-point tolerance
      expect(box!.width).toBeGreaterThanOrEqual(43.9);
      expect(box!.height).toBeGreaterThanOrEqual(43.9);
    });

    test('footer CTA button has minimum 44x44px touch target', async ({ page }) => {
      const footerBtn = page.getByTestId('footer-cta-get-started');
      await footerBtn.scrollIntoViewIfNeeded();
      const box = await footerBtn.boundingBox();

      expect(box).not.toBeNull();
      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    });

    test('theme toggle has minimum 44x44px touch target', async ({ page }) => {
      // Target the actual button inside the theme toggle component
      const themeToggleButton = page.getByTestId('theme-toggle-button');
      const box = await themeToggleButton.boundingBox();

      expect(box).not.toBeNull();
      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    });
  });

  /**
   * Test Case 5: E2E Test
   * Input: Test feature cards on mobile
   * Expected: Feature cards stack vertically on mobile viewport
   */
  test.describe('Test Case 5: Feature Cards Mobile Layout', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    test('feature cards are visible on mobile', async ({ page }) => {
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();

      // Check all 4 feature cards are visible
      for (let i = 1; i <= 4; i++) {
        const title = page.getByTestId(`feature-title-${i}`);
        await title.scrollIntoViewIfNeeded();
        await expect(title).toBeVisible();
      }
    });

    test('feature cards stack vertically on mobile (single column)', async ({ page }) => {
      const featuresGrid = page.getByTestId('features-grid');
      await featuresGrid.scrollIntoViewIfNeeded();

      // Get all glassmorphism cards within features section
      const cards = page.locator('[data-testid="features-section"] [data-testid="glassmorphism-card"]');
      const cardCount = await cards.count();

      expect(cardCount).toBe(4);

      // Get bounding boxes for first two cards
      const card1Box = await cards.nth(0).boundingBox();
      const card2Box = await cards.nth(1).boundingBox();

      expect(card1Box).not.toBeNull();
      expect(card2Box).not.toBeNull();

      // On mobile, cards should be stacked vertically (card2 should be below card1)
      // Allow for some horizontal offset but y should be clearly different
      expect(card2Box!.y).toBeGreaterThan(card1Box!.y + card1Box!.height - 20);
    });

    test('feature cards fit within mobile viewport width', async ({ page }) => {
      const cards = page.locator('[data-testid="features-section"] [data-testid="glassmorphism-card"]');
      const cardCount = await cards.count();

      for (let i = 0; i < cardCount; i++) {
        const card = cards.nth(i);
        await card.scrollIntoViewIfNeeded();
        const box = await card.boundingBox();

        expect(box).not.toBeNull();
        // Card should fit within viewport with some margin
        expect(box!.x).toBeGreaterThanOrEqual(0);
        expect(box!.x + box!.width).toBeLessThanOrEqual(375);
      }
    });

    test('feature card content is readable on mobile', async ({ page }) => {
      // Check that feature descriptions are visible
      for (let i = 1; i <= 4; i++) {
        const description = page.getByTestId(`feature-description-${i}`);
        await description.scrollIntoViewIfNeeded();
        await expect(description).toBeVisible();

        // Description should have actual text
        const text = await description.textContent();
        expect(text).toBeTruthy();
        expect(text!.length).toBeGreaterThan(10);
      }
    });
  });

  /**
   * Additional responsive tests
   */
  test.describe('Additional Mobile Responsiveness Tests', () => {
    test('homepage is accessible at tablet breakpoint (768px)', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      expect(documentWidth).toBeLessThanOrEqual(768);
    });

    test('How It Works steps display vertically on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const step1 = page.getByTestId('step-1');
      const step2 = page.getByTestId('step-2');
      const step3 = page.getByTestId('step-3');

      await step1.scrollIntoViewIfNeeded();
      await expect(step1).toBeVisible();
      await expect(step2).toBeVisible();
      await expect(step3).toBeVisible();

      const step1Box = await step1.boundingBox();
      const step2Box = await step2.boundingBox();
      const step3Box = await step3.boundingBox();

      expect(step1Box).not.toBeNull();
      expect(step2Box).not.toBeNull();
      expect(step3Box).not.toBeNull();

      // Steps should be vertically stacked on mobile
      expect(step2Box!.y).toBeGreaterThan(step1Box!.y);
      expect(step3Box!.y).toBeGreaterThan(step2Box!.y);
    });

    test('navbar logo remains visible on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const logo = page.getByTestId('navbar-logo');
      await expect(logo).toBeVisible();

      const logoText = await logo.textContent();
      expect(logoText).toContain('URL Shortener');
    });
  });
});
