import { test, expect } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests for Homepage
 *
 * These tests verify that the homepage renders correctly across
 * Chrome, Firefox, Safari (WebKit), and Edge browsers.
 *
 * Each test verifies:
 * - All major sections are visible and properly rendered
 * - Layout is correct (no major visual inconsistencies)
 * - Interactive elements are functional
 * - Styling is applied correctly
 */

test.describe('Cross-Browser Homepage Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('homepage renders all sections correctly', async ({ page }) => {
    // Verify Hero Section renders with proper styling
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();
    await expect(heroSection).toHaveClass(/bg-gradient-to-br/);

    // Verify main headline is visible
    const headline = heroSection.locator('h1');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Shorten, Share, Track');

    // Verify subheadline is visible
    const subheadline = page.locator('[data-testid="hero-subheadline"]');
    await expect(subheadline).toBeVisible();
    await expect(subheadline).toContainText('Transform your long URLs');

    // Verify Features Section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify all 3 feature cards are rendered
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();
    for (let i = 0; i < 3; i++) {
      await expect(page.locator(`[data-testid="feature-card-${i}"]`)).toBeVisible();
    }

    // Verify How It Works Section
    const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
    await expect(howItWorksSection).toBeVisible();

    // Verify all 3 step cards
    for (let i = 1; i <= 3; i++) {
      await expect(page.locator(`[data-testid="step-card-${i}"]`)).toBeVisible();
      await expect(page.locator(`[data-testid="step-number-${i}"]`)).toBeVisible();
    }

    // Verify Footer Section
    const footerSection = page.locator('[data-testid="footer-section"]');
    await expect(footerSection).toBeVisible();
    await expect(page.locator('[data-testid="footer-brand"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-nav"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-legal"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-copyright"]')).toBeVisible();
  });

  test('CTA buttons are visible and have correct styling', async ({ page }) => {
    // Primary CTA - Get Started Free
    const registerCTA = page.locator('[data-testid="cta-register"]');
    await expect(registerCTA).toBeVisible();
    await expect(registerCTA).toHaveText('Get Started Free');
    await expect(registerCTA).toHaveAttribute('href', '/register');

    // Verify button has proper class styling
    await expect(registerCTA).toHaveClass(/btn/);
    await expect(registerCTA).toHaveClass(/btn-lg/);

    // Secondary CTA - Login link
    const loginLink = page.locator('[data-testid="login-link"]');
    await expect(loginLink).toBeVisible();
    await expect(loginLink).toHaveText('Log in');
    await expect(loginLink).toHaveAttribute('href', '/login');
  });

  test('features section displays all feature cards with icons', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify section header
    const header = featuresSection.locator('h2');
    await expect(header).toHaveText('Powerful Features');

    // Verify URL Shortening icon
    await expect(page.locator('[data-testid="url-shortening-icon"]')).toBeVisible();

    // Verify Analytics icon
    await expect(page.locator('[data-testid="analytics-icon"]')).toBeVisible();

    // Verify Link Management icon
    await expect(page.locator('[data-testid="link-management-icon"]')).toBeVisible();

    // Verify feature descriptions are visible
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    await expect(featureCards).toHaveCount(3);

    // Check each card has title and description
    for (let i = 0; i < 3; i++) {
      const card = page.locator(`[data-testid="feature-card-${i}"]`);
      const title = card.locator('h3');
      const description = card.locator('p');
      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }
  });

  test('how it works section displays all steps correctly', async ({ page }) => {
    const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
    await howItWorksSection.scrollIntoViewIfNeeded();

    // Verify section header
    const header = howItWorksSection.locator('h2');
    await expect(header).toContainText('How It Works');

    // Verify steps container
    const stepsContainer = page.locator('[data-testid="steps-container"]');
    await expect(stepsContainer).toBeVisible();

    // Verify each step card
    const expectedSteps = [
      { number: 1, title: 'Paste Your Long URL' },
      { number: 2, title: 'Get Your Shortened Link' },
      { number: 3, title: 'Share and Track Performance' },
    ];

    for (const step of expectedSteps) {
      const stepCard = page.locator(`[data-testid="step-card-${step.number}"]`);
      await expect(stepCard).toBeVisible();

      // Verify step number badge
      const numberBadge = page.locator(`[data-testid="step-number-${step.number}"]`);
      await expect(numberBadge).toBeVisible();
      await expect(numberBadge).toHaveText(String(step.number));

      // Verify step icon
      const icon = page.locator(`[data-testid="step-icon-${step.number}"]`);
      await expect(icon).toBeVisible();

      // Verify step title
      const title = stepCard.locator('.card-title');
      await expect(title).toContainText(step.title);
    }
  });

  test('footer section displays all links and copyright', async ({ page }) => {
    const footerSection = page.locator('[data-testid="footer-section"]');
    await footerSection.scrollIntoViewIfNeeded();

    // Verify brand section
    const brand = page.locator('[data-testid="footer-brand"]');
    await expect(brand).toBeVisible();
    await expect(brand.locator('h3')).toHaveText('URL Shortener');

    // Verify navigation links
    await expect(page.locator('[data-testid="footer-link-home"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-link-home"]')).toHaveAttribute('href', '/');

    await expect(page.locator('[data-testid="footer-link-login"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-link-login"]')).toHaveAttribute('href', '/login');

    await expect(page.locator('[data-testid="footer-link-register"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-link-register"]')).toHaveAttribute('href', '/register');

    // Verify legal links
    await expect(page.locator('[data-testid="footer-link-privacy"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-link-terms"]')).toBeVisible();

    // Verify copyright
    const copyright = page.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();
    const currentYear = new Date().getFullYear();
    await expect(copyright).toContainText(`${currentYear}`);
    await expect(copyright).toContainText('URL Shortener');
  });

  test('page has correct layout structure', async ({ page }) => {
    // Verify main element exists
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify sections appear in correct order
    const heroSection = page.locator('[data-testid="hero-section"]');
    const featuresSection = page.locator('[data-testid="features-section"]');
    const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
    const footerSection = page.locator('[data-testid="footer-section"]');

    // Get bounding boxes to verify order
    const heroBounds = await heroSection.boundingBox();
    const featuresBounds = await featuresSection.boundingBox();
    const howItWorksBounds = await howItWorksSection.boundingBox();
    const footerBounds = await footerSection.boundingBox();

    // Verify vertical order: hero < features < howItWorks < footer
    expect(heroBounds!.y).toBeLessThan(featuresBounds!.y);
    expect(featuresBounds!.y).toBeLessThan(howItWorksBounds!.y);
    expect(howItWorksBounds!.y).toBeLessThan(footerBounds!.y);
  });

  test('responsive grid layouts render correctly', async ({ page }) => {
    // Verify features grid has proper structure
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();
    await expect(featuresGrid).toHaveClass(/grid/);

    // Verify steps container has grid layout
    const stepsContainer = page.locator('[data-testid="steps-container"]');
    await expect(stepsContainer).toBeVisible();
    await expect(stepsContainer).toHaveClass(/grid/);

    // Verify footer grid
    const footerGrid = page.locator('[data-testid="footer-grid"]');
    await expect(footerGrid).toBeVisible();
    await expect(footerGrid).toHaveClass(/grid/);
  });

  test('hero section has gradient background', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toHaveClass(/bg-gradient-to-br/);
    await expect(heroSection).toHaveClass(/from-primary/);
    await expect(heroSection).toHaveClass(/via-secondary/);
    await expect(heroSection).toHaveClass(/to-accent/);
  });

  test('interactive links are clickable and navigate correctly', async ({ page }) => {
    // Test register CTA navigation
    const registerCTA = page.locator('[data-testid="cta-register"]');
    await registerCTA.click();
    await expect(page).toHaveURL('/register');

    // Go back to homepage
    await page.goto('/');

    // Test login link navigation
    const loginLink = page.locator('[data-testid="login-link"]');
    await loginLink.click();
    await expect(page).toHaveURL('/login');

    // Go back to homepage
    await page.goto('/');

    // Test footer home link
    const homeLink = page.locator('[data-testid="footer-link-home"]');
    await homeLink.scrollIntoViewIfNeeded();
    await homeLink.click();
    await expect(page).toHaveURL('/');
  });

  test('page title and meta tags are set correctly', async ({ page }) => {
    // Verify page title
    const title = await page.title();
    expect(title).toBe('URL Shortener - Shorten, Share, Track Your Links');

    // Verify meta description
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDescription).toContain('Create memorable short links instantly');

    // Verify Open Graph meta tags
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
    expect(ogTitle).toBe('URL Shortener - Shorten, Share, Track Your Links');

    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
    expect(ogDescription).toContain('Create memorable short links instantly');

    const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
    expect(ogType).toBe('website');
  });

  test('minimum touch target sizes are respected', async ({ page }) => {
    // Verify register CTA has minimum height for touch targets
    const registerCTA = page.locator('[data-testid="cta-register"]');
    const registerBox = await registerCTA.boundingBox();
    expect(registerBox!.height).toBeGreaterThanOrEqual(44);

    // Verify login link has minimum height
    const loginLink = page.locator('[data-testid="login-link"]');
    await expect(loginLink).toHaveClass(/min-h-\[44px\]/);

    // Verify footer links have minimum height
    const footerHomeLink = page.locator('[data-testid="footer-link-home"]');
    await footerHomeLink.scrollIntoViewIfNeeded();
    await expect(footerHomeLink).toHaveClass(/min-h-\[44px\]/);
  });

  test('text content is visible and readable', async ({ page }) => {
    // Verify hero headline is large and visible
    const headline = page.locator('[data-testid="hero-section"] h1');
    await expect(headline).toBeVisible();
    const headlineFont = await headline.evaluate(el => getComputedStyle(el).fontSize);
    const headlineFontSize = parseFloat(headlineFont);
    expect(headlineFontSize).toBeGreaterThan(20); // At least 20px

    // Verify feature titles are visible
    for (let i = 0; i < 3; i++) {
      const featureTitle = page.locator(`[data-testid="feature-card-${i}"] h3`);
      await expect(featureTitle).toBeVisible();
    }

    // Verify step titles are visible
    for (let i = 1; i <= 3; i++) {
      const stepTitle = page.locator(`[data-testid="step-card-${i}"] .card-title`);
      await expect(stepTitle).toBeVisible();
    }
  });
});
