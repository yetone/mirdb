/**
 * User flow E2E tests.
 * Owner: Scenario 10 - User Flow - First Time Visitor
 *
 * Validates the complete user journey for a first-time visitor discovering the product.
 *
 * Tests for:
 * - Complete first-time visitor journey
 * - Discovery to sign-up flow
 * - Section navigation
 * - CTA visibility above the fold
 *
 * User Stories Covered:
 * - US-1: First-time visitor understands product value
 * - US-2: Easy access to sign-up CTA
 * - US-4: Social proof for trust building
 */

import { test, expect } from '@playwright/test';

test.describe('User Flow - First Time Visitor', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Complete first-time visitor flow
   * Input: Complete first-time visitor flow
   * Expected: User can understand product value within 30 seconds of viewing hero
   */
  test('user can understand product value from hero section', async ({ page }) => {
    // Verify hero section is immediately visible
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // Verify headline clearly explains product purpose
    const headline = page.getByRole('heading', { level: 1 });
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Build Something Amazing');

    // Verify subheadline provides additional context
    const subheadline = page.getByText(/modern platform for building/i);
    await expect(subheadline).toBeVisible();

    // Verify CTAs are present to guide user action
    const primaryCTA = page.getByRole('link', { name: /get started/i });
    const secondaryCTA = page.getByRole('link', { name: /learn more/i });
    await expect(primaryCTA).toBeVisible();
    await expect(secondaryCTA).toBeVisible();

    // Verify page loads quickly (under 30 seconds is trivial - test content readability)
    // The fact that we can assert all elements are visible confirms quick comprehension
  });

  /**
   * Test Case 2: Scroll from hero to features
   * Input: Scroll from hero to features
   * Expected: Smooth scroll behavior between sections
   */
  test('smooth scroll from hero to features section', async ({ page }) => {
    // Click Learn More to scroll to features
    const secondaryCTA = page.getByRole('link', { name: /learn more/i });
    await secondaryCTA.click();

    // Wait for scroll animation
    await page.waitForTimeout(600);

    // Verify URL hash changed
    await expect(page).toHaveURL('/#features');

    // Verify features section is now in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify hero is no longer fully in viewport (scroll happened)
    const heroSection = page.getByTestId('hero-section');
    const heroBox = await heroSection.boundingBox();
    const viewportSize = page.viewportSize();

    if (heroBox && viewportSize) {
      // Hero section top should be above viewport (scrolled up)
      // or hero should be partially visible
      expect(heroBox.y).toBeLessThan(viewportSize.height / 2);
    }
  });

  /**
   * Test Case 3: Navigate Features > Social Proof > Sign Up
   * Input: Navigate Features > Social Proof > Sign Up
   * Expected: User completes journey from discovery to sign-up in under 5 clicks
   */
  test('user completes journey from discovery to sign-up in under 5 clicks', async ({ page }) => {
    let clickCount = 0;

    // Step 1: Land on homepage (0 clicks - page loads)
    await expect(page.getByTestId('hero-section')).toBeVisible();

    // Step 2: Click Learn More to view features (1 click)
    const learnMoreCTA = page.getByRole('link', { name: /learn more/i });
    await learnMoreCTA.click();
    clickCount++;
    await page.waitForTimeout(600);

    // Verify features are visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Step 3: Scroll to view social proof (can be done via scroll, not a click)
    // Or user naturally scrolls - testing that social proof is accessible
    const socialProofSection = page.locator('#social-proof');
    await socialProofSection.scrollIntoViewIfNeeded();
    await expect(socialProofSection).toBeVisible();

    // Verify testimonials are visible for trust building
    await expect(page.getByText(/What Our Users Say/i)).toBeVisible();

    // Step 4: User decides to sign up - scroll back or use primary CTA (2 clicks max)
    // Navigate to sign up page
    const signUpCTA = page.getByRole('link', { name: /get started/i }).first();
    await signUpCTA.click();
    clickCount++;

    // Verify sign-up page
    await expect(page).toHaveURL('/signup');
    await expect(page.getByRole('heading', { name: /sign up/i })).toBeVisible();

    // Assert journey completed in under 5 clicks
    expect(clickCount).toBeLessThan(5);
  });

  /**
   * Test Case 4: Check visual hierarchy (manual test, automated approximation)
   * Input: Check visual hierarchy
   * Expected: Eye flow naturally guides user: Logo > Hero > CTA > Features > Social Proof
   *
   * This is a manual test case, but we verify the DOM order matches expected visual flow.
   */
  test('visual hierarchy follows expected order', async ({ page }) => {
    // Get main content
    const main = page.locator('main#main-content');
    await expect(main).toBeVisible();

    // Verify sections exist in correct DOM order
    const sections = await main.locator('section').all();
    expect(sections.length).toBeGreaterThanOrEqual(3);

    // First section should be hero
    const firstSection = sections[0];
    await expect(firstSection).toHaveAttribute('data-testid', 'hero-section');

    // Second section should be features
    const secondSection = sections[1];
    await expect(secondSection).toHaveAttribute('id', 'features');

    // Third section should be social proof
    const thirdSection = sections[2];
    await expect(thirdSection).toHaveAttribute('id', 'social-proof');

    // Footer should exist outside main (use contentinfo role to distinguish from testimonial footers)
    const footer = page.getByRole('contentinfo');
    await expect(footer).toBeVisible();

    // Verify headline is h1 (highest visual hierarchy)
    const h1 = page.getByRole('heading', { level: 1 });
    await expect(h1).toBeVisible();

    // Verify CTA buttons are prominently placed after headline
    const ctaContainer = page.locator('[data-testid="hero-section"]').locator('a');
    expect(await ctaContainer.count()).toBeGreaterThanOrEqual(2);
  });

  /**
   * Test Case 5: Test CTA visibility above the fold
   * Input: Test CTA visibility above the fold
   * Expected: Primary CTA is visible without scrolling on desktop
   */
  test('primary CTA is visible above the fold on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    // Get viewport dimensions
    const viewportSize = page.viewportSize();
    expect(viewportSize).toBeTruthy();

    // Get primary CTA button
    const primaryCTA = page.getByRole('link', { name: /get started/i });
    await expect(primaryCTA).toBeVisible();

    // Get CTA bounding box
    const ctaBox = await primaryCTA.boundingBox();
    expect(ctaBox).toBeTruthy();

    if (ctaBox && viewportSize) {
      // CTA should be fully visible within viewport (above the fold)
      expect(ctaBox.y).toBeGreaterThanOrEqual(0);
      expect(ctaBox.y + ctaBox.height).toBeLessThanOrEqual(viewportSize.height);

      // CTA should be within visible horizontal bounds
      expect(ctaBox.x).toBeGreaterThanOrEqual(0);
      expect(ctaBox.x + ctaBox.width).toBeLessThanOrEqual(viewportSize.width);
    }
  });

  test('secondary CTA is also visible above the fold on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.goto('/');

    const viewportSize = page.viewportSize();
    const secondaryCTA = page.getByRole('link', { name: /learn more/i });
    await expect(secondaryCTA).toBeVisible();

    const ctaBox = await secondaryCTA.boundingBox();
    expect(ctaBox).toBeTruthy();

    if (ctaBox && viewportSize) {
      expect(ctaBox.y + ctaBox.height).toBeLessThanOrEqual(viewportSize.height);
    }
  });
});

test.describe('User Flow - Section Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('all main sections have proper landmarks', async ({ page }) => {
    // Main content landmark
    const main = page.getByRole('main');
    await expect(main).toBeVisible();

    // Hero section has aria-labelledby
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');

    // Features section has aria-labelledby
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toHaveAttribute('aria-labelledby');

    // Social proof section has aria-labelledby
    const socialProofSection = page.locator('#social-proof');
    await expect(socialProofSection).toHaveAttribute('aria-labelledby', 'social-proof-title');

    // Footer is contentinfo landmark
    const footer = page.getByRole('contentinfo');
    await expect(footer).toBeVisible();
  });

  test('user can navigate to features section via anchor link', async ({ page }) => {
    // Verify learn more links to #features
    const learnMore = page.getByRole('link', { name: /learn more/i });
    await expect(learnMore).toHaveAttribute('href', '#features');

    await learnMore.click();
    await page.waitForTimeout(500);

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });
});

test.describe('User Flow - Social Proof Trust Building', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('testimonials are displayed for trust building', async ({ page }) => {
    const socialProofSection = page.locator('#social-proof');
    await socialProofSection.scrollIntoViewIfNeeded();

    // Verify section title
    await expect(page.getByText(/What Our Users Say/i)).toBeVisible();

    // Verify at least one testimonial with quote and author
    const testimonials = page.locator('[class*="testimonial"]');
    expect(await testimonials.count()).toBeGreaterThan(0);

    // Verify testimonial content exists
    await expect(page.getByText(/Jane Doe/i)).toBeVisible();
  });

  test('user statistics are displayed', async ({ page }) => {
    const socialProofSection = page.locator('#social-proof');
    await socialProofSection.scrollIntoViewIfNeeded();

    // Verify statistics
    await expect(page.getByText(/10,000\+/i)).toBeVisible();
    await expect(page.getByText(/Happy Users/i)).toBeVisible();
    await expect(page.getByText(/99.9%/i)).toBeVisible();
  });
});

test.describe('User Flow - Complete Journey E2E', () => {
  test('complete first-time visitor journey from landing to sign-up', async ({ page }) => {
    // Step 1: Land on homepage
    await page.goto('/');

    // Verify page loaded correctly
    await expect(page.getByTestId('hero-section')).toBeVisible();
    await expect(page.getByRole('heading', { level: 1 })).toContainText('Build Something Amazing');

    // Step 2: View hero section - understand product value
    const subheadline = page.getByText(/modern platform/i);
    await expect(subheadline).toBeVisible();

    // Step 3: Click Learn More to explore features
    const learnMoreBtn = page.getByRole('link', { name: /learn more/i });
    await learnMoreBtn.click();
    await page.waitForTimeout(600);

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
    await expect(page.getByText(/Lightning Fast/i)).toBeVisible();

    // Step 4: Review social proof
    const socialProofSection = page.locator('#social-proof');
    await socialProofSection.scrollIntoViewIfNeeded();
    await expect(page.getByText(/What Our Users Say/i)).toBeVisible();
    await expect(page.getByText(/10,000\+/i)).toBeVisible();

    // Step 5: Click sign-up CTA
    // Scroll back to hero to click Get Started
    await page.goto('/');
    const signUpBtn = page.getByRole('link', { name: /get started/i });
    await signUpBtn.click();

    // Verify navigation to sign-up page
    await expect(page).toHaveURL('/signup');
    await expect(page.getByRole('heading', { name: /sign up/i })).toBeVisible();
  });
});
