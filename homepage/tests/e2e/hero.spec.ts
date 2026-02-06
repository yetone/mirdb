import { test, expect } from '@playwright/test';

test.describe('Hero Section - CTA Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 4: Click primary CTA button - User is redirected to sign-up page
   */
  test('clicking primary CTA button navigates to signup page', async ({ page }) => {
    const primaryCTA = page.getByRole('link', { name: /get started/i });
    await expect(primaryCTA).toBeVisible();

    await primaryCTA.click();

    await expect(page).toHaveURL('/signup');
    await expect(page.getByRole('heading', { name: /sign up/i })).toBeVisible();
  });

  /**
   * Test Case 5: Click secondary CTA button - User is scrolled to features section
   */
  test('clicking secondary CTA button scrolls to features section', async ({ page }) => {
    const secondaryCTA = page.getByRole('link', { name: /learn more/i });
    await expect(secondaryCTA).toBeVisible();

    await secondaryCTA.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify the URL hash
    await expect(page).toHaveURL('/#features');

    // Verify features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  /**
   * Test Case 6: Tab to CTA buttons and press Enter - Buttons are activatable via keyboard
   */
  test('CTA buttons are activatable via keyboard with visible focus states', async ({ page }) => {
    // Navigate to the primary CTA using Tab
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Primary CTA

    const primaryCTA = page.getByRole('link', { name: /get started/i });
    await expect(primaryCTA).toBeFocused();

    // Check focus is visible (has focus-visible outline)
    const focusOutline = await primaryCTA.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.outlineWidth !== '0px' || styles.boxShadow !== 'none';
    });
    expect(focusOutline).toBeTruthy();

    // Press Enter to navigate
    await page.keyboard.press('Enter');
    await expect(page).toHaveURL('/signup');
  });

  test('secondary CTA is keyboard accessible', async ({ page }) => {
    // Tab to secondary CTA
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Primary CTA
    await page.keyboard.press('Tab'); // Secondary CTA

    const secondaryCTA = page.getByRole('link', { name: /learn more/i });
    await expect(secondaryCTA).toBeFocused();

    // Press Enter to activate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);
    await expect(page).toHaveURL('/#features');
  });
});

test.describe('Hero Section - Visual Rendering', () => {
  test('hero section displays headline and subheadline', async ({ page }) => {
    await page.goto('/');

    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    const headline = page.getByRole('heading', { level: 1 });
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText('Build Something Amazing');

    const subheadline = page.getByText(/modern platform for building/i);
    await expect(subheadline).toBeVisible();
  });

  test('both CTA buttons are visible', async ({ page }) => {
    await page.goto('/');

    const primaryCTA = page.getByRole('link', { name: /get started/i });
    const secondaryCTA = page.getByRole('link', { name: /learn more/i });

    await expect(primaryCTA).toBeVisible();
    await expect(secondaryCTA).toBeVisible();
  });
});
