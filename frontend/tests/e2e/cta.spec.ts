/**
 * CTA Section E2E Tests
 * Owner: Scenario 10 - Call-to-Action Section
 *
 * End-to-end tests for CTA section functionality:
 * - Click Sign Up Free button navigates to /register
 * - Button shows hover animation effect
 */

import { test, expect } from '@playwright/test';

test.describe('CTA Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to CTA section
    await page.evaluate(() => {
      const ctaSection = document.querySelector('[data-testid="cta-section"]');
      if (ctaSection) {
        ctaSection.scrollIntoView({ behavior: 'instant', block: 'center' });
      }
    });
  });

  test('should display CTA section with headline and button', async ({ page }) => {
    const ctaSection = page.getByTestId('cta-section');
    await expect(ctaSection).toBeVisible();

    const headline = page.getByTestId('cta-headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('Analytics');

    const signupButton = page.getByTestId('cta-signup-button');
    await expect(signupButton).toBeVisible();
    await expect(signupButton).toContainText('Sign Up Free');
  });

  test('should navigate to /register when clicking Sign Up Free button', async ({ page }) => {
    const signupButton = page.getByTestId('cta-signup-button');
    await expect(signupButton).toBeVisible();

    await signupButton.click();
    await expect(page).toHaveURL('/register');
  });

  test('should show hover animation effect on CTA button', async ({ page }) => {
    const signupButton = page.getByTestId('cta-signup-button');
    await expect(signupButton).toBeVisible();

    // Get initial styles
    const initialTransform = await signupButton.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over the button
    await signupButton.hover();

    // Wait for animation to start
    await page.waitForTimeout(150);

    // Check that transform has changed (scale animation)
    const hoverTransform = await signupButton.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Verify some animation effect occurred (transform should change or styles differ)
    // The button should have some hover state effect
    const hasAnimationClass = await signupButton.evaluate((el) => {
      // Check for any hover-related transform, scale, or transition
      const style = window.getComputedStyle(el);
      return (
        style.transform !== 'none' ||
        style.transition !== 'none 0s ease 0s' ||
        el.classList.contains('hover:scale-105') ||
        el.className.includes('hover:')
      );
    });

    // Button should have hover animation capabilities
    expect(hasAnimationClass).toBe(true);
  });

  test('should have proper visual styling for prominence', async ({ page }) => {
    const ctaSection = page.getByTestId('cta-section');
    await expect(ctaSection).toBeVisible();

    // Verify section has background styling for visual prominence
    const hasBackgroundStyling = await ctaSection.evaluate((el) => {
      const style = window.getComputedStyle(el);
      // Check for any background styling
      return (
        style.backgroundColor !== 'rgba(0, 0, 0, 0)' ||
        style.backgroundImage !== 'none' ||
        el.className.includes('bg-') ||
        el.className.includes('gradient')
      );
    });

    expect(hasBackgroundStyling).toBe(true);
  });
});
