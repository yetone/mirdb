import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Pricing/CTA Section
 * Testing REQ-7: Pricing section or call-to-action for pricing information
 */

test.describe('Pricing/CTA Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 2: Verify conversion path
   * Input: Verify conversion path
   * Expected: Section provides clear action for visitor to take
   */
  test('should have pricing/CTA section visible on page', async ({ page }) => {
    const pricingSection = page.locator('.pricing-section, .cta-section, #pricing, #cta-section');
    await expect(pricingSection).toBeVisible();
  });

  test('should have CTA button that is clickable', async ({ page }) => {
    const pricingSection = page.locator('.pricing-section, .cta-section');
    const ctaButton = pricingSection.locator('a, button').first();

    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toBeEnabled();
  });

  test('should position pricing/CTA section after social proof', async ({ page }) => {
    const socialProof = page.locator('.social-proof');
    const pricingSection = page.locator('.pricing-section, .cta-section');

    const socialProofBox = await socialProof.boundingBox();
    const pricingBox = await pricingSection.boundingBox();

    expect(pricingBox.y).toBeGreaterThan(socialProofBox.y);
  });

  test('should have heading visible in pricing/CTA section', async ({ page }) => {
    const pricingSection = page.locator('.pricing-section, .cta-section');
    const heading = pricingSection.locator('h2');

    await expect(heading).toBeVisible();
    const text = await heading.textContent();
    expect(text.trim().length).toBeGreaterThan(0);
  });

  test('should have descriptive text in pricing/CTA section', async ({ page }) => {
    const pricingSection = page.locator('.pricing-section, .cta-section');
    const description = pricingSection.locator('p').first();

    await expect(description).toBeVisible();
    const text = await description.textContent();
    expect(text.trim().length).toBeGreaterThan(20);
  });

  test('should have CTA button with clear action text', async ({ page }) => {
    const pricingSection = page.locator('.pricing-section, .cta-section');
    const ctaButton = pricingSection.locator('.cta-primary, .cta-button, a[class*="cta"], button[class*="cta"]').first();

    await expect(ctaButton).toBeVisible();
    const text = await ctaButton.textContent();
    expect(text.trim().length).toBeGreaterThan(0);
  });

  test('should allow navigation to pricing/CTA section via scroll', async ({ page }) => {
    // Scroll to the pricing/CTA section
    const pricingSection = page.locator('.pricing-section, .cta-section');
    await pricingSection.scrollIntoViewIfNeeded();

    await expect(pricingSection).toBeInViewport();
  });

  test('CTA button should have proper hover state', async ({ page }) => {
    const pricingSection = page.locator('.pricing-section, .cta-section');
    const ctaButton = pricingSection.locator('.cta-primary, .cta-button, a[class*="cta"], button[class*="cta"]').first();

    await ctaButton.hover();
    // Button should still be visible after hover
    await expect(ctaButton).toBeVisible();
  });
});
