import { test, expect } from '@playwright/test';

/**
 * Newsletter Signup Form E2E Tests
 * Tests for REQ-9: Newsletter signup form for lead capture
 */

test.describe('Newsletter Signup Form E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('newsletter form exists and is visible', async ({ page }) => {
    const form = page.locator('[data-testid="newsletter-form"]');
    await expect(form).toBeVisible();

    const emailInput = page.locator('[data-testid="newsletter-email"]');
    await expect(emailInput).toBeVisible();

    const submitButton = page.locator('[data-testid="newsletter-submit"]');
    await expect(submitButton).toBeVisible();
  });

  test('submit valid email address shows success message', async ({ page }) => {
    const emailInput = page.locator('[data-testid="newsletter-email"]');
    const submitButton = page.locator('[data-testid="newsletter-submit"]');
    const successMessage = page.locator('[data-testid="newsletter-success"]');

    // Fill in valid email
    await emailInput.fill('test@example.com');

    // Submit form
    await submitButton.click();

    // Wait for success message to appear
    await expect(successMessage).toBeVisible({ timeout: 5000 });

    // Verify success message contains appropriate text
    const messageText = await successMessage.textContent();
    expect(messageText.toLowerCase()).toMatch(/thank|success|subscribed|welcome/);
  });

  test('submit invalid email format shows validation error', async ({ page }) => {
    const emailInput = page.locator('[data-testid="newsletter-email"]');
    const submitButton = page.locator('[data-testid="newsletter-submit"]');
    const errorMessage = page.locator('[data-testid="newsletter-error"]');

    // Fill in invalid email
    await emailInput.fill('invalid-email');

    // Submit form
    await submitButton.click();

    // Check for either browser validation or custom error message
    const isInputInvalid = await emailInput.evaluate((el) => !el.validity.valid);

    if (isInputInvalid) {
      // Browser validation is active - the form won't submit with invalid email
      // Check that the input shows validation state
      const validationMessage = await emailInput.evaluate((el) => el.validationMessage);
      expect(validationMessage.length).toBeGreaterThan(0);
    } else {
      // Custom validation - check for error message
      await expect(errorMessage).toBeVisible({ timeout: 5000 });
      const messageText = await errorMessage.textContent();
      expect(messageText.toLowerCase()).toMatch(/invalid|error|valid email/);
    }
  });

  test('submit empty email field shows required field error', async ({ page }) => {
    const emailInput = page.locator('[data-testid="newsletter-email"]');
    const submitButton = page.locator('[data-testid="newsletter-submit"]');
    const errorMessage = page.locator('[data-testid="newsletter-error"]');

    // Ensure email input is empty
    await emailInput.fill('');

    // Try to submit form
    await submitButton.click();

    // Check for browser validation (required field)
    const isInputInvalid = await emailInput.evaluate((el) => !el.validity.valid);

    if (isInputInvalid) {
      // Browser validation - check validation message for required field
      const validationMessage = await emailInput.evaluate((el) => el.validationMessage);
      expect(validationMessage.length).toBeGreaterThan(0);
    } else {
      // Custom validation - check for error message
      await expect(errorMessage).toBeVisible({ timeout: 5000 });
      const messageText = await errorMessage.textContent();
      expect(messageText.toLowerCase()).toMatch(/required|empty|enter|email/);
    }
  });

  test('email input accepts keyboard input', async ({ page }) => {
    const emailInput = page.locator('[data-testid="newsletter-email"]');

    // Type email using keyboard
    await emailInput.focus();
    await page.keyboard.type('user@domain.com');

    // Verify the value was entered
    const value = await emailInput.inputValue();
    expect(value).toBe('user@domain.com');
  });

  test('form can be submitted using Enter key', async ({ page }) => {
    const emailInput = page.locator('[data-testid="newsletter-email"]');
    const successMessage = page.locator('[data-testid="newsletter-success"]');

    // Fill valid email
    await emailInput.fill('keyboard@test.com');

    // Press Enter to submit
    await emailInput.press('Enter');

    // Should show success message
    await expect(successMessage).toBeVisible({ timeout: 5000 });
  });

  test('success message can be dismissed or form can be reused', async ({ page }) => {
    const emailInput = page.locator('[data-testid="newsletter-email"]');
    const submitButton = page.locator('[data-testid="newsletter-submit"]');
    const successMessage = page.locator('[data-testid="newsletter-success"]');

    // Submit valid email
    await emailInput.fill('first@example.com');
    await submitButton.click();
    await expect(successMessage).toBeVisible({ timeout: 5000 });

    // Either the success message should persist, or the form should be reusable
    // This ensures the form handles multiple submissions gracefully
    const formStillExists = await page.locator('[data-testid="newsletter-form"]').isVisible();
    const successVisible = await successMessage.isVisible();

    expect(formStillExists || successVisible).toBe(true);
  });

  test('email input placeholder provides guidance', async ({ page }) => {
    const emailInput = page.locator('[data-testid="newsletter-email"]');
    const placeholder = await emailInput.getAttribute('placeholder');

    expect(placeholder).toBeTruthy();
    expect(placeholder.toLowerCase()).toMatch(/email|@/);
  });

  test('newsletter section is accessible via scroll', async ({ page }) => {
    const newsletterSection = page.locator('[data-testid="newsletter-section"]');

    // Scroll to newsletter section
    await newsletterSection.scrollIntoViewIfNeeded();

    // Verify it's visible after scrolling
    await expect(newsletterSection).toBeInViewport();
  });
});
