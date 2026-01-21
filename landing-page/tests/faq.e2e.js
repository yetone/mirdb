import { test, expect } from '@playwright/test';

/**
 * E2E Tests for FAQ Section
 * Testing REQ-8: FAQ or common questions section
 */

test.describe('FAQ Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check FAQ section exists
   * Input: Check FAQ section exists
   * Expected: FAQ section with questions and answers is present
   */
  test('should have FAQ section visible on page', async ({ page }) => {
    const faqSection = page.locator('.faq-section, #faq');
    await faqSection.scrollIntoViewIfNeeded();
    await expect(faqSection).toBeVisible();
  });

  test('should have FAQ heading visible', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const heading = faqSection.locator('h2');
    await expect(heading).toBeVisible();

    const text = await heading.textContent();
    const lowerText = text.toLowerCase();
    // Heading should contain FAQ-related text
    expect(lowerText.includes('faq') || lowerText.includes('frequently asked') || lowerText.includes('question')).toBe(true);
  });

  /**
   * Test Case 2: Count FAQ items
   * Input: Count FAQ items
   * Expected: At least 3 FAQ items are displayed
   */
  test('should have at least 3 FAQ items visible', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const faqItems = faqSection.locator('.faq-item');
    const count = await faqItems.count();

    expect(count).toBeGreaterThanOrEqual(3);
  });

  test('should have all FAQ questions visible', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const faqQuestions = faqSection.locator('.faq-question');
    const count = await faqQuestions.count();

    expect(count).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < count; i++) {
      await expect(faqQuestions.nth(i)).toBeVisible();
    }
  });

  /**
   * Test Case 3: Test FAQ accordion interaction
   * Input: Test FAQ accordion interaction
   * Expected: Clicking question expands/collapses answer
   */
  test('should expand answer when clicking on a question', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const firstQuestion = faqSection.locator('.faq-question').first();
    const firstAnswer = faqSection.locator('.faq-answer').first();

    // Initially the answer should be hidden
    await expect(firstAnswer).toBeHidden();

    // Click the question
    await firstQuestion.click();

    // Answer should now be visible
    await expect(firstAnswer).toBeVisible();

    // aria-expanded should be true
    await expect(firstQuestion).toHaveAttribute('aria-expanded', 'true');
  });

  test('should collapse answer when clicking on an expanded question', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const firstQuestion = faqSection.locator('.faq-question').first();
    const firstAnswer = faqSection.locator('.faq-answer').first();

    // Click to expand
    await firstQuestion.click();
    await expect(firstAnswer).toBeVisible();

    // Click again to collapse
    await firstQuestion.click();
    await expect(firstAnswer).toBeHidden();

    // aria-expanded should be false
    await expect(firstQuestion).toHaveAttribute('aria-expanded', 'false');
  });

  test('should allow multiple FAQ items to be expanded independently', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const questions = faqSection.locator('.faq-question');
    const answers = faqSection.locator('.faq-answer');

    // Expand first question
    await questions.nth(0).click();
    await expect(answers.nth(0)).toBeVisible();

    // Expand second question
    await questions.nth(1).click();
    await expect(answers.nth(1)).toBeVisible();

    // First should still be expanded
    await expect(answers.nth(0)).toBeVisible();
  });

  test('should be keyboard accessible', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const firstQuestion = faqSection.locator('.faq-question').first();
    const firstAnswer = faqSection.locator('.faq-answer').first();

    // Focus on the first question
    await firstQuestion.focus();

    // Press Enter to expand
    await page.keyboard.press('Enter');
    await expect(firstAnswer).toBeVisible();

    // Press Enter again to collapse
    await page.keyboard.press('Enter');
    await expect(firstAnswer).toBeHidden();
  });

  test('should be keyboard accessible with Space key', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const firstQuestion = faqSection.locator('.faq-question').first();
    const firstAnswer = faqSection.locator('.faq-answer').first();

    // Focus on the first question
    await firstQuestion.focus();

    // Press Space to expand
    await page.keyboard.press('Space');
    await expect(firstAnswer).toBeVisible();
  });

  /**
   * Additional accessibility and visual tests
   */
  test('should have proper focus indicators', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const firstQuestion = faqSection.locator('.faq-question').first();

    // Focus and check that it's focusable
    await firstQuestion.focus();
    await expect(firstQuestion).toBeFocused();
  });

  test('should position FAQ section appropriately on page', async ({ page }) => {
    const ctaSection = page.locator('.cta-section');
    const faqSection = page.locator('.faq-section');

    const ctaBox = await ctaSection.boundingBox();
    const faqBox = await faqSection.boundingBox();

    // FAQ should come after CTA section
    expect(faqBox.y).toBeGreaterThan(ctaBox.y);
  });

  test('FAQ questions should have visible icon indicator', async ({ page }) => {
    const faqSection = page.locator('.faq-section');
    await faqSection.scrollIntoViewIfNeeded();

    const faqIcons = faqSection.locator('.faq-icon');
    const count = await faqIcons.count();

    expect(count).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < count; i++) {
      await expect(faqIcons.nth(i)).toBeVisible();
    }
  });
});
