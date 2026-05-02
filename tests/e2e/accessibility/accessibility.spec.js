import { test, expect } from '@playwright/test';
import { checkContrast, checkKeyboardNavigation, checkAltText } from '../../utils/accessibility.js';

const HOMEPAGE_URL = 'http://localhost:8080';

// Before all tests, ensure server is running
test.beforeAll(async () => {
  // In real implementation, would start static server
  // For now, assume it's already running
});

// Accessibility Compliance Tests

test('color contrast verification @contrast', async ({ page }) => {
  await page.goto(HOMEPAGE_URL);

  // Check hero section text
  const heroText = page.locator('.hero h1, .hero p');
  const heroContrast = await checkContrast(heroText);
  expect(heroContrast).toBe(true);

  // Check feature cards
  const featureCards = page.locator('.feature-card');
  const allCardsPass = await featureCards.evaluateAll(async (cards) => {
    return Promise.all(cards.map(async card => {
      const contrast = await checkContrast(card);
      return contrast;
    }));
  });

  expect(allCardsPass.every(pass => pass)).toBe(true);
});

test('image alt text verification @alt-text', async ({ page }) => {
  await page.goto(HOMEPAGE_URL);
  const hasValidAltText = await checkAltText(page);
  expect(hasValidAltText).toBe(true);
});

test('keyboard navigation verification @keyboard', async ({ page }) => {
  await page.goto(HOMEPAGE_URL);
  const navigationWorks = await checkKeyboardNavigation(page);
  expect(navigationWorks).toBe(true);
});

test('all accessibility requirements met @full-compliance', async ({ page }) => {
  await page.goto(HOMEPAGE_URL);

  const contrastPass = await checkContrast(page.locator('body'));
  const altTextPass = await checkAltText(page);
  const keyboardPass = await checkKeyboardNavigation(page);

  expect(contrastPass && altTextPass && keyboardPass).toBe(true);
});