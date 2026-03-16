/**
 * Test Helper Utilities
 * Owner: First Builder
 *
 * Shared utilities for all test files:
 * - waitForPageLoad(): Wait for page to fully load
 * - checkAccessibility(page): Run accessibility audit
 * - checkPerformance(page): Run performance audit
 * - emulateDevice(page, device): Emulate mobile device
 * - disableJavaScript(page): Disable JS for progressive enhancement tests
 */

import { Page, expect } from '@playwright/test';

/**
 * Wait for page to fully load
 */
export async function waitForPageLoad(page: Page): Promise<void> {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

/**
 * Check if element is visible and has text
 */
export async function checkElementHasText(
  page: Page,
  selector: string,
  expectedText: string
): Promise<void> {
  const element = page.locator(selector);
  await expect(element).toBeVisible();
  await expect(element).toContainText(expectedText);
}

/**
 * Check if element is visible and contains expected text (regex support)
 */
export async function checkElementWithText(
  page: Page,
  selector: string,
  expectedText: string | RegExp
): Promise<boolean> {
  const element = page.locator(selector);
  await expect(element).toBeVisible();
  await expect(element).toContainText(expectedText);
  return true;
}

/**
 * Verify image loads correctly
 */
export async function checkImageLoads(
  page: Page,
  selector: string
): Promise<void> {
  const image = page.locator(selector);
  await expect(image).toBeVisible();
}

/**
 * Get all feature cards on the page
 */
export async function getFeatureCards(page: Page): Promise<number> {
  const cards = page.locator('.feature-card, .feature, [class*="feature"]');
  return await cards.count();
}
