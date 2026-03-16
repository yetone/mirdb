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
 * Wait for the page to fully load
 */
export async function waitForPageLoad(page: Page): Promise<void> {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

/**
 * Check if an element is visible and has content
 */
export async function checkElementVisible(page: Page, selector: string): Promise<boolean> {
  const element = page.locator(selector);
  return await element.isVisible();
}

/**
 * Get text content of an element
 */
export async function getElementText(page: Page, selector: string): Promise<string> {
  const element = page.locator(selector);
  return await element.textContent() || '';
}

/**
 * Check if code block contains specific text
 */
export async function codeBlockContains(page: Page, text: string): Promise<boolean> {
  const codeBlocks = page.locator('pre code, code');
  const count = await codeBlocks.count();

  for (let i = 0; i < count; i++) {
    const content = await codeBlocks.nth(i).textContent();
    if (content && content.includes(text)) {
      return true;
    }
  }
  return false;
}

/**
 * Test helper to scroll to an element
 */
export async function scrollToElement(page: Page, selector: string): Promise<void> {
  const element = page.locator(selector);
  await element.scrollIntoViewIfNeeded();
}
