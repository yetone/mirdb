/**
 * Shared Test Utilities
 * Owner: First Scenario Builder
 *
 * Exports:
 * - navigateToSection(page, sectionId): Navigate to specific section
 * - waitForPageLoad(page): Wait for full page load
 * - setViewport(page, device): Set viewport for device testing
 * - checkElementText(page, selector, text): Check element visibility and text
 * - checkImageLoads(page, selector): Check if image loads successfully
 */

import { Page, expect } from '@playwright/test';

/**
 * Navigate to a specific section by scrolling to its element
 */
export async function navigateToSection(page: Page, sectionId: string): Promise<void> {
  await page.evaluate((id) => {
    const element = document.getElementById(id);
    if (element) {
      element.scrollIntoView({ behavior: 'instant', block: 'start' });
    }
  }, sectionId);
}

/**
 * Wait for full page load including images
 */
export async function waitForPageLoad(page: Page): Promise<void> {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

/**
 * Common viewport sizes for testing
 */
export const viewports = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
};

/**
 * Set viewport for specific device testing
 */
export async function setViewport(page: Page, device: keyof typeof viewports): Promise<void> {
  await page.setViewportSize(viewports[device]);
}

/**
 * Check if an element is visible and has expected text
 */
export async function checkElementText(page: Page, selector: string, expectedText: string): Promise<void> {
  const element = page.locator(selector);
  await expect(element).toBeVisible();
  await expect(element).toContainText(expectedText);
}

/**
 * Check if an image loads successfully
 */
export async function checkImageLoads(page: Page, selector: string): Promise<boolean> {
  const image = page.locator(selector);
  await expect(image).toBeVisible();

  const naturalWidth = await image.evaluate((img: HTMLImageElement) => img.naturalWidth);
  return naturalWidth > 0;
}
