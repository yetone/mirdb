/**
 * Shared Test Utilities
 * Owner: First Scenario Builder
 *
 * Exports:
 * - navigateToSection(page, sectionId): Navigate to specific section
 * - waitForPageLoad(page): Wait for full page load
 * - setViewport(page, device): Set viewport for device testing
 * - checkA11y(page): Run accessibility checks
 */
import { Page, expect } from '@playwright/test';

/**
 * Navigate to a specific section on the page
 */
export async function navigateToSection(page: Page, sectionId: string): Promise<void> {
  await page.goto(`/#${sectionId}`);
  await page.waitForSelector(`#${sectionId}`, { state: 'visible' });
}

/**
 * Wait for full page load including images
 */
export async function waitForPageLoad(page: Page): Promise<void> {
  await page.waitForLoadState('networkidle');
}

/**
 * Set viewport for specific device testing
 */
export async function setViewport(page: Page, device: 'mobile' | 'tablet' | 'desktop'): Promise<void> {
  const viewports = {
    mobile: { width: 375, height: 667 },
    tablet: { width: 768, height: 1024 },
    desktop: { width: 1280, height: 800 },
  };
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
