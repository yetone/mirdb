/**
 * Shared test utilities for MirDB Homepage E2E tests.
 *
 * Expected exports:
 * - BASE_URL: Local development server URL
 * - VIEWPORTS: Standard viewport sizes for testing
 * - waitForPageLoad(): Helper to wait for full page load
 * - getByTestId(): Custom selector helper
 */

import { Page } from '@playwright/test';

export const BASE_URL = 'http://localhost:3000';

export const VIEWPORTS = {
  desktop: { width: 1920, height: 1080 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 667 },
} as const;

/**
 * Wait for page to fully load including images
 */
export async function waitForPageLoad(page: Page): Promise<void> {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

/**
 * Custom selector helper for data-testid attributes - returns selector string
 */
export function getByTestId(testId: string): string {
  return `[data-testid="${testId}"]`;
}

/**
 * Get element by data-testid attribute - returns Locator
 */
export function getTestIdLocator(page: Page, testId: string) {
  return page.locator(`[data-testid="${testId}"]`);
}

/**
 * GitHub repository URL for MirDB
 */
export const GITHUB_URL = 'https://github.com/yetone/mirdb';

/**
 * Documentation URL (README on GitHub)
 */
export const DOCS_URL = 'https://github.com/yetone/mirdb#readme';
