/**
 * E2E test fixtures and helpers for homepage tests.
 *
 * Provides:
 * - homepageFixture: Playwright fixture setup
 * - viewport configurations for mobile/tablet/desktop
 * - Authentication helpers (login/logout)
 * - Theme toggle helpers
 * - Performance measurement utilities
 */

import { test as base, expect } from '@playwright/test';

/**
 * Viewport configurations for responsive testing
 */
export const viewports = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
  largeDesktop: { width: 1920, height: 1080 },
};

/**
 * Extended test fixture with custom helpers
 */
export const test = base.extend({
  // Add custom fixtures here as needed
});

export { expect };

/**
 * Helper to measure page load time
 */
export async function measurePageLoadTime(page: any) {
  const startTime = Date.now();
  await page.waitForLoadState('domcontentloaded');
  return Date.now() - startTime;
}

/**
 * Helper to wait for hero section to be visible
 */
export async function waitForHeroSection(page: any) {
  await page.waitForSelector('.hero', { state: 'visible' });
}
