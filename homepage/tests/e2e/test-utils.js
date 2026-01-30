/**
 * MirDB Homepage E2E Tests - Shared Utilities
 * Created by first E2E test builder
 *
 * Shared utilities for Playwright tests:
 * - Page navigation helpers
 * - Element selectors
 * - Viewport configuration
 * - Common assertions
 */

const { expect } = require('@playwright/test');

// Test configuration
const BASE_URL = process.env.BASE_URL || 'http://localhost:8080';

/**
 * Common selectors used across tests
 */
const SELECTORS = {
  header: '#header',
  headerLogo: '.header__logo',
  headerTagline: '.header__tagline',
  headerNav: '.header__nav',
  headerNavLinks: '.header__nav-link',
  hero: '#hero',
  features: '#features',
  architecture: '#architecture',
  usage: '#usage',
  status: '#status',
  gettingStarted: '#getting-started',
  footer: '#footer',
};

/**
 * Viewport sizes for responsive testing
 */
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
};

/**
 * Wait for page to be fully loaded
 * @param {import('@playwright/test').Page} page
 */
async function waitForPageLoad(page) {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

/**
 * Check if an image loaded successfully
 * @param {import('@playwright/test').Page} page
 * @param {string} selector
 */
async function checkImageLoaded(page, selector) {
  const image = page.locator(selector);
  await expect(image).toBeVisible();

  const naturalWidth = await image.evaluate((img) => img.naturalWidth);
  return naturalWidth > 0;
}

module.exports = {
  BASE_URL,
  SELECTORS,
  VIEWPORTS,
  waitForPageLoad,
  checkImageLoaded,
};
