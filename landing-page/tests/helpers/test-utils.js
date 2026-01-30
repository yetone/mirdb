/**
 * MirDB Landing Page - Test Utilities
 * Owner: First scenario builder
 *
 * Shared test helpers for:
 * - Page navigation and setup
 * - Common assertions
 * - Viewport configuration
 * - Accessibility testing helpers
 */

const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 },
};

/**
 * Navigate to the landing page
 * @param {import('@playwright/test').Page} page
 */
async function setupPage(page) {
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');
}

/**
 * Set viewport for specific device
 * @param {import('@playwright/test').Page} page
 * @param {'mobile' | 'tablet' | 'desktop'} device
 */
async function setViewport(page, device) {
  const viewport = VIEWPORTS[device];
  if (viewport) {
    await page.setViewportSize(viewport);
  }
}

/**
 * Wait for CSS animations to complete
 * @param {import('@playwright/test').Page} page
 */
async function waitForAnimations(page) {
  await page.waitForTimeout(500);
}

module.exports = {
  VIEWPORTS,
  setupPage,
  setViewport,
  waitForAnimations,
};
