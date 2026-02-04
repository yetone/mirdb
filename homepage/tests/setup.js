/**
 * Test Setup and Utilities
 * Owner: First builder (Shared)
 *
 * Common test utilities and configuration
 */

const { test, expect } = require('@playwright/test');

/**
 * Common test configuration
 */
const config = {
  baseUrl: 'http://localhost:8080',
  viewport: {
    desktop: { width: 1920, height: 1080 },
    tablet: { width: 768, height: 1024 },
    mobile: { width: 375, height: 667 }
  },
  wcag: {
    contrastRatioNormal: 4.5,
    contrastRatioLarge: 3,
  }
};

// Viewport configurations (Scenario 6 - Responsive Design)
const VIEWPORTS = {
  desktop: { width: 1920, height: 1080 },
  desktopSmall: { width: 1366, height: 768 },
  tablet: { width: 768, height: 1024 },
  tabletLandscape: { width: 1024, height: 768 },
  mobile: { width: 375, height: 667 },
  mobileLarge: { width: 414, height: 896 },
};

// Minimum touch target size (WCAG 2.1 AA)
const MIN_TOUCH_TARGET = 44;

// Test selectors - Updated to match actual page structure
const SELECTORS = {
  hero: '#hero',
  heroTitle: '#hero h1',
  heroTagline: '#hero p',
  heroCtaGroup: '#hero .flex',
  heroLogo: '.hero__logo, #hero img',
  featuresSection: '#features',
  featuresGrid: '#features .grid',
  featureCard: '#features .bg-gray-700',
  codeExamples: '#code-examples',
  codeBlock: '#code-examples pre',
  tabs: '.tabs',
  tab: '.tab, button[role="tab"]',
  architecture: '#architecture',
  roadmap: '#roadmap',
  footer: '.footer, footer',
  skipLink: 'a[href="#main-content"]',
  mainContent: '#main-content',
  btn: 'a[class*="bg-"], button[class*="bg-"]',
  btnPrimary: '.bg-primary, .btn--primary',
  btnSecondary: '.bg-gray-700, .btn--secondary',
};

/**
 * Navigate to the homepage
 * @param {import('@playwright/test').Page} page
 */
async function goToHomepage(page) {
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');
}

/**
 * Check if element has visible focus
 * @param {import('@playwright/test').Locator} locator
 * @returns {Promise<boolean>}
 */
async function hasFocusIndicator(locator) {
  const outlineStyle = await locator.evaluate(el => {
    const styles = window.getComputedStyle(el);
    return {
      outline: styles.outline,
      outlineWidth: styles.outlineWidth,
      outlineStyle: styles.outlineStyle,
      boxShadow: styles.boxShadow
    };
  });

  // Check if there's a visible outline or box-shadow
  const hasOutline = outlineStyle.outlineStyle !== 'none' &&
                     outlineStyle.outlineWidth !== '0px';
  const hasBoxShadow = outlineStyle.boxShadow !== 'none';

  return hasOutline || hasBoxShadow;
}

/**
 * Press Tab key and return focused element
 * @param {import('@playwright/test').Page} page
 */
async function pressTab(page) {
  await page.keyboard.press('Tab');
  return page.locator(':focus');
}

/**
 * Check element visibility
 * @param {import('@playwright/test').Locator} locator
 */
async function isVisuallyVisible(locator) {
  const boundingBox = await locator.boundingBox();
  if (!boundingBox) return false;

  // Check if element is within viewport and has dimensions
  return boundingBox.width > 0 && boundingBox.height > 0;
}

module.exports = {
  config,
  goToHomepage,
  hasFocusIndicator,
  pressTab,
  isVisuallyVisible,
  test,
  expect,
  VIEWPORTS,
  MIN_TOUCH_TARGET,
  SELECTORS
};
