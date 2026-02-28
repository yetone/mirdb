/**
 * Shared Test Utilities
 * Owner: First Builder
 *
 * Purpose: Shared helpers for e2e tests
 */

const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1024, height: 768 },
};

async function waitForPageLoad(page) {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForLoadState('networkidle');
}

async function checkContrast(page, element) {
  const styles = await element.evaluate((el) => {
    const computed = window.getComputedStyle(el);
    return {
      color: computed.color,
      backgroundColor: computed.backgroundColor,
    };
  });
  return styles;
}

async function getComputedStyles(page, selector) {
  return await page.locator(selector).evaluate((el) => {
    const computed = window.getComputedStyle(el);
    return {
      color: computed.color,
      backgroundColor: computed.backgroundColor,
      fontSize: computed.fontSize,
      fontWeight: computed.fontWeight,
    };
  });
}

module.exports = {
  VIEWPORTS,
  waitForPageLoad,
  checkContrast,
  getComputedStyles,
};
