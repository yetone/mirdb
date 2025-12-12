import { test, expect } from '@playwright/test';

/**
 * E2E Accessibility Tests - Screen Reader Compatibility (NFR-4)
 * Scenario: Accessibility - Screen Reader Compatibility
 * Description: Verify screen reader compatibility with ARIA labels as per NFR-4
 *
 * Test Case 1: Scan page for ARIA landmarks
 * Input: Scan page for ARIA landmarks
 * Expected: Page has header, main, and footer landmarks
 * Type: e2e
 */
test.describe('TC1: ARIA Landmarks - Page has header, main, and footer landmarks', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have header landmark (banner role)', async ({ page }) => {
    // Check for header/banner landmark
    const header = page.getByRole('banner');
    await expect(header).toBeVisible();
  });

  test('should have main landmark', async ({ page }) => {
    // Check for main content landmark
    const main = page.getByRole('main');
    await expect(main).toBeVisible();
  });

  test('should have footer landmark (contentinfo role)', async ({ page }) => {
    // Check for footer/contentinfo landmark
    const footer = page.getByRole('contentinfo');
    await expect(footer).toBeVisible();
  });

  test('should have navigation landmark', async ({ page }) => {
    // Check for navigation landmark
    const navigation = page.getByRole('navigation');
    await expect(navigation.first()).toBeVisible();
  });

  test('should have all three required landmarks on the page', async ({ page }) => {
    // Verify all three landmarks exist and are accessible
    const header = page.getByRole('banner');
    const main = page.getByRole('main');
    const footer = page.getByRole('contentinfo');

    await expect(header).toBeAttached();
    await expect(main).toBeAttached();
    await expect(footer).toBeAttached();
  });

  test('should have proper landmark structure with header at top', async ({ page }) => {
    const header = page.getByRole('banner');
    const main = page.getByRole('main');

    // Header should exist and be above main content
    const headerBounds = await header.boundingBox();
    const mainBounds = await main.boundingBox();

    expect(headerBounds).not.toBeNull();
    expect(mainBounds).not.toBeNull();

    if (headerBounds && mainBounds) {
      // Header should be above main content (lower y value)
      expect(headerBounds.y).toBeLessThan(mainBounds.y);
    }
  });

  test('should have footer after main content', async ({ page }) => {
    const main = page.getByRole('main');
    const footer = page.getByRole('contentinfo');

    const mainBounds = await main.boundingBox();
    const footerBounds = await footer.boundingBox();

    expect(mainBounds).not.toBeNull();
    expect(footerBounds).not.toBeNull();

    if (mainBounds && footerBounds) {
      // Footer should be below main content
      expect(footerBounds.y).toBeGreaterThan(mainBounds.y);
    }
  });

  test('should have navigation with aria-label for screen readers', async ({ page }) => {
    const navigation = page.getByRole('navigation');

    // Main navigation should have an aria-label
    const mainNav = navigation.first();
    await expect(mainNav).toHaveAttribute('aria-label', /navigation/i);
  });

  test('should have distinct navigation landmarks with different labels', async ({ page }) => {
    const navigations = page.getByRole('navigation');

    // Get all navigation elements
    const count = await navigations.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // If multiple navigations exist, they should have different aria-labels
    if (count > 1) {
      const labels: string[] = [];
      for (let i = 0; i < count; i++) {
        const nav = navigations.nth(i);
        const label = await nav.getAttribute('aria-label');
        if (label) {
          labels.push(label);
        }
      }
      // Check that labels are unique
      const uniqueLabels = new Set(labels);
      expect(uniqueLabels.size).toBe(labels.length);
    }
  });

  test('should have heading structure within main content', async ({ page }) => {
    // Main content should have a primary heading (h1)
    const h1 = page.getByRole('heading', { level: 1 });
    await expect(h1).toBeVisible();
  });

  test('should allow keyboard navigation through landmarks', async ({ page }) => {
    // Tab through the page and verify we can reach different landmarks
    await page.keyboard.press('Tab');

    // Should be able to navigate using Tab key
    const activeElement = await page.evaluate(() => {
      return document.activeElement?.tagName.toLowerCase();
    });

    // First focusable element should be a link or button
    expect(['a', 'button', 'input']).toContain(activeElement);
  });

  test('should have skip link or proper landmark navigation support', async ({ page }) => {
    // Verify that the page structure supports screen reader navigation
    // Either through landmarks or skip links

    // Check that navigating to main content is possible
    const main = page.getByRole('main');
    await expect(main).toBeAttached();

    // Main should contain focusable content
    const mainLocator = await main.elementHandle();
    if (mainLocator) {
      const hasFocusableContent = await page.evaluate((el) => {
        const focusable = el?.querySelectorAll('a, button, input, [tabindex]:not([tabindex="-1"])');
        return focusable && focusable.length > 0;
      }, mainLocator);
      expect(hasFocusableContent).toBe(true);
    }
  });
});

/**
 * Additional E2E accessibility tests for comprehensive screen reader support
 */
test.describe('E2E Accessibility - Screen Reader Support', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('all images should have alt attributes', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img');
    const count = await images.count();

    // Each image should have an alt attribute
    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const altAttr = await img.getAttribute('alt');
      // alt attribute should exist (can be empty string for decorative images)
      expect(altAttr).not.toBeNull();
    }
  });

  test('navigation links should have visible text or aria-label', async ({ page }) => {
    const navLinks = page.getByRole('navigation').getByRole('link');
    const count = await navLinks.count();

    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');

      // Link should have either visible text or aria-label
      const hasAccessibleName = (text && text.trim().length > 0) || (ariaLabel && ariaLabel.trim().length > 0);
      expect(hasAccessibleName).toBe(true);
    }
  });

  test('buttons should have accessible names', async ({ page }) => {
    const buttons = page.getByRole('button');
    const count = await buttons.count();

    for (let i = 0; i < count; i++) {
      const button = buttons.nth(i);
      const text = await button.textContent();
      const ariaLabel = await button.getAttribute('aria-label');

      // Button should have either visible text or aria-label
      const hasAccessibleName = (text && text.trim().length > 0) || (ariaLabel && ariaLabel.trim().length > 0);
      expect(hasAccessibleName).toBe(true);
    }
  });

  test('interactive elements should be focusable', async ({ page }) => {
    // Get all links and buttons
    const links = page.getByRole('link');
    const buttons = page.getByRole('button');

    // Check that links are focusable
    const linkCount = await links.count();
    for (let i = 0; i < Math.min(linkCount, 5); i++) {
      const link = links.nth(i);
      const tabIndex = await link.getAttribute('tabindex');
      // tabindex should be null (default) or >= 0
      if (tabIndex !== null) {
        expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0);
      }
    }

    // Check that buttons are focusable
    const buttonCount = await buttons.count();
    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const tabIndex = await button.getAttribute('tabindex');
      // tabindex should be null (default) or >= 0
      if (tabIndex !== null) {
        expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0);
      }
    }
  });
});
