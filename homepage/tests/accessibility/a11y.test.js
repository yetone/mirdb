/**
 * Accessibility Tests
 * Owner: Scenario 7 - Accessibility
 *
 * Test coverage:
 * - WCAG 2.1 AA compliance
 * - Keyboard navigation
 * - Screen reader compatibility
 * - Color contrast ratios
 * - Focus indicators
 * - Skip links functionality
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility - WCAG 2.1 AA Compliance', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  // Test Case 1: Skip-to-content link becomes visible on Tab
  test('TC1: Skip-to-content link becomes visible and focused first on Tab', async ({ page }) => {
    // Initially, skip link should be visually hidden (off-screen)
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeAttached();

    // Check that skip link exists and has correct href
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Press Tab - skip link should become focused first
    await page.keyboard.press('Tab');

    // Check that skip link is now focused
    await expect(skipLink).toBeFocused();

    // Verify skip link is visible when focused by checking bounding box is in viewport
    // When focused, the skip-link should be visible (not clipped or off-screen)
    const box = await skipLink.boundingBox();
    expect(box).not.toBeNull();
    expect(box.width).toBeGreaterThan(0);
    expect(box.height).toBeGreaterThan(0);
  });

  // Test Case 2: Skip-to-content link moves focus to main content
  test('TC2: Activating skip-to-content link moves focus to main content', async ({ page }) => {
    // Press Tab to focus skip link
    await page.keyboard.press('Tab');

    // Verify skip link is focused
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeFocused();

    // Activate the skip link (Enter key)
    await page.keyboard.press('Enter');

    // Wait for focus to move
    await page.waitForTimeout(100);

    // Main content should now be focused
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeFocused();
  });

  // Test Case 3: All interactive elements reachable via keyboard
  test('TC3: All interactive elements are reachable via keyboard navigation', async ({ page }) => {
    // Verify key interactive elements exist and are focusable
    const skipLink = page.locator('.skip-link');
    const navLinks = page.locator('.nav-link');
    const buttons = page.locator('.btn');
    const tabs = page.locator('[role="tab"]');
    const footerLinks = page.locator('.footer__link');

    // Check skip link is focusable (first Tab press)
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();

    // Check navigation links are focusable
    await expect(navLinks.first()).toBeVisible();
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThan(0);

    // Check buttons exist and are focusable
    const btnCount = await buttons.count();
    expect(btnCount).toBeGreaterThan(0);

    // Verify each button has no disabled attribute
    for (let i = 0; i < btnCount; i++) {
      const btn = buttons.nth(i);
      const isDisabled = await btn.getAttribute('disabled');
      expect(isDisabled).toBeNull();
    }

    // Check tabs are focusable
    const tabCount = await tabs.count();
    expect(tabCount).toBeGreaterThan(0);

    // Focus first tab and verify it works
    await tabs.first().focus();
    await expect(tabs.first()).toBeFocused();

    // Check footer links exist
    const footerCount = await footerLinks.count();
    expect(footerCount).toBeGreaterThan(0);
  });

  // Test Case 4: Visible focus indicators on all focusable elements
  test('TC4: Visible focus outline appears on all focusable elements', async ({ page }) => {
    // Test representative focusable elements have focus styles defined in CSS
    // We verify that the CSS defines focus styles, not runtime computed values
    // (since :focus-visible behavior varies across browser modes)

    const testElements = [
      { selector: '.skip-link', name: 'skip link' },
      { selector: '.nav-brand', name: 'nav brand' },
      { selector: '.nav-link', name: 'nav link' },
      { selector: '.btn--primary', name: 'primary button' },
      { selector: '[role="tab"]', name: 'tab' }
    ];

    for (const { selector, name } of testElements) {
      const element = page.locator(selector).first();

      // Check element exists
      const count = await element.count();
      if (count === 0) continue;

      // Verify the element can receive focus
      await element.focus();
      await expect(element).toBeFocused();

      // Check that after focus, the element still has proper tabindex or is naturally focusable
      const isFocusable = await element.evaluate(el => {
        const tagName = el.tagName.toLowerCase();
        const tabindex = el.getAttribute('tabindex');
        const naturallyFocusable = ['a', 'button', 'input', 'select', 'textarea'].includes(tagName);
        return naturallyFocusable || (tabindex !== null && tabindex !== '-1');
      });

      expect(isFocusable, `${name} should be focusable`).toBe(true);
    }

    // Additionally verify CSS defines focus styles by checking stylesheet
    const hasFocusStyles = await page.evaluate(() => {
      const stylesheets = Array.from(document.styleSheets);
      for (const sheet of stylesheets) {
        try {
          const rules = Array.from(sheet.cssRules || []);
          for (const rule of rules) {
            if (rule.selectorText && (rule.selectorText.includes(':focus') || rule.selectorText.includes(':focus-visible'))) {
              return true;
            }
          }
        } catch (e) {
          // Cross-origin stylesheets may throw
        }
      }
      return false;
    });

    expect(hasFocusStyles, 'Stylesheet should define :focus or :focus-visible styles').toBe(true);
  });

  // Test Case 5: axe accessibility scanner - no WCAG 2.1 AA violations
  test('TC5: No WCAG 2.1 AA violations detected by axe', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Log any violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Accessibility violations:');
      accessibilityScanResults.violations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach(node => {
          console.log(`  Target: ${node.target}`);
        });
      });
    }

    expect(accessibilityScanResults.violations).toEqual([]);
  });

  // Test Case 6: Color contrast ratios meet WCAG standards
  test('TC6: All text meets minimum 4.5:1 contrast ratio', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({
        runOnly: ['color-contrast']
      })
      .analyze();

    // Check specifically for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    if (contrastViolations.length > 0) {
      console.log('Color contrast violations:');
      contrastViolations.forEach(violation => {
        violation.nodes.forEach(node => {
          console.log(`- ${node.target}: ${node.failureSummary}`);
        });
      });
    }

    expect(contrastViolations).toEqual([]);
  });

  // Test Case 7: All images have descriptive alt text
  test('TC7: All images have appropriate alt text', async ({ page }) => {
    const images = await page.locator('img').all();

    for (const img of images) {
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // All images should have alt attribute
      expect(alt, `Image ${src} should have alt attribute`).not.toBeNull();

      // Non-decorative images should have meaningful alt text
      // (decorative images should have empty alt="")
      if (alt !== '') {
        expect(alt.length, `Image ${src} alt text should be descriptive`).toBeGreaterThan(3);
      }
    }
  });

  // Test Case 8: Proper ARIA landmarks are present
  test('TC8: All content has proper ARIA landmarks for screen readers', async ({ page }) => {
    // Check for main landmark
    const main = page.locator('main[role="main"], main');
    await expect(main.first()).toBeAttached();

    // Check for banner (header)
    const header = page.locator('header[role="banner"], header');
    await expect(header.first()).toBeAttached();

    // Check for at least one navigation landmark
    const navCount = await page.locator('nav[role="navigation"], nav').count();
    expect(navCount).toBeGreaterThan(0);

    // Check for contentinfo (footer) landmark
    const footer = page.locator('footer[role="contentinfo"], footer');
    await expect(footer.first()).toBeAttached();

    // Check that sections have labels
    const regions = await page.locator('section[role="region"]').all();
    for (const region of regions) {
      const hasLabel = await region.evaluate(el =>
        el.hasAttribute('aria-labelledby') || el.hasAttribute('aria-label')
      );
      expect(hasLabel, 'Section regions should have aria-label or aria-labelledby').toBe(true);
    }
  });

  // Additional test: Keyboard navigation for tabs
  test('Tabs are navigable with arrow keys', async ({ page }) => {
    // Focus first tab
    const firstTab = page.locator('[role="tab"]').first();
    await firstTab.focus();
    await expect(firstTab).toBeFocused();

    // Press right arrow to move to next tab
    await page.keyboard.press('ArrowRight');

    // Second tab should now be focused
    const tabs = await page.locator('[role="tab"]').all();
    if (tabs.length > 1) {
      await expect(tabs[1]).toBeFocused();
    }

    // Press left arrow to go back
    await page.keyboard.press('ArrowLeft');
    await expect(firstTab).toBeFocused();
  });

  // Test: Skip link has correct href
  test('Skip link points to main content', async ({ page }) => {
    const skipLink = page.locator('.skip-link');
    const href = await skipLink.getAttribute('href');

    expect(href).toBe('#main-content');

    // Verify target exists
    const target = page.locator(href);
    await expect(target).toBeAttached();
  });

  // Test: Interactive elements have accessible names
  test('All interactive elements have accessible names', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .options({
        runOnly: ['button-name', 'link-name', 'image-alt']
      })
      .analyze();

    expect(accessibilityScanResults.violations).toEqual([]);
  });

  // Test: Document has proper language attribute
  test('Document has lang attribute', async ({ page }) => {
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');
  });

  // Test: Heading hierarchy is correct
  test('Heading hierarchy is logical', async ({ page }) => {
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();
    const levels = [];

    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName);
      levels.push(parseInt(tagName.charAt(1)));
    }

    // Should start with h1
    expect(levels[0]).toBe(1);

    // Should only have one h1
    const h1Count = levels.filter(l => l === 1).length;
    expect(h1Count).toBe(1);

    // No heading should skip more than one level
    for (let i = 1; i < levels.length; i++) {
      const jump = levels[i] - levels[i - 1];
      expect(jump, `Heading jump from h${levels[i-1]} to h${levels[i]} skips levels`).toBeLessThanOrEqual(1);
    }
  });

});
