/**
 * Accessibility Tests
 * Owner: Scenario 6 - Accessibility Compliance
 *
 * Tests:
 * - All images have meaningful alt text
 * - Semantic HTML structure (header, main, footer, sections)
 * - Keyboard navigation works for all interactive elements
 * - Color contrast meets WCAG AA standards
 * - Focus indicators visible
 * - Heading hierarchy is logical (h1 > h2 > h3)
 *
 * Traceability: NFR-2
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/homepage/');
  });

  // Test Case 1: Check for semantic header element
  test('TC1: Page contains semantic header element wrapping navigation/logo', async ({ page }) => {
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify header contains navigation
    const nav = header.locator('nav');
    await expect(nav).toBeVisible();

    // Verify navigation has aria-label for accessibility
    await expect(nav).toHaveAttribute('aria-label', 'Main navigation');
  });

  // Test Case 2: Check for semantic main element
  test('TC2: Page contains semantic main element wrapping primary content', async ({ page }) => {
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Main should contain the primary content sections
    const sections = main.locator('section');
    const count = await sections.count();
    expect(count).toBeGreaterThan(0);
  });

  // Test Case 3: Check for semantic footer element
  test('TC3: Page contains semantic footer element for footer content', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Footer should contain links and copyright info
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);
  });

  // Test Case 4: Check logo image alt text
  test('TC4: Logo image has meaningful alt text describing MirDB logo', async ({ page }) => {
    const logo = page.locator('img.hero-logo');
    await expect(logo).toBeVisible();

    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.toLowerCase()).toContain('logo');
    expect(altText!.toLowerCase()).toContain('mirdb');
  });

  // Test Case 5: Check usage GIF alt text
  test('TC5: Usage demo GIF has meaningful alt text describing the demonstration', async ({ page }) => {
    const usageGif = page.locator('img.hero-demo-image');
    await expect(usageGif).toBeVisible();

    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.toLowerCase()).toContain('usage');
    expect(altText!.length).toBeGreaterThan(10); // Ensure meaningful description
  });

  // Test Case 6: Verify single h1 element
  test('TC6: Page contains exactly one h1 element (main title)', async ({ page }) => {
    const h1Elements = page.locator('h1');
    const count = await h1Elements.count();
    expect(count).toBe(1);

    // Verify h1 contains expected text
    await expect(h1Elements.first()).toContainText('MirDB');
  });

  // Test Case 7: Verify heading hierarchy
  test('TC7: No heading levels are skipped (proper h1 > h2 > h3 hierarchy)', async ({ page }) => {
    // Get all headings in order
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    let lastLevel = 0;
    const violations: string[] = [];

    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const level = parseInt(tagName.charAt(1));

      // Check if heading level is skipped (e.g., h1 followed by h3)
      if (lastLevel > 0 && level > lastLevel + 1) {
        const text = await heading.textContent();
        violations.push(`Skipped from h${lastLevel} to ${tagName}: "${text?.trim()}"`);
      }

      lastLevel = level;
    }

    expect(violations).toEqual([]);
  });

  // Test Case 8: Test Tab key navigation
  test('TC8: Tab key navigates through all interactive elements in logical order', async ({ page }) => {
    // Focus the body first
    await page.locator('body').focus();

    const interactiveElements: string[] = [];
    const expectedMinElements = 5; // At minimum: nav links, CTA buttons, footer links

    // Tab through elements and collect them
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tag: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 30) || '',
          href: (el as HTMLAnchorElement).href || ''
        };
      });

      if (focusedElement) {
        const identifier = focusedElement.text || focusedElement.href;
        if (!interactiveElements.includes(identifier)) {
          interactiveElements.push(identifier);
        }
      }
    }

    // Should find at least the minimum expected interactive elements
    expect(interactiveElements.length).toBeGreaterThanOrEqual(expectedMinElements);
  });

  // Test Case 9: Check focus visibility on buttons
  test('TC9: Focused buttons have visible focus ring or indicator', async ({ page }) => {
    // Find all buttons (CTA buttons)
    const buttons = page.locator('.btn');
    const count = await buttons.count();
    expect(count).toBeGreaterThan(0);

    // Test each button for focus visibility
    for (let i = 0; i < count; i++) {
      const button = buttons.nth(i);
      await button.focus();

      // Check that the button has a visible outline or box-shadow when focused
      const outlineStyle = await button.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return {
          outline: computed.outline,
          outlineWidth: computed.outlineWidth,
          outlineStyle: computed.outlineStyle,
          boxShadow: computed.boxShadow
        };
      });

      // Button should have either outline or box-shadow for focus indication
      const hasVisibleFocus =
        (outlineStyle.outlineStyle !== 'none' && outlineStyle.outlineWidth !== '0px') ||
        outlineStyle.boxShadow !== 'none';

      expect(hasVisibleFocus).toBe(true);
    }
  });

  // Test Case 10: Check focus visibility on links
  test('TC10: Focused links have visible focus ring or indicator', async ({ page }) => {
    // Get a sample of links to test
    const links = page.locator('a').first();
    await expect(links).toBeVisible();

    // Focus the link
    await links.focus();

    // Check that the link has a visible outline when focused
    const outlineStyle = await links.evaluate(el => {
      const computed = window.getComputedStyle(el);
      return {
        outline: computed.outline,
        outlineWidth: computed.outlineWidth,
        outlineStyle: computed.outlineStyle,
        boxShadow: computed.boxShadow
      };
    });

    // Link should have visible focus indicator
    const hasVisibleFocus =
      (outlineStyle.outlineStyle !== 'none' && outlineStyle.outlineWidth !== '0px') ||
      outlineStyle.boxShadow !== 'none';

    expect(hasVisibleFocus).toBe(true);
  });

  // Test Case 11: Run axe accessibility audit
  test('TC11: No critical or serious accessibility violations detected', async ({ page }) => {
    // Run axe accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log violations for debugging if any exist
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Violations:', JSON.stringify(criticalViolations, null, 2));
    }

    expect(criticalViolations).toHaveLength(0);
  });

  // Additional test: ARIA labels are properly set
  test('Sections have proper aria-labelledby attributes', async ({ page }) => {
    const sections = page.locator('section[aria-labelledby]');
    const count = await sections.count();

    // Should have multiple sections with aria-labelledby
    expect(count).toBeGreaterThan(0);

    // Verify each aria-labelledby references an existing element
    for (let i = 0; i < count; i++) {
      const section = sections.nth(i);
      const labelledBy = await section.getAttribute('aria-labelledby');

      if (labelledBy) {
        const labelElement = page.locator(`#${labelledBy}`);
        await expect(labelElement).toBeVisible();
      }
    }
  });

  // Additional test: Color contrast check via axe
  test('Text has sufficient color contrast (WCAG AA)', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Check specifically for color-contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    // Should have no color contrast violations
    expect(contrastViolations).toHaveLength(0);
  });

  // Additional test: Language attribute is set
  test('HTML document has lang attribute set', async ({ page }) => {
    const html = page.locator('html');
    const lang = await html.getAttribute('lang');
    expect(lang).toBeTruthy();
    expect(lang).toBe('en');
  });
});
