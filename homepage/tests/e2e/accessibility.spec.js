/**
 * E2E tests for accessibility compliance
 * Owner: Scenario 7 - Accessibility Compliance
 *
 * Tests NFR-2, NFR-3, and accessibility considerations:
 * - Semantic HTML validation
 * - Heading hierarchy
 * - ARIA labels presence
 * - Keyboard navigation
 * - Color contrast ratios
 * - Focus indicators
 * - Alt text for images
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

// Base URL for the homepage
const BASE_URL = 'http://localhost:3000';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  // Test Case 1: Check for header element
  test('should have semantic <header> element', async ({ page }) => {
    const header = page.locator('header');
    await expect(header).toBeVisible();
    await expect(header).toHaveCount(1);

    // Verify header has proper role
    const role = await header.getAttribute('role');
    expect(role === 'banner' || role === null).toBeTruthy();
  });

  // Test Case 2: Check for nav element
  test('should have semantic <nav> element for navigation', async ({ page }) => {
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Verify nav has proper role or aria-label
    const role = await nav.getAttribute('role');
    const ariaLabel = await nav.getAttribute('aria-label');
    expect(role === 'navigation' || ariaLabel !== null).toBeTruthy();
  });

  // Test Case 3: Check for main element
  test('should have semantic <main> element wrapping primary content', async ({ page }) => {
    const main = page.locator('main');
    await expect(main).toBeVisible();
    await expect(main).toHaveCount(1);

    // Verify main has proper role
    const role = await main.getAttribute('role');
    expect(role === 'main' || role === null).toBeTruthy();
  });

  // Test Case 4: Check for footer element
  test('should have semantic <footer> element', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
    await expect(footer).toHaveCount(1);

    // Verify footer has proper role
    const role = await footer.getAttribute('role');
    expect(role === 'contentinfo' || role === null).toBeTruthy();
  });

  // Test Case 5: Verify single h1 element
  test('should have exactly one <h1> element', async ({ page }) => {
    const h1Elements = page.locator('h1');
    await expect(h1Elements).toHaveCount(1);

    // Verify h1 has meaningful content
    const h1Text = await h1Elements.textContent();
    expect(h1Text.trim().length).toBeGreaterThan(0);
  });

  // Test Case 6: Verify heading hierarchy
  test('should have proper heading hierarchy without skipped levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) => {
      return elements.map(el => ({
        tag: el.tagName.toLowerCase(),
        level: parseInt(el.tagName.replace('H', '')),
        text: el.textContent.trim().substring(0, 50)
      }));
    });

    // Verify we have headings
    expect(headings.length).toBeGreaterThan(0);

    // Check that first heading is h1
    expect(headings[0].level).toBe(1);

    // Check for no skipped levels
    let maxLevelSoFar = 1;
    for (let i = 1; i < headings.length; i++) {
      const currentLevel = headings[i].level;
      // Current heading can be same level, one level deeper, or any level back up
      if (currentLevel > maxLevelSoFar + 1) {
        throw new Error(
          `Heading hierarchy violation: Found ${headings[i].tag} after ${headings[i-1].tag}. ` +
          `Skipped from level ${maxLevelSoFar} to level ${currentLevel}. ` +
          `Text: "${headings[i].text}"`
        );
      }
      if (currentLevel > maxLevelSoFar) {
        maxLevelSoFar = currentLevel;
      }
    }
  });

  // Test Case 7: Check for image alt text
  test('should have alt attributes on all <img> elements', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    // Skip if no images
    if (imageCount === 0) {
      return;
    }

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      expect(alt, `Image ${src} should have alt attribute`).not.toBeNull();
    }
  });

  // Test Case 8: Check logo alt text
  test('should have descriptive alt text containing "MirDB" on logo', async ({ page }) => {
    // Find the logo image (in hero section or header)
    const heroLogo = page.locator('.hero-logo');
    const headerLogo = page.locator('.header-logo img');

    // Check hero logo
    if (await heroLogo.count() > 0) {
      const altText = await heroLogo.getAttribute('alt');
      expect(altText, 'Hero logo should have alt text').not.toBeNull();
      expect(altText.toLowerCase()).toContain('mirdb');
    }

    // Check header logo
    if (await headerLogo.count() > 0) {
      const altText = await headerLogo.getAttribute('alt');
      expect(altText, 'Header logo should have alt text').not.toBeNull();
      expect(altText.toLowerCase()).toContain('mirdb');
    }
  });

  // Test Case 9: Test keyboard Tab navigation
  test('should allow Tab key to navigate through all interactive elements', async ({ page }) => {
    // Start from the beginning
    await page.keyboard.press('Tab');

    // Collect focused elements by pressing Tab
    const focusedElements = [];
    const maxTabs = 50; // Prevent infinite loop

    for (let i = 0; i < maxTabs; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tag: el.tagName.toLowerCase(),
          href: el.getAttribute('href'),
          text: el.textContent?.trim().substring(0, 30),
          type: el.getAttribute('type'),
          role: el.getAttribute('role'),
          ariaLabel: el.getAttribute('aria-label')
        };
      });

      if (!focusedElement) break;

      // Check if we've cycled back to the beginning
      if (focusedElements.length > 0) {
        const firstElement = focusedElements[0];
        if (
          focusedElement.tag === firstElement.tag &&
          focusedElement.href === firstElement.href &&
          focusedElement.text === firstElement.text
        ) {
          break;
        }
      }

      focusedElements.push(focusedElement);
      await page.keyboard.press('Tab');
    }

    // Should have multiple focusable elements
    expect(focusedElements.length).toBeGreaterThan(3);

    // Verify we can reach links and buttons
    const hasLinks = focusedElements.some(el => el.tag === 'a');
    const hasButtons = focusedElements.some(el => el.tag === 'button');

    expect(hasLinks, 'Should be able to tab to links').toBeTruthy();
    expect(hasButtons, 'Should be able to tab to buttons').toBeTruthy();
  });

  // Test Case 10: Verify focus indicators
  test('should have visible focus indicators on focusable elements', async ({ page }) => {
    // Verify that CSS contains focus-visible styles for links and buttons
    // This tests that focus indicators are defined in the stylesheets
    const hasFocusStyles = await page.evaluate(() => {
      // Check all stylesheets for focus-visible rules
      let foundFocusVisibleRule = false;

      for (const sheet of document.styleSheets) {
        try {
          for (const rule of sheet.cssRules || []) {
            if (rule.cssText && rule.cssText.includes('focus-visible')) {
              foundFocusVisibleRule = true;
              break;
            }
          }
        } catch (e) {
          // Skip cross-origin stylesheets
        }
        if (foundFocusVisibleRule) break;
      }

      return foundFocusVisibleRule;
    });

    expect(hasFocusStyles, 'CSS should contain focus-visible rules for accessibility').toBeTruthy();

    // Use keyboard navigation and verify focus works
    await page.keyboard.press('Tab');

    // Verify an element received focus
    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el || el === document.body) return null;
      return {
        tag: el.tagName.toLowerCase(),
        classList: Array.from(el.classList)
      };
    });

    expect(focusedElement, 'Keyboard Tab should focus an interactive element').not.toBeNull();
    expect(['a', 'button', 'input', 'select', 'textarea']).toContain(focusedElement.tag);

    // Tab through several elements to verify keyboard navigation works
    let focusableCount = 1;
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
      const nextFocused = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return el.tagName.toLowerCase();
      });
      if (nextFocused && ['a', 'button', 'input', 'select', 'textarea'].includes(nextFocused)) {
        focusableCount++;
      }
    }

    expect(focusableCount, 'Multiple interactive elements should be keyboard focusable').toBeGreaterThan(1);
  });

  // Test Case 11: Check ARIA labels on buttons without text
  test('should have aria-label on icon-only buttons', async ({ page }) => {
    // Find buttons that might be icon-only (like mobile menu toggle, copy buttons)
    const buttons = page.locator('button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const textContent = await button.textContent();
      const ariaLabel = await button.getAttribute('aria-label');
      const ariaLabelledby = await button.getAttribute('aria-labelledby');
      const title = await button.getAttribute('title');

      // If button has minimal or no visible text, it should have aria-label
      const trimmedText = textContent.replace(/\s+/g, ' ').trim();

      // Check for icon-only buttons (text is empty or just "Copy" or similar short text that might be hidden)
      if (trimmedText.length === 0 || trimmedText.length <= 4) {
        const hasAccessibleName = ariaLabel || ariaLabelledby || title;
        expect(
          hasAccessibleName,
          `Icon-only button at index ${i} should have aria-label, aria-labelledby, or title`
        ).toBeTruthy();
      }
    }
  });

  // Test Case 12: Run axe accessibility audit
  test('should pass axe accessibility audit with no critical or serious violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // If there are violations, provide detailed information
    if (criticalViolations.length > 0) {
      const violationDetails = criticalViolations.map(v => ({
        id: v.id,
        impact: v.impact,
        description: v.description,
        helpUrl: v.helpUrl,
        nodes: v.nodes.map(n => ({
          html: n.html.substring(0, 200),
          failureSummary: n.failureSummary
        }))
      }));

      console.error('Accessibility violations found:', JSON.stringify(violationDetails, null, 2));
    }

    expect(
      criticalViolations,
      `Found ${criticalViolations.length} critical/serious accessibility violations`
    ).toHaveLength(0);
  });
});
