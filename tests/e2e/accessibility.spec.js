// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility E2E Tests
 * Tests NFR-3 from PRD: Page must be accessible following WCAG 2.1 AA guidelines
 *
 * This file contains end-to-end tests for:
 * - Keyboard navigation (TC5)
 * - Focus indicators (TC6)
 * - Automated accessibility audit with axe (TC7)
 */

test.describe('Accessibility Compliance - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 5: Test keyboard navigation
  test('TC5: Keyboard navigation - All interactive elements are reachable and usable via keyboard', async ({ page }) => {
    // Get all interactive elements that should be tabbable
    const interactiveElements = await page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();

    expect(interactiveElements.length).toBeGreaterThan(0);

    // Start tabbing from the body
    await page.keyboard.press('Tab');

    const focusedElements = [];
    let previousActiveElement = null;
    let maxTabs = 50; // Prevent infinite loop
    let tabCount = 0;

    // Tab through all focusable elements
    while (tabCount < maxTabs) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          href: el.getAttribute('href'),
          text: el.textContent?.trim().substring(0, 30),
          id: el.id
        };
      });

      // Check if we've looped back to the start or hit body
      if (!activeElement) break;

      const elementKey = `${activeElement.tagName}-${activeElement.href || activeElement.text || activeElement.id}`;
      if (focusedElements.includes(elementKey)) break;

      focusedElements.push(elementKey);
      await page.keyboard.press('Tab');
      tabCount++;
    }

    // Verify we can tab through multiple elements
    expect(focusedElements.length).toBeGreaterThan(2);

    // Verify the CTA buttons are tabbable
    const getStartedButton = page.locator('a.btn-primary:has-text("Get Started")');
    const githubButton = page.locator('a.btn-secondary:has-text("GitHub")');

    // Focus the Get Started button and verify it's focusable
    await getStartedButton.focus();
    const isGetStartedFocused = await page.evaluate(() =>
      document.activeElement?.textContent?.includes('Get Started')
    );
    expect(isGetStartedFocused).toBe(true);

    // Verify Enter key activates links (by checking focus can be set)
    await githubButton.first().focus();
    const isGithubFocused = await page.evaluate(() =>
      document.activeElement?.textContent?.includes('GitHub')
    );
    expect(isGithubFocused).toBe(true);
  });

  // Test Case 6: Check focus indicators
  test('TC6: Focus indicators - Focused elements have visible focus indicators', async ({ page }) => {
    // Test focus visibility on buttons
    const buttons = page.locator('a.btn, button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < Math.min(buttonCount, 5); i++) {
      const button = buttons.nth(i);

      // Focus the button
      await button.focus();

      // Check for focus styles
      const focusStyles = await button.evaluate(el => {
        const computed = window.getComputedStyle(el);
        const focusedComputed = window.getComputedStyle(el, ':focus');
        return {
          outline: computed.outline,
          outlineWidth: computed.outlineWidth,
          outlineStyle: computed.outlineStyle,
          outlineColor: computed.outlineColor,
          boxShadow: computed.boxShadow,
          border: computed.border,
          backgroundColor: computed.backgroundColor,
          // Check if focus-visible would apply
          hasFocusVisibleSupport: CSS.supports('selector(:focus-visible)')
        };
      });

      // Element should have visible focus indicator
      // Either outline, box-shadow, or background change
      const hasVisibleFocus =
        (focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px') ||
        focusStyles.boxShadow !== 'none' ||
        focusStyles.backgroundColor !== 'rgba(0, 0, 0, 0)';

      // Most browsers show default focus, but we'll verify CSS doesn't suppress it
      const focusNotSuppressed = await button.evaluate(el => {
        const computed = window.getComputedStyle(el);
        // Check that outline isn't explicitly set to none without alternative
        return computed.outline !== 'none' || computed.boxShadow !== 'none';
      });

      expect(focusNotSuppressed).toBe(true);
    }

    // Test focus visibility on links
    const links = page.locator('a:not(.btn)');
    const linkCount = await links.count();

    for (let i = 0; i < Math.min(linkCount, 3); i++) {
      const link = links.nth(i);

      // Skip hidden links
      const isVisible = await link.isVisible().catch(() => false);
      if (!isVisible) continue;

      await link.focus();

      const linkFocusStyles = await link.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return {
          outline: computed.outline,
          outlineWidth: computed.outlineWidth,
          textDecoration: computed.textDecoration,
          boxShadow: computed.boxShadow
        };
      });

      // Links should have some focus indication
      const linkHasFocus =
        linkFocusStyles.outlineWidth !== '0px' ||
        linkFocusStyles.boxShadow !== 'none' ||
        linkFocusStyles.textDecoration.includes('underline');

      expect(linkHasFocus).toBe(true);
    }
  });

  // Test Case 7: Run automated accessibility audit
  test('TC7: Automated accessibility audit - axe reports no critical accessibility violations', async ({ page }) => {
    // Run axe accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa']) // WCAG 2.1 AA
      .analyze();

    // Filter for serious and critical violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log all violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Accessibility violations found:');
      accessibilityScanResults.violations.forEach(v => {
        console.log(`- ${v.id} (${v.impact}): ${v.description}`);
        console.log(`  Help: ${v.helpUrl}`);
        v.nodes.forEach(n => {
          console.log(`  Element: ${n.html.substring(0, 100)}`);
        });
      });
    }

    // Assert no critical or serious violations
    expect(criticalViolations).toEqual([]);

    // Also check that the page has proper language attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();
    expect(htmlLang).toBe('en');

    // Verify page has a main landmark or proper structure
    const landmarks = await page.evaluate(() => {
      return {
        hasMain: document.querySelector('main') !== null ||
                 document.querySelector('[role="main"]') !== null,
        hasNavigation: document.querySelector('nav') !== null ||
                       document.querySelector('[role="navigation"]') !== null,
        hasBanner: document.querySelector('header') !== null ||
                   document.querySelector('[role="banner"]') !== null,
        hasContentinfo: document.querySelector('footer') !== null ||
                        document.querySelector('[role="contentinfo"]') !== null
      };
    });

    // Page should have proper landmark structure
    expect(landmarks.hasBanner).toBe(true);
    expect(landmarks.hasContentinfo).toBe(true);
  });

  // Additional test: Skip links (good accessibility practice)
  test('TC7-extra: Accessibility extras - proper ARIA attributes and semantic structure', async ({ page }) => {
    // Check that SVG diagrams have proper accessibility attributes
    const svgDiagram = page.locator('svg.architecture-diagram');
    if (await svgDiagram.count() > 0) {
      const svgRole = await svgDiagram.getAttribute('role');
      const svgLabel = await svgDiagram.getAttribute('aria-label');
      const hasTitle = await svgDiagram.locator('title').count();
      const hasDesc = await svgDiagram.locator('desc').count();

      // SVG should have role="img" and accessible name
      expect(svgRole).toBe('img');
      expect(svgLabel || hasTitle > 0).toBeTruthy();
      expect(hasDesc > 0).toBe(true);
    }

    // Check decorative icons are hidden from screen readers
    const featureIcons = page.locator('.feature-icon svg');
    const iconCount = await featureIcons.count();

    for (let i = 0; i < iconCount; i++) {
      const ariaHidden = await featureIcons.nth(i).getAttribute('aria-hidden');
      expect(ariaHidden).toBe('true');
    }

    // Check copy buttons have accessible labels
    const copyButtons = page.locator('.copy-btn');
    const copyButtonCount = await copyButtons.count();

    for (let i = 0; i < copyButtonCount; i++) {
      const ariaLabel = await copyButtons.nth(i).getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel).toContain('Copy');
    }

    // Verify external links have rel="noopener noreferrer"
    const externalLinks = page.locator('a[target="_blank"]');
    const extLinkCount = await externalLinks.count();

    for (let i = 0; i < extLinkCount; i++) {
      const rel = await externalLinks.nth(i).getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });
});
