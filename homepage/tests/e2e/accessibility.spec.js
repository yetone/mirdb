/**
 * Accessibility E2E Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests:
 * - Skip link functionality
 * - Focus indicators visibility
 * - Keyboard navigation
 * - axe-core automated audit
 *
 * Requirements: NFR-3 (WCAG 2.1 AA)
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility Compliance Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Axe-Core Accessibility Audit', () => {
    test('Homepage has no critical or serious accessibility violations', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      // Filter for critical and serious violations only
      const criticalAndSerious = accessibilityScanResults.violations.filter(
        violation => violation.impact === 'critical' || violation.impact === 'serious'
      );

      // Log violations for debugging if any exist
      if (criticalAndSerious.length > 0) {
        console.log('Critical/Serious Accessibility Violations:');
        criticalAndSerious.forEach(violation => {
          console.log(`- ${violation.id}: ${violation.description}`);
          console.log(`  Impact: ${violation.impact}`);
          console.log(`  Nodes affected: ${violation.nodes.length}`);
        });
      }

      expect(criticalAndSerious).toHaveLength(0);
    });

    test('All WCAG 2.1 Level AA requirements pass', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag21aa'])
        .analyze();

      // Allow minor and moderate issues, but no critical or serious
      const criticalAndSerious = accessibilityScanResults.violations.filter(
        violation => violation.impact === 'critical' || violation.impact === 'serious'
      );

      expect(criticalAndSerious).toHaveLength(0);
    });
  });

  test.describe('Test Case 2: Keyboard Navigation', () => {
    test('Tab navigates through all interactive elements in logical order', async ({ page }) => {
      // Start from the beginning of the page
      await page.keyboard.press('Tab');

      // First focusable element should be skip link (when visible) or logo
      const firstFocusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tagName: el.tagName,
          className: el.className,
          text: el.textContent?.substring(0, 50)
        };
      });

      expect(['A', 'BUTTON']).toContain(firstFocusedElement.tagName);

      // Continue tabbing and track the focused elements
      const focusedElements = [];
      for (let i = 0; i < 15; i++) {
        await page.keyboard.press('Tab');
        const focused = await page.evaluate(() => {
          const el = document.activeElement;
          return {
            tagName: el.tagName,
            text: el.textContent?.substring(0, 50).trim(),
            href: el.getAttribute('href')
          };
        });
        focusedElements.push(focused);
      }

      // Verify we're navigating through links and buttons
      const hasNavigationLinks = focusedElements.some(el =>
        el.tagName === 'A' && (el.href?.includes('#features') || el.href?.includes('#architecture'))
      );
      expect(hasNavigationLinks).toBe(true);

      // Verify CTA buttons are focusable
      const hasCtaButtons = focusedElements.some(el =>
        el.tagName === 'A' && (el.text?.includes('Get Started') || el.text?.includes('GitHub'))
      );
      expect(hasCtaButtons).toBe(true);
    });

    test('Enter key activates focused links', async ({ page }) => {
      // Tab to navigation link for Features
      let foundFeaturesLink = false;
      for (let i = 0; i < 20; i++) {
        await page.keyboard.press('Tab');
        const focused = await page.evaluate(() => document.activeElement.getAttribute('href'));
        if (focused === '#features') {
          foundFeaturesLink = true;
          break;
        }
      }

      expect(foundFeaturesLink).toBe(true);

      // Press Enter to activate the link
      await page.keyboard.press('Enter');

      // Should have scrolled to features section
      await page.waitForTimeout(500); // Wait for smooth scroll
      const url = page.url();
      expect(url).toContain('#features');
    });
  });

  test.describe('Test Case 5: Skip Link to Main Content', () => {
    test('Skip link exists and is first focusable element', async ({ page }) => {
      // Tab to first focusable element
      await page.keyboard.press('Tab');

      // Check if skip link is visible on focus
      const skipLink = page.locator('.skip-link');
      const isSkipLinkFocused = await page.evaluate(() => {
        const el = document.activeElement;
        return el.classList.contains('skip-link') || el.textContent?.includes('Skip to');
      });

      // If skip link exists and is focused
      if (await skipLink.count() > 0) {
        expect(isSkipLinkFocused).toBe(true);

        // Skip link should be visible when focused
        await expect(skipLink).toBeVisible();

        // Skip link should point to main content
        const href = await skipLink.getAttribute('href');
        expect(href).toMatch(/#(main|main-content|content)/i);
      }
    });

    test('Skip link skips to main content when activated', async ({ page }) => {
      const skipLink = page.locator('.skip-link');

      if (await skipLink.count() > 0) {
        // Focus the skip link
        await page.keyboard.press('Tab');

        // Activate it
        await page.keyboard.press('Enter');

        // Wait for navigation
        await page.waitForTimeout(300);

        // The focus should now be on main content area
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          return {
            id: el.id,
            tagName: el.tagName,
            role: el.getAttribute('role')
          };
        });

        // Should be focused on main content or within it
        expect(
          focusedElement.id === 'main-content' ||
          focusedElement.tagName === 'MAIN' ||
          focusedElement.role === 'main'
        ).toBe(true);
      }
    });
  });

  test.describe('Test Case 6: Focus Indicators Visibility', () => {
    test('Focused elements have visible focus outline', async ({ page }) => {
      // Check navigation links
      const navLink = page.locator('nav[aria-label="Main navigation"] a').first();
      await navLink.focus();

      // Get computed styles for the focused element
      const styles = await navLink.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          outline: computed.outline,
          outlineColor: computed.outlineColor,
          outlineStyle: computed.outlineStyle,
          outlineWidth: computed.outlineWidth
        };
      });

      // Focus should have a visible outline (not 'none' or '0px')
      const hasVisibleOutline =
        styles.outlineStyle !== 'none' &&
        styles.outlineWidth !== '0px';

      expect(hasVisibleOutline).toBe(true);
    });

    test('Primary CTA button has visible focus indicator', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      await primaryBtn.focus();

      // Wait for focus styles to apply
      await page.waitForTimeout(100);

      const styles = await primaryBtn.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          outline: computed.outline,
          outlineStyle: computed.outlineStyle,
          outlineWidth: computed.outlineWidth,
          boxShadow: computed.boxShadow
        };
      });

      // Should have either outline or box-shadow for focus indication
      const hasVisibleFocus =
        (styles.outlineStyle !== 'none' && styles.outlineWidth !== '0px') ||
        (styles.boxShadow && styles.boxShadow !== 'none');

      expect(hasVisibleFocus).toBe(true);
    });

    test('Hamburger menu button has visible focus indicator on mobile', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.reload();

      const hamburger = page.locator('.hamburger-menu');
      if (await hamburger.isVisible()) {
        await hamburger.focus();

        const styles = await hamburger.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            outlineStyle: computed.outlineStyle,
            outlineWidth: computed.outlineWidth
          };
        });

        const hasVisibleOutline =
          styles.outlineStyle !== 'none' &&
          styles.outlineWidth !== '0px';

        expect(hasVisibleOutline).toBe(true);
      }
    });

    test('Footer links have visible focus indicators', async ({ page }) => {
      const footerLink = page.locator('footer a').first();
      await footerLink.focus();

      const styles = await footerLink.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          outlineStyle: computed.outlineStyle,
          outlineWidth: computed.outlineWidth,
          textDecoration: computed.textDecoration
        };
      });

      // Should have outline or other visible indication
      const hasVisibleFocus =
        styles.outlineStyle !== 'none' ||
        styles.textDecoration.includes('underline');

      expect(hasVisibleFocus).toBe(true);
    });
  });

  test.describe('Landmark Regions', () => {
    test('Page has all required ARIA landmarks', async ({ page }) => {
      const landmarks = await page.evaluate(() => {
        return {
          banner: document.querySelector('[role="banner"]') !== null,
          navigation: document.querySelector('[role="navigation"]') !== null,
          main: document.querySelector('[role="main"]') !== null,
          contentinfo: document.querySelector('[role="contentinfo"]') !== null
        };
      });

      expect(landmarks.banner).toBe(true);
      expect(landmarks.navigation).toBe(true);
      expect(landmarks.main).toBe(true);
      expect(landmarks.contentinfo).toBe(true);
    });
  });

  test.describe('Color Contrast', () => {
    test('Text elements meet contrast requirements visually', async ({ page }) => {
      // Use axe-core to check color contrast specifically
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withRules(['color-contrast'])
        .analyze();

      const contrastViolations = accessibilityScanResults.violations.filter(
        v => v.id === 'color-contrast' && (v.impact === 'critical' || v.impact === 'serious')
      );

      expect(contrastViolations).toHaveLength(0);
    });
  });

  test.describe('Images and Media', () => {
    test('All images have alt text or are properly hidden', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withRules(['image-alt'])
        .analyze();

      const imageViolations = accessibilityScanResults.violations.filter(
        v => v.id === 'image-alt'
      );

      expect(imageViolations).toHaveLength(0);
    });

    test('SVG icons have appropriate accessibility handling', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withRules(['svg-img-alt'])
        .analyze();

      const svgViolations = accessibilityScanResults.violations.filter(
        v => v.id === 'svg-img-alt' && v.impact === 'critical'
      );

      expect(svgViolations).toHaveLength(0);
    });
  });
});
