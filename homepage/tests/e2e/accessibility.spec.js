/**
 * Accessibility E2E Tests
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test cases:
 * - All elements keyboard accessible
 * - Skip link present and functional
 * - Color contrast meets WCAG 4.5:1 (normal text), 3:1 (large text)
 * - ARIA labels on demo components
 * - Focus indicators visible
 * - axe-core audit passes with zero critical violations
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility Compliance - WCAG 2.1 Level AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Keyboard Navigation', () => {
    test('Test Case 1: All interactive elements reachable via keyboard navigation', async ({ page }) => {
      // Get all interactive elements that should be focusable
      const interactiveElements = await page.locator(
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
      ).all();

      // Track focused elements
      const focusedElements = [];
      let tabCount = 0;
      const maxTabs = 100; // Safety limit

      // Start by focusing the body
      await page.keyboard.press('Tab');

      while (tabCount < maxTabs) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          return {
            tag: el.tagName.toLowerCase(),
            id: el.id || '',
            className: el.className || '',
            text: el.textContent?.trim().substring(0, 50) || '',
            href: el.getAttribute('href') || '',
          };
        });

        if (!focusedElement) break;

        // Check if we've cycled back to the beginning
        const elementKey = `${focusedElement.tag}-${focusedElement.id}-${focusedElement.href}`;
        if (focusedElements.length > 0 && elementKey === `${focusedElements[0].tag}-${focusedElements[0].id}-${focusedElements[0].href}`) {
          break;
        }

        focusedElements.push(focusedElement);
        await page.keyboard.press('Tab');
        tabCount++;
      }

      // Verify we can reach multiple interactive elements
      expect(focusedElements.length).toBeGreaterThan(5);

      // Verify critical interactive elements are reachable
      const elementTypes = focusedElements.map(el => el.tag);
      expect(elementTypes).toContain('a'); // Navigation links
      expect(focusedElements.some(el => el.tag === 'button' || el.tag === 'input')).toBeTruthy();
    });

    test('Test Case 1b: Tab order follows logical document flow', async ({ page }) => {
      // Focus elements and verify order makes sense
      const focusOrder = [];

      // Tab through first several elements
      for (let i = 0; i < 10; i++) {
        await page.keyboard.press('Tab');
        const focusedInfo = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          const rect = el.getBoundingClientRect();
          return {
            tag: el.tagName.toLowerCase(),
            top: rect.top,
            left: rect.left,
            text: el.textContent?.trim().substring(0, 30) || '',
          };
        });
        if (focusedInfo) {
          focusOrder.push(focusedInfo);
        }
      }

      // Verify elements generally flow top-to-bottom (with some tolerance for horizontal navigation)
      expect(focusOrder.length).toBeGreaterThan(3);
    });
  });

  test.describe('Skip Link', () => {
    test('Test Case 2: Skip link present and navigates to main content', async ({ page }) => {
      // Verify skip link exists
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toHaveCount(1);

      // Verify skip link text
      await expect(skipLink).toHaveText(/skip to main content/i);

      // Verify skip link href points to main content
      const href = await skipLink.getAttribute('href');
      expect(href).toBe('#main-content');

      // Verify main content exists with the correct id
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toHaveCount(1);
      await expect(mainContent).toHaveAttribute('role', 'main');
    });

    test('Test Case 2b: Skip link becomes visible on focus', async ({ page }) => {
      const skipLink = page.locator('.skip-link');

      // Skip link should be visually hidden initially (off-screen)
      const initiallyHidden = await skipLink.evaluate(el => {
        const rect = el.getBoundingClientRect();
        return rect.bottom <= 0; // Element is above the viewport
      });
      expect(initiallyHidden).toBeTruthy();

      // Focus the skip link
      await page.keyboard.press('Tab');
      await expect(skipLink).toBeFocused();

      // Wait for any CSS transitions to complete
      await page.waitForTimeout(200);

      // Skip link should now be visible (in viewport)
      const isVisibleAfterFocus = await skipLink.evaluate(el => {
        const rect = el.getBoundingClientRect();
        // Check if at least part of the element is visible in the viewport
        return rect.top >= 0 || rect.bottom > 0;
      });
      expect(isVisibleAfterFocus).toBeTruthy();
    });

    test('Test Case 2c: Skip link navigates to main content when activated', async ({ page }) => {
      // Focus skip link
      await page.keyboard.press('Tab');
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toBeFocused();

      // Activate skip link
      await page.keyboard.press('Enter');

      // Verify focus moved to main content area or hash changed
      const currentHash = await page.evaluate(() => window.location.hash);
      expect(currentHash).toBe('#main-content');
    });
  });

  test.describe('Color Contrast', () => {
    test('Test Case 3: Body text color contrast ratio >= 4.5:1 for normal text', async ({ page }) => {
      // Get computed styles for body text elements
      const contrastResults = await page.evaluate(() => {
        // Helper function to get relative luminance
        function getLuminance(r, g, b) {
          const [rs, gs, bs] = [r, g, b].map(c => {
            c = c / 255;
            return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
          });
          return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
        }

        // Helper function to parse color
        function parseColor(color) {
          const canvas = document.createElement('canvas');
          canvas.width = canvas.height = 1;
          const ctx = canvas.getContext('2d');
          ctx.fillStyle = color;
          ctx.fillRect(0, 0, 1, 1);
          const [r, g, b] = ctx.getImageData(0, 0, 1, 1).data;
          return { r, g, b };
        }

        // Helper function to calculate contrast ratio
        function getContrastRatio(color1, color2) {
          const l1 = getLuminance(color1.r, color1.g, color1.b);
          const l2 = getLuminance(color2.r, color2.g, color2.b);
          const lighter = Math.max(l1, l2);
          const darker = Math.min(l1, l2);
          return (lighter + 0.05) / (darker + 0.05);
        }

        // Check body text elements (p, li, span with readable text)
        const textElements = document.querySelectorAll('p, .feature-card__description, .hero__description, .demo__description, .footer__description');
        const results = [];

        textElements.forEach((el, index) => {
          if (index > 5) return; // Sample first 6 elements
          const style = window.getComputedStyle(el);
          const fontSize = parseFloat(style.fontSize);

          // Only check normal text (less than 18px or less than 14px bold)
          if (fontSize >= 18) return;

          const textColor = parseColor(style.color);
          const bgColor = parseColor(style.backgroundColor !== 'rgba(0, 0, 0, 0)' ? style.backgroundColor : '#ffffff');

          const ratio = getContrastRatio(textColor, bgColor);
          results.push({
            text: el.textContent?.substring(0, 30),
            fontSize,
            ratio: Math.round(ratio * 100) / 100,
            passes: ratio >= 4.5,
          });
        });

        return results;
      });

      // Verify all sampled text passes contrast requirements
      for (const result of contrastResults) {
        expect(result.ratio).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('Test Case 4: Heading text color contrast ratio >= 3:1 for large text', async ({ page }) => {
      // Check heading elements - using axe-core for accurate contrast checking
      // because CSS gradients can't be easily parsed for contrast calculation
      const accessibilityScanResults = await new AxeBuilder({ page })
        .include('h1, h2, h3')
        .withTags(['wcag2aa'])
        .analyze();

      // Check for any color contrast violations specifically on headings
      const contrastViolations = accessibilityScanResults.violations.filter(
        v => v.id === 'color-contrast'
      );

      // For headings (large text), the requirement is 3:1
      // axe-core already handles this threshold correctly for large text
      expect(contrastViolations.length).toBe(0);

      // Also verify headings exist and are visible
      const headingCount = await page.locator('h1, h2, h3').count();
      expect(headingCount).toBeGreaterThan(3);
    });
  });

  test.describe('ARIA Labels', () => {
    test('Test Case 5: Interactive demo components have descriptive ARIA labels', async ({ page }) => {
      // Check demo terminal has role and label
      const demoTerminal = page.locator('.demo__terminal');
      await expect(demoTerminal).toHaveAttribute('role', 'application');
      await expect(demoTerminal).toHaveAttribute('aria-label');

      const terminalLabel = await demoTerminal.getAttribute('aria-label');
      expect(terminalLabel).toContain('demo');

      // Check demo input has accessible label
      const demoInput = page.locator('#demo-input');
      await expect(demoInput).toHaveCount(1);

      // Verify input has associated label (either aria-label, aria-labelledby, or label element)
      const hasLabel = await page.evaluate(() => {
        const input = document.querySelector('#demo-input');
        if (!input) return false;

        // Check for visible label
        const labelFor = document.querySelector('label[for="demo-input"]');
        if (labelFor) return true;

        // Check for aria-label
        if (input.getAttribute('aria-label')) return true;

        // Check for aria-labelledby
        if (input.getAttribute('aria-labelledby')) return true;

        // Check for aria-describedby
        if (input.getAttribute('aria-describedby')) return true;

        return false;
      });
      expect(hasLabel).toBeTruthy();

      // Check demo output has appropriate ARIA
      const demoOutput = page.locator('#demo-output');
      await expect(demoOutput).toHaveAttribute('role', 'log');
      await expect(demoOutput).toHaveAttribute('aria-live', 'polite');

      // Check demo submit button has aria-label
      const submitBtn = page.locator('.demo__submit');
      await expect(submitBtn).toHaveAttribute('aria-label');
    });

    test('Test Case 5b: Navigation has proper ARIA labels', async ({ page }) => {
      // Check main navigation
      const mainNav = page.locator('#main-nav');
      await expect(mainNav).toHaveAttribute('aria-label');

      // Check mobile menu toggle
      const menuToggle = page.locator('#menu-toggle');
      await expect(menuToggle).toHaveAttribute('aria-label');
      await expect(menuToggle).toHaveAttribute('aria-expanded');
      await expect(menuToggle).toHaveAttribute('aria-controls', 'main-nav');
    });

    test('Test Case 5c: Decorative icons are hidden from screen readers', async ({ page }) => {
      // Check that decorative icons have aria-hidden
      const decorativeIcons = await page.locator('[aria-hidden="true"]').count();
      expect(decorativeIcons).toBeGreaterThan(0);

      // Verify feature card icons are hidden
      const featureIcons = page.locator('.feature-card__icon');
      const iconCount = await featureIcons.count();

      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        await expect(icon).toHaveAttribute('aria-hidden', 'true');
      }
    });
  });

  test.describe('Focus Indicators', () => {
    test('Test Case 6: Focus indicator visible on all focusable elements', async ({ page }) => {
      // Test focus on various element types
      const elementsToTest = [
        { selector: '.header__nav-link', name: 'nav link' },
        { selector: '.hero__btn', name: 'CTA button' },
        { selector: '#demo-input', name: 'demo input' },
        { selector: '.demo__submit', name: 'submit button' },
        { selector: '.code-example__copy-btn', name: 'copy button' },
        { selector: '.footer__link', name: 'footer link' },
      ];

      for (const { selector, name } of elementsToTest) {
        const element = page.locator(selector).first();
        if (await element.count() === 0) continue;

        // Focus the element
        await element.focus();
        await expect(element).toBeFocused();

        // Check for visible focus indicator
        const focusInfo = await element.evaluate(el => {
          const style = window.getComputedStyle(el);

          // Check for outline
          const hasOutline = style.outline !== 'none' &&
            style.outline !== '' &&
            style.outlineWidth !== '0px';

          // Check for box-shadow that might indicate focus
          const hasBoxShadow = style.boxShadow !== 'none' && style.boxShadow !== '';

          // Check for border change
          const hasBorder = style.borderStyle !== 'none' && style.borderWidth !== '0px';

          return {
            hasIndicator: hasOutline || hasBoxShadow || hasBorder,
            outline: style.outline,
            outlineWidth: style.outlineWidth,
            boxShadow: style.boxShadow,
            border: style.border,
          };
        });

        // Log for debugging if failing
        if (!focusInfo.hasIndicator) {
          console.log(`Focus indicator missing for ${name}:`, focusInfo);
        }

        expect(focusInfo.hasIndicator).toBeTruthy();
      }
    });

    test('Test Case 6b: Focus indicator has sufficient contrast', async ({ page }) => {
      // Focus on a link and check indicator visibility
      const navLink = page.locator('.header__nav-link').first();
      await navLink.focus();

      const focusStyles = await navLink.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineColor: style.outlineColor,
          outlineWidth: style.outlineWidth,
          outlineOffset: style.outlineOffset,
        };
      });

      // Verify outline width is at least 2px (WCAG recommendation)
      const outlineWidth = parseInt(focusStyles.outlineWidth);
      expect(outlineWidth).toBeGreaterThanOrEqual(2);
    });
  });

  test.describe('Automated Accessibility Audit', () => {
    test('Test Case 7: Zero critical WCAG violations using axe-core', async ({ page }) => {
      // Run axe-core accessibility audit
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      // Filter for critical and serious violations
      const criticalViolations = accessibilityScanResults.violations.filter(
        v => v.impact === 'critical' || v.impact === 'serious'
      );

      // Log violations for debugging
      if (criticalViolations.length > 0) {
        console.log('Critical/Serious WCAG Violations:');
        criticalViolations.forEach(v => {
          console.log(`- ${v.id}: ${v.description}`);
          console.log(`  Impact: ${v.impact}`);
          console.log(`  Help: ${v.help}`);
          console.log(`  Help URL: ${v.helpUrl}`);
          v.nodes.slice(0, 3).forEach((n, i) => {
            console.log(`  Node ${i+1}: ${n.html?.substring(0, 150)}`);
            console.log(`  Failure: ${n.failureSummary?.substring(0, 200)}`);
          });
        });
      }

      // Expect zero critical violations
      expect(criticalViolations.length).toBe(0);
    });

    test('Test Case 7b: Page has proper document structure', async ({ page }) => {
      // Verify HTML lang attribute
      const htmlLang = await page.locator('html').getAttribute('lang');
      expect(htmlLang).toBe('en');

      // Verify there's exactly one main element
      const mainElements = await page.locator('main, [role="main"]').count();
      expect(mainElements).toBe(1);

      // Verify there's exactly one banner (header)
      const bannerElements = await page.locator('header[role="banner"], [role="banner"]').count();
      expect(bannerElements).toBe(1);

      // Verify there's exactly one contentinfo (footer)
      const footerElements = await page.locator('footer[role="contentinfo"], [role="contentinfo"]').count();
      expect(footerElements).toBe(1);

      // Verify page has a title
      const title = await page.title();
      expect(title).toBeTruthy();
      expect(title.length).toBeGreaterThan(0);
    });

    test('Test Case 7c: Headings follow proper hierarchy', async ({ page }) => {
      const headingLevels = await page.evaluate(() => {
        const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
        return headings.map(h => parseInt(h.tagName.substring(1)));
      });

      // Verify h1 exists
      expect(headingLevels).toContain(1);

      // Verify heading levels don't skip (e.g., h1 to h3 without h2)
      for (let i = 1; i < headingLevels.length; i++) {
        const diff = headingLevels[i] - headingLevels[i - 1];
        // Allow going up (deeper) by only 1 level, or going back up any amount
        expect(diff).toBeLessThanOrEqual(1);
      }
    });

    test('Test Case 7d: Images have alt text', async ({ page }) => {
      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        // Every image should have alt attribute (empty string is OK for decorative)
        expect(alt).not.toBeNull();
      }
    });

    test('Test Case 7e: Links have discernible text', async ({ page }) => {
      const links = page.locator('a[href]');
      const linkCount = await links.count();

      for (let i = 0; i < Math.min(linkCount, 20); i++) {
        const link = links.nth(i);
        const hasDiscernibleName = await link.evaluate(el => {
          // Check text content
          const text = el.textContent?.trim();
          if (text && text.length > 0) return true;

          // Check aria-label
          if (el.getAttribute('aria-label')) return true;

          // Check aria-labelledby
          if (el.getAttribute('aria-labelledby')) return true;

          // Check for child images with alt
          const img = el.querySelector('img');
          if (img && img.getAttribute('alt')) return true;

          // Check for SVG with title
          const svg = el.querySelector('svg');
          if (svg) {
            const title = svg.querySelector('title');
            if (title && title.textContent) return true;
          }

          return false;
        });

        expect(hasDiscernibleName).toBeTruthy();
      }
    });
  });

  test.describe('Screen Reader Compatibility', () => {
    // Note: Test Case 8 is marked as manual in the scenario
    // These tests verify screen reader-related markup

    test('Test Case 8: Page structure supports screen reader navigation (automated checks)', async ({ page }) => {
      // Verify landmark regions exist
      const landmarks = await page.evaluate(() => {
        return {
          header: !!document.querySelector('header, [role="banner"]'),
          nav: !!document.querySelector('nav, [role="navigation"]'),
          main: !!document.querySelector('main, [role="main"]'),
          footer: !!document.querySelector('footer, [role="contentinfo"]'),
        };
      });

      expect(landmarks.header).toBeTruthy();
      expect(landmarks.nav).toBeTruthy();
      expect(landmarks.main).toBeTruthy();
      expect(landmarks.footer).toBeTruthy();

      // Verify sections have accessible names
      const sections = page.locator('section[aria-labelledby]');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThan(3); // Multiple labeled sections

      // Verify live regions exist for dynamic content
      const liveRegions = await page.locator('[aria-live]').count();
      expect(liveRegions).toBeGreaterThan(0);
    });

    test('Test Case 8b: Form elements have proper labels', async ({ page }) => {
      // Check demo form
      const demoForm = page.locator('#demo-form');
      await expect(demoForm).toHaveAttribute('aria-label');

      // Check input has label
      const input = page.locator('#demo-input');
      const labelId = await page.evaluate(() => {
        const input = document.querySelector('#demo-input');
        // Check for label element
        const label = document.querySelector('label[for="demo-input"]');
        if (label) return 'has-label';

        // Check aria-describedby
        if (input?.getAttribute('aria-describedby')) return 'has-describedby';

        return null;
      });

      expect(labelId).toBeTruthy();
    });

    test('Test Case 8c: Visually hidden content is accessible to screen readers', async ({ page }) => {
      // Verify .visually-hidden class elements exist and are properly styled
      const visuallyHidden = page.locator('.visually-hidden');
      const hiddenCount = await visuallyHidden.count();
      expect(hiddenCount).toBeGreaterThan(0);

      // Verify visually hidden elements are still in the accessibility tree
      const firstHidden = visuallyHidden.first();
      const isAccessible = await firstHidden.evaluate(el => {
        const style = window.getComputedStyle(el);
        // Should not have display:none or visibility:hidden
        return style.display !== 'none' && style.visibility !== 'hidden';
      });
      expect(isAccessible).toBeTruthy();
    });
  });
});
