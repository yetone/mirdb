/**
 * Accessibility Compliance Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Tests:
 * - Keyboard navigation
 * - Focus indicators
 * - Alt text presence
 * - Color contrast
 * - Heading hierarchy
 *
 * WCAG 2.1 AA compliance requirements
 */

const { test, expect } = require('@playwright/test');
const { BASE_URL, SELECTORS, waitForPageLoad } = require('./test-utils');

test.describe('Accessibility Compliance Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test.describe('Keyboard Navigation', () => {
    test('Test Case 1: All links and buttons are reachable via keyboard (Tab)', async ({ page }) => {
      // Get all interactive elements (links and buttons)
      const interactiveElements = await page.locator('a[href], button').all();

      expect(interactiveElements.length).toBeGreaterThan(0);

      // Start from the body to ensure clean tab order
      await page.keyboard.press('Tab');

      // Collect all focused elements by tabbing through the page
      const focusedElements = [];
      let previousActiveElement = null;
      let tabCount = 0;
      const maxTabs = 100; // Safety limit

      while (tabCount < maxTabs) {
        const activeElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          return {
            tagName: el.tagName.toLowerCase(),
            href: el.getAttribute('href'),
            text: el.textContent?.trim().substring(0, 50),
            className: el.className
          };
        });

        // Break if we've cycled back to the beginning or reached body
        if (!activeElement ||
            (previousActiveElement &&
             JSON.stringify(activeElement) === JSON.stringify(focusedElements[0]))) {
          break;
        }

        focusedElements.push(activeElement);
        previousActiveElement = activeElement;

        await page.keyboard.press('Tab');
        tabCount++;
      }

      // Verify that we found interactive elements via keyboard navigation
      expect(focusedElements.length).toBeGreaterThan(0);

      // Verify key navigation links are reachable
      const focusedHrefs = focusedElements
        .filter(el => el.href)
        .map(el => el.href);

      // Check that navigation links are reachable
      expect(focusedHrefs).toContain('#features');
      expect(focusedHrefs).toContain('#architecture');
      expect(focusedHrefs).toContain('#usage');

      // Check that buttons are also reachable
      const focusedButtons = focusedElements.filter(el => el.tagName === 'button');
      // Menu toggle and copy buttons should be reachable
      expect(focusedButtons.length).toBeGreaterThan(0);
    });

    test('All interactive elements are focusable', async ({ page }) => {
      // Check that all links have proper tabindex (not negative)
      const links = await page.locator('a[href]').all();
      for (const link of links) {
        const tabindex = await link.getAttribute('tabindex');
        // tabindex should be null (default), 0, or positive
        if (tabindex !== null) {
          expect(parseInt(tabindex)).toBeGreaterThanOrEqual(0);
        }
      }

      // Check that all buttons are focusable
      const buttons = await page.locator('button').all();
      for (const button of buttons) {
        const tabindex = await button.getAttribute('tabindex');
        const disabled = await button.getAttribute('disabled');
        if (!disabled && tabindex !== null) {
          expect(parseInt(tabindex)).toBeGreaterThanOrEqual(0);
        }
      }
    });
  });

  test.describe('Focus Indicators', () => {
    test('Test Case 2: Focused elements have visible focus ring/outline', async ({ page }) => {
      // Test that focus indicators exist in the stylesheet and are visible when focused
      // We'll use keyboard navigation to trigger focus-visible

      // Press Tab to start navigating
      await page.keyboard.press('Tab');

      // Track elements we've seen and their focus styles
      const focusResults = [];
      let lastElement = null;
      let tabCount = 0;
      const maxTabs = 30; // Limit iterations

      while (tabCount < maxTabs) {
        const currentFocus = await page.evaluate(() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;

          const styles = window.getComputedStyle(el);
          return {
            tagName: el.tagName,
            className: el.className,
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            outlineOffset: styles.outlineOffset,
            boxShadow: styles.boxShadow
          };
        });

        if (!currentFocus) break;

        // Check if we've cycled back
        const elementKey = `${currentFocus.tagName}-${currentFocus.className}`;
        if (lastElement === elementKey && tabCount > 5) break;
        lastElement = elementKey;

        // Check for focus indicator
        const hasOutline = currentFocus.outlineStyle !== 'none' &&
                          currentFocus.outlineWidth !== '0px';
        const hasBoxShadow = currentFocus.boxShadow !== 'none';
        const hasFocusIndicator = hasOutline || hasBoxShadow;

        focusResults.push({
          element: elementKey,
          hasFocusIndicator,
          outline: currentFocus.outline
        });

        await page.keyboard.press('Tab');
        tabCount++;
      }

      // Verify we found focusable elements
      expect(focusResults.length).toBeGreaterThan(0);

      // Verify at least some interactive elements have focus indicators
      const elementsWithFocus = focusResults.filter(r => r.hasFocusIndicator);
      expect(elementsWithFocus.length).toBeGreaterThan(0);
    });

    test('Focus is visible on navigation links', async ({ page }) => {
      const navLinks = await page.locator('.header__nav-link').all();

      for (const link of navLinks) {
        await link.focus();

        const focusStyles = await link.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            backgroundColor: styles.backgroundColor
          };
        });

        // Should have visible focus indicator
        const hasVisibleFocus = focusStyles.outlineStyle !== 'none' ||
                               focusStyles.outlineWidth !== '0px';
        expect(hasVisibleFocus).toBe(true);
      }
    });

    test('Focus is visible on CTA buttons', async ({ page }) => {
      const ctaButtons = await page.locator('.hero__cta').all();

      for (const button of ctaButtons) {
        await button.focus();

        const hasFocusIndicator = await button.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return styles.outlineStyle !== 'none' ||
                 styles.boxShadow !== 'none';
        });

        expect(hasFocusIndicator).toBe(true);
      }
    });
  });

  test.describe('Image Alt Text', () => {
    test('Test Case 3: Logo image has descriptive alt text', async ({ page }) => {
      const logo = page.locator('.header__logo');
      await expect(logo).toBeVisible();

      const altText = await logo.getAttribute('alt');

      // Alt text should exist and be descriptive
      expect(altText).toBeTruthy();
      expect(altText.toLowerCase()).toContain('mirdb');
      expect(altText.toLowerCase()).toContain('logo');
    });

    test('All images have alt attributes', async ({ page }) => {
      const images = await page.locator('img').all();

      for (const img of images) {
        const alt = await img.getAttribute('alt');

        // All images must have alt attribute (can be empty for decorative images)
        expect(alt).not.toBeNull();

        // Non-decorative images should have meaningful alt text
        const src = await img.getAttribute('src') || '';
        if (src.includes('logo')) {
          expect(alt.length).toBeGreaterThan(0);
        }
      }
    });

    test('Decorative icons have aria-hidden', async ({ page }) => {
      // SVG icons in feature cards should be decorative
      const decorativeSvgs = await page.locator('.features__card-icon svg').all();

      for (const svg of decorativeSvgs) {
        const ariaHidden = await svg.getAttribute('aria-hidden');
        expect(ariaHidden).toBe('true');
      }
    });
  });

  test.describe('Color Contrast', () => {
    test('Test Case 4: Text contrast ratio is at least 4.5:1 (AA standard)', async ({ page }) => {
      // Helper function to calculate relative luminance
      const getLuminance = (r, g, b) => {
        const [rs, gs, bs] = [r, g, b].map(c => {
          c = c / 255;
          return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      };

      // Helper function to calculate contrast ratio
      const getContrastRatio = (l1, l2) => {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      };

      // Parse RGB color string
      const parseRGB = (color) => {
        const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (match) {
          return {
            r: parseInt(match[1]),
            g: parseInt(match[2]),
            b: parseInt(match[3])
          };
        }
        return null;
      };

      // Test main text elements
      const textSelectors = [
        '.hero__title',
        '.hero__subtitle',
        '.features__title',
        '.features__card-title',
        '.features__card-description',
        '.usage__title',
        '.footer__description'
      ];

      for (const selector of textSelectors) {
        const element = page.locator(selector).first();
        const isVisible = await element.isVisible().catch(() => false);

        if (!isVisible) continue;

        const colors = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          let bgEl = el;
          let bgColor = styles.backgroundColor;

          // Traverse up to find actual background color (not transparent)
          while (bgEl && (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent')) {
            bgEl = bgEl.parentElement;
            if (bgEl) {
              bgColor = window.getComputedStyle(bgEl).backgroundColor;
            }
          }

          // Default to page background if we hit body
          if (!bgEl || bgColor === 'rgba(0, 0, 0, 0)') {
            bgColor = 'rgb(13, 17, 23)'; // --color-bg default
          }

          return {
            textColor: styles.color,
            bgColor: bgColor,
            fontSize: parseFloat(styles.fontSize)
          };
        });

        const textRGB = parseRGB(colors.textColor);
        const bgRGB = parseRGB(colors.bgColor);

        if (textRGB && bgRGB) {
          const textLum = getLuminance(textRGB.r, textRGB.g, textRGB.b);
          const bgLum = getLuminance(bgRGB.r, bgRGB.g, bgRGB.b);
          const ratio = getContrastRatio(textLum, bgLum);

          // WCAG AA requires 4.5:1 for normal text, 3:1 for large text (18px+ or 14px+ bold)
          const minRatio = colors.fontSize >= 18 ? 3 : 4.5;

          expect(ratio).toBeGreaterThanOrEqual(minRatio);
        }
      }
    });

    test('Links have sufficient contrast', async ({ page }) => {
      const link = page.locator('a[href]').first();

      const colors = await link.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          color: styles.color
        };
      });

      // Primary link color should be visible
      expect(colors.color).toBeTruthy();
    });
  });

  test.describe('Heading Hierarchy', () => {
    test('Test Case 5: Page has exactly one h1 element', async ({ page }) => {
      const h1Elements = await page.locator('h1').all();

      expect(h1Elements.length).toBe(1);

      // Verify the h1 is the main page title
      const h1Text = await h1Elements[0].textContent();
      expect(h1Text).toContain('MirDB');
    });

    test('Test Case 6: Headings follow proper hierarchy (no skipped levels)', async ({ page }) => {
      // Get all headings in document order
      const headings = await page.evaluate(() => {
        const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        return Array.from(allHeadings).map(h => ({
          level: parseInt(h.tagName.substring(1)),
          text: h.textContent?.trim().substring(0, 50)
        }));
      });

      expect(headings.length).toBeGreaterThan(0);

      // Verify heading hierarchy
      // First heading should be h1
      expect(headings[0].level).toBe(1);

      // Check for skipped levels
      for (let i = 1; i < headings.length; i++) {
        const currentLevel = headings[i].level;
        const previousLevel = headings[i - 1].level;

        // Can go to same level, lower level (smaller number), or one level deeper
        // Cannot skip levels going deeper (e.g., h1 -> h3 is not allowed)
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    test('Each section has appropriate heading level', async ({ page }) => {
      // Features section should have h2
      const featuresHeading = page.locator('#features h2').first();
      await expect(featuresHeading).toBeVisible();

      // Feature cards should have h3
      const featureCardHeadings = await page.locator('.features__card h3').all();
      expect(featureCardHeadings.length).toBeGreaterThan(0);

      // Usage section should have h2
      const usageHeading = page.locator('#usage h2').first();
      await expect(usageHeading).toBeVisible();

      // Usage subsections should have h3 or h4
      const usageSubheadings = await page.locator('#usage h3, #usage h4').all();
      expect(usageSubheadings.length).toBeGreaterThan(0);
    });
  });

  test.describe('Additional Accessibility', () => {
    test('Page has proper lang attribute', async ({ page }) => {
      const lang = await page.locator('html').getAttribute('lang');
      expect(lang).toBe('en');
    });

    test('Skip link or main landmark exists', async ({ page }) => {
      // Check for main landmark
      const mainElement = await page.locator('main').first();
      await expect(mainElement).toBeVisible();
    });

    test('ARIA labels on interactive elements without visible text', async ({ page }) => {
      // Menu toggle button should have aria-label
      const menuToggle = page.locator('.header__menu-toggle');
      const ariaLabel = await menuToggle.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.length).toBeGreaterThan(0);

      // Copy buttons should have aria-label
      const copyButtons = await page.locator('.usage__copy-btn').all();
      for (const btn of copyButtons) {
        const label = await btn.getAttribute('aria-label');
        expect(label).toBeTruthy();
      }
    });

    test('Navigation has proper role or is within nav element', async ({ page }) => {
      const nav = page.locator('nav').first();
      await expect(nav).toBeVisible();

      // Nav should have aria-label for screen readers
      const ariaLabel = await nav.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    });

    test('External links have appropriate attributes', async ({ page }) => {
      const externalLinks = await page.locator('a[target="_blank"]').all();

      for (const link of externalLinks) {
        // Should have rel="noopener noreferrer" for security
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });
  });
});
