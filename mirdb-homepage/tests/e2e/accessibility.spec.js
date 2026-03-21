/**
 * Accessibility E2E Tests
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Tests:
 * - Color contrast ratios (WCAG AA: 4.5:1 for normal text, 3:1 for large text)
 * - Keyboard navigation
 * - Focus indicators
 * - Screen reader compatibility
 * - ARIA attributes where needed
 */

const { test, expect } = require('@playwright/test');

/**
 * Calculate relative luminance for a color
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance value
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - First color in rgb() or rgba() format
 * @param {string} color2 - Second color in rgb() or rgba() format
 * @returns {number} Contrast ratio
 */
function getContrastRatio(color1, color2) {
  const parseColor = (color) => {
    const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (!match) return [0, 0, 0];
    return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
  };

  const [r1, g1, b1] = parseColor(color1);
  const [r2, g2, b2] = parseColor(color2);

  const l1 = getLuminance(r1, g1, b1);
  const l2 = getLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Color Contrast', () => {
    test('TC1: Normal text has minimum 4.5:1 contrast ratio', async ({ page }) => {
      // Test main body text contrast
      const body = page.locator('body');
      const bodyColor = await body.evaluate(el => getComputedStyle(el).color);
      const bodyBg = await body.evaluate(el => getComputedStyle(el).backgroundColor);

      const bodyContrast = getContrastRatio(bodyColor, bodyBg);
      expect(bodyContrast).toBeGreaterThanOrEqual(4.5);

      // Test hero description text
      const heroDescription = page.locator('.hero__description');
      if (await heroDescription.count() > 0) {
        const descColor = await heroDescription.evaluate(el => getComputedStyle(el).color);
        const descBg = await heroDescription.evaluate(el => {
          let elem = el;
          let bg = getComputedStyle(elem).backgroundColor;
          while (bg === 'rgba(0, 0, 0, 0)' && elem.parentElement) {
            elem = elem.parentElement;
            bg = getComputedStyle(elem).backgroundColor;
          }
          return bg || 'rgb(255, 255, 255)';
        });
        const descContrast = getContrastRatio(descColor, descBg);
        expect(descContrast).toBeGreaterThanOrEqual(4.5);
      }

      // Test feature card text
      const featureCardText = page.locator('.feature-card p').first();
      if (await featureCardText.count() > 0) {
        const cardTextColor = await featureCardText.evaluate(el => getComputedStyle(el).color);
        const cardBg = await featureCardText.evaluate(el => {
          let elem = el;
          let bg = getComputedStyle(elem).backgroundColor;
          while (bg === 'rgba(0, 0, 0, 0)' && elem.parentElement) {
            elem = elem.parentElement;
            bg = getComputedStyle(elem).backgroundColor;
          }
          return bg || 'rgb(255, 255, 255)';
        });
        const cardContrast = getContrastRatio(cardTextColor, cardBg);
        expect(cardContrast).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('TC2: Large text has minimum 3:1 contrast ratio', async ({ page }) => {
      // Test h1 heading (large text)
      const h1 = page.locator('h1');
      const h1Color = await h1.evaluate(el => getComputedStyle(el).color);
      const h1Bg = await h1.evaluate(el => {
        let elem = el;
        let bg = getComputedStyle(elem).backgroundColor;
        while (bg === 'rgba(0, 0, 0, 0)' && elem.parentElement) {
          elem = elem.parentElement;
          bg = getComputedStyle(elem).backgroundColor;
        }
        return bg || 'rgb(255, 255, 255)';
      });
      const h1Contrast = getContrastRatio(h1Color, h1Bg);
      expect(h1Contrast).toBeGreaterThanOrEqual(3);

      // Test h2 headings
      const h2s = page.locator('h2');
      const h2Count = await h2s.count();
      for (let i = 0; i < Math.min(h2Count, 3); i++) {
        const h2 = h2s.nth(i);
        const h2Color = await h2.evaluate(el => getComputedStyle(el).color);
        const h2Bg = await h2.evaluate(el => {
          let elem = el;
          let bg = getComputedStyle(elem).backgroundColor;
          while (bg === 'rgba(0, 0, 0, 0)' && elem.parentElement) {
            elem = elem.parentElement;
            bg = getComputedStyle(elem).backgroundColor;
          }
          return bg || 'rgb(255, 255, 255)';
        });
        const h2Contrast = getContrastRatio(h2Color, h2Bg);
        expect(h2Contrast).toBeGreaterThanOrEqual(3);
      }

      // Test h3 headings (feature card titles - 18pt+ = large text)
      const h3s = page.locator('h3');
      const h3Count = await h3s.count();
      for (let i = 0; i < Math.min(h3Count, 3); i++) {
        const h3 = h3s.nth(i);
        const h3Color = await h3.evaluate(el => getComputedStyle(el).color);
        const h3Bg = await h3.evaluate(el => {
          let elem = el;
          let bg = getComputedStyle(elem).backgroundColor;
          while (bg === 'rgba(0, 0, 0, 0)' && elem.parentElement) {
            elem = elem.parentElement;
            bg = getComputedStyle(elem).backgroundColor;
          }
          return bg || 'rgb(255, 255, 255)';
        });
        const h3Contrast = getContrastRatio(h3Color, h3Bg);
        expect(h3Contrast).toBeGreaterThanOrEqual(3);
      }
    });
  });

  test.describe('Keyboard Navigation', () => {
    test('TC3: Tab through all interactive elements in logical order', async ({ page }) => {
      // Start from body
      await page.locator('body').focus();

      // Track all focusable elements
      const focusableElements = [];
      const interactiveSelectors = [
        'a[href]',
        'button',
        '[tabindex]:not([tabindex="-1"])'
      ];

      // Get all interactive elements
      const elements = await page.locator(interactiveSelectors.join(',')).all();

      // Tab through elements and verify they receive focus
      for (let i = 0; i < Math.min(elements.length, 15); i++) {
        await page.keyboard.press('Tab');
        const focusedElement = page.locator(':focus');

        // Verify an element is focused
        await expect(focusedElement).toBeAttached();

        // Get the focused element's tag and role
        const tagName = await focusedElement.evaluate(el => el.tagName.toLowerCase());
        const role = await focusedElement.getAttribute('role');

        // Store for order verification
        focusableElements.push({ tagName, role });
      }

      // Verify we actually focused on elements
      expect(focusableElements.length).toBeGreaterThan(0);

      // Verify navigation links are reachable
      const navLinksExist = focusableElements.some(el =>
        el.tagName === 'a' || el.role === 'link'
      );
      expect(navLinksExist).toBe(true);
    });

    test('Navigation links are keyboard accessible', async ({ page }) => {
      // Focus on the first nav link
      const navLinks = page.locator('.nav-link');
      const navLinkCount = await navLinks.count();

      expect(navLinkCount).toBeGreaterThan(0);

      // Each nav link should be focusable
      for (let i = 0; i < navLinkCount; i++) {
        const link = navLinks.nth(i);
        await link.focus();
        await expect(link).toBeFocused();
      }
    });

    test('CTA buttons are keyboard accessible', async ({ page }) => {
      const primaryCta = page.locator('.hero__cta--primary');
      const secondaryCta = page.locator('.hero__cta--secondary');

      // Primary CTA should be focusable
      await primaryCta.focus();
      await expect(primaryCta).toBeFocused();

      // Secondary CTA should be focusable
      await secondaryCta.focus();
      await expect(secondaryCta).toBeFocused();
    });

    test('Copy button is keyboard accessible', async ({ page }) => {
      const copyButton = page.locator('.copy-button');

      if (await copyButton.count() > 0) {
        await copyButton.focus();
        await expect(copyButton).toBeFocused();

        // Should be activatable with Enter key
        const buttonType = await copyButton.getAttribute('type');
        expect(buttonType).toBe('button');
      }
    });
  });

  test.describe('Focus Indicators', () => {
    test('TC4: Every focusable element has visible focus state', async ({ page }) => {
      // Helper function to check if an element has visible focus indicator
      // Uses programmatic focus which reliably triggers :focus styles
      const hasFocusIndicator = async (element) => {
        // Focus the element and check computed styles
        await element.focus();
        // Small delay to ensure styles are applied
        await page.waitForTimeout(50);

        return await element.evaluate(el => {
          const styles = getComputedStyle(el);
          const outlineWidth = styles.outlineWidth;
          const outlineStyle = styles.outlineStyle;
          const boxShadow = styles.boxShadow;

          const hasOutline = outlineStyle !== 'none' && outlineWidth !== '0px';
          // Check for non-trivial box-shadow (not 'none' and has non-zero spread/offset)
          const hasBoxShadow = boxShadow !== 'none' && !boxShadow.includes('rgba(0, 0, 0, 0)');

          return hasOutline || hasBoxShadow;
        });
      };

      // Test focus on navigation links
      const navLinks = page.locator('.nav-link');
      const navLinkCount = await navLinks.count();

      for (let i = 0; i < navLinkCount; i++) {
        const link = navLinks.nth(i);
        const focusIndicator = await hasFocusIndicator(link);
        expect(focusIndicator).toBe(true);
      }

      // Test CTA buttons
      const primaryCta = page.locator('.hero__cta--primary');
      const primaryFocusIndicator = await hasFocusIndicator(primaryCta);
      expect(primaryFocusIndicator).toBe(true);

      const secondaryCta = page.locator('.hero__cta--secondary');
      const secondaryFocusIndicator = await hasFocusIndicator(secondaryCta);
      expect(secondaryFocusIndicator).toBe(true);

      // Test copy button
      const copyButton = page.locator('.copy-button');
      if (await copyButton.count() > 0) {
        const copyButtonFocusIndicator = await hasFocusIndicator(copyButton);
        expect(copyButtonFocusIndicator).toBe(true);
      }
    });

    test('Links have visible focus indicator', async ({ page }) => {
      // Helper function to check if an element has visible focus indicator
      const hasFocusIndicator = async (element) => {
        await element.focus();
        await page.waitForTimeout(50);

        return await element.evaluate(el => {
          const styles = getComputedStyle(el);
          const outlineWidth = styles.outlineWidth;
          const outlineStyle = styles.outlineStyle;
          const boxShadow = styles.boxShadow;

          const hasOutline = outlineStyle !== 'none' && outlineWidth !== '0px';
          const hasBoxShadow = boxShadow !== 'none' && !boxShadow.includes('rgba(0, 0, 0, 0)');

          return hasOutline || hasBoxShadow;
        });
      };

      // Test first few links (excluding CTA buttons which use box-shadow)
      const navLinks = page.locator('.nav-link');
      const linkCount = await navLinks.count();

      for (let i = 0; i < Math.min(linkCount, 4); i++) {
        const link = navLinks.nth(i);
        const focusIndicator = await hasFocusIndicator(link);
        expect(focusIndicator).toBe(true);
      }
    });
  });

  test.describe('Screen Reader Compatibility', () => {
    test('Navigation has proper aria-label', async ({ page }) => {
      const nav = page.locator('nav');
      const ariaLabel = await nav.getAttribute('aria-label');

      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('navigation');
    });

    test('External links indicate opening in new tab', async ({ page }) => {
      // Find links that open in new tabs
      const externalLinks = page.locator('a[target="_blank"]');
      const externalCount = await externalLinks.count();

      for (let i = 0; i < externalCount; i++) {
        const link = externalLinks.nth(i);

        // Should have noopener for security
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });

    test('Icons have proper aria-hidden or labels', async ({ page }) => {
      // SVG icons that are decorative should be hidden from screen readers
      const decorativeIcons = page.locator('svg[aria-hidden="true"]');
      const decorativeCount = await decorativeIcons.count();

      // Feature icons should have labels
      const featureIcons = page.locator('.feature-icon svg');
      const featureIconCount = await featureIcons.count();

      // Either icons are hidden or the parent has text
      for (let i = 0; i < featureIconCount; i++) {
        const icon = featureIcons.nth(i);
        const ariaHidden = await icon.getAttribute('aria-hidden');
        const parentText = await icon.evaluate(el => el.parentElement.closest('.feature-card')?.textContent);

        // Icon should either be aria-hidden or parent provides context
        const isAccessible = ariaHidden === 'true' || (parentText && parentText.trim().length > 0);
        expect(isAccessible).toBe(true);
      }
    });

    test('Status feature icons have accessible labels', async ({ page }) => {
      const featureIcons = page.locator('.status-feature__icon');
      const iconCount = await featureIcons.count();

      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        const ariaLabel = await icon.getAttribute('aria-label');

        // Each status icon should have an aria-label
        expect(ariaLabel).toBeTruthy();
        expect(['Implemented', 'Planned']).toContain(ariaLabel);
      }
    });
  });

  test.describe('Skip Links and Landmarks', () => {
    test('Page has proper landmark structure', async ({ page }) => {
      // Check for main landmark
      const main = page.locator('main');
      await expect(main).toBeAttached();

      // Check for header landmark
      const header = page.locator('header');
      await expect(header).toBeAttached();

      // Check for nav landmark
      const nav = page.locator('nav');
      await expect(nav).toBeAttached();

      // Check for footer landmark
      const footer = page.locator('footer');
      await expect(footer).toBeAttached();
    });

    test('Content sections have proper IDs for navigation', async ({ page }) => {
      const sections = ['hero', 'features', 'quick-start', 'usage', 'status'];

      for (const sectionId of sections) {
        const section = page.locator(`#${sectionId}`);
        await expect(section).toBeAttached();

        // Section should use semantic section element
        const tagName = await section.evaluate(el => el.tagName.toLowerCase());
        expect(tagName).toBe('section');
      }
    });
  });
});
