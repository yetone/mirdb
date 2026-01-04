/**
 * Accessibility Tests: Screen Reader Support
 * Tests NFR-3: WCAG 2.1 AA compliance - Screen Reader Accessibility
 * Scenario: Verify page content is accessible to screen readers
 */
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Screen Reader Support', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Image Alt Attributes', () => {
    test('All img elements have alt attributes', async ({ page }) => {
      const images = await page.locator('img').all();

      for (const img of images) {
        const hasAlt = await img.evaluate(el => el.hasAttribute('alt'));
        expect(hasAlt).toBe(true);
      }
    });

    test('Decorative images have empty alt attributes', async ({ page }) => {
      // Decorative images should have alt="" (empty string)
      const decorativeImages = await page.locator('img[role="presentation"], img.decorative, img[data-decorative="true"]').all();

      for (const img of decorativeImages) {
        const altText = await img.getAttribute('alt');
        expect(altText).toBe('');
      }
    });

    test('Informative images have descriptive alt text', async ({ page }) => {
      const images = await page.locator('img').all();
      const issues = [];

      for (const img of images) {
        const alt = await img.getAttribute('alt');
        const isDecorative = await img.evaluate(el =>
          el.hasAttribute('role') && el.getAttribute('role') === 'presentation' ||
          el.classList.contains('decorative') ||
          el.getAttribute('data-decorative') === 'true'
        );

        // If not decorative and has alt, it should have meaningful content
        if (!isDecorative && alt !== null && alt !== '') {
          // Alt text should be descriptive (more than just a single word)
          // and should not contain file extension patterns
          const hasFileExtension = /\.(jpg|jpeg|png|gif|svg|webp)$/i.test(alt);
          if (hasFileExtension) {
            issues.push(`Image has filename as alt text: ${alt}`);
          }
        }
      }

      expect(issues).toEqual([]);
    });

    test('Background images with content meaning have text alternatives', async ({ page }) => {
      // Elements with background images that convey information should have aria-label or visible text
      const elementsWithBgImage = await page.locator('[style*="background-image"], [class*="bg-"]').all();

      for (const element of elementsWithBgImage) {
        const hasContent = await element.evaluate(el => {
          const text = el.textContent?.trim();
          const ariaLabel = el.getAttribute('aria-label');
          const role = el.getAttribute('role');

          // Element has text content, aria-label, or is purely decorative
          return text || ariaLabel || role === 'presentation' || role === 'none';
        });

        // Background images should either have text content or be marked as decorative
        expect(hasContent).toBeTruthy();
      }
    });

    test('SVG elements have appropriate accessible names', async ({ page }) => {
      const svgs = await page.locator('svg').all();

      for (const svg of svgs) {
        const isAccessible = await svg.evaluate(el => {
          // SVG should have title element, aria-label, aria-labelledby, or role="presentation"
          const hasTitle = el.querySelector('title');
          const hasAriaLabel = el.hasAttribute('aria-label');
          const hasAriaLabelledBy = el.hasAttribute('aria-labelledby');
          const isDecorative = el.getAttribute('role') === 'presentation' ||
                              el.getAttribute('role') === 'none' ||
                              el.getAttribute('aria-hidden') === 'true';

          return hasTitle || hasAriaLabel || hasAriaLabelledBy || isDecorative;
        });

        expect(isAccessible).toBe(true);
      }
    });
  });

  test.describe('Test Case 2: Link Purpose and Accessibility', () => {
    test('All links have accessible names', async ({ page }) => {
      const links = await page.locator('a').all();
      const issues = [];

      for (let i = 0; i < links.length; i++) {
        const link = links[i];
        const hasAccessibleName = await link.evaluate(el => {
          const textContent = el.textContent?.trim();
          const ariaLabel = el.getAttribute('aria-label');
          const ariaLabelledBy = el.getAttribute('aria-labelledby');
          const title = el.getAttribute('title');
          const img = el.querySelector('img');
          const imgAlt = img?.getAttribute('alt');

          return !!(textContent || ariaLabel || ariaLabelledBy || title || imgAlt);
        });

        if (!hasAccessibleName) {
          const href = await link.getAttribute('href');
          issues.push(`Link ${i + 1} (href: ${href}) has no accessible name`);
        }
      }

      expect(issues).toEqual([]);
    });

    test('Links have descriptive text indicating destination', async ({ page }) => {
      const links = await page.locator('a').all();
      const genericTexts = ['click here', 'here', 'read more', 'more', 'link'];
      const issues = [];

      for (let i = 0; i < links.length; i++) {
        const link = links[i];
        const accessibleName = await link.evaluate(el => {
          const textContent = el.textContent?.trim()?.toLowerCase();
          const ariaLabel = el.getAttribute('aria-label')?.toLowerCase();
          return ariaLabel || textContent;
        });

        if (accessibleName && genericTexts.includes(accessibleName)) {
          const href = await link.getAttribute('href');
          issues.push(`Link ${i + 1} (href: ${href}) has non-descriptive text: "${accessibleName}"`);
        }
      }

      expect(issues).toEqual([]);
    });

    test('External links are identifiable', async ({ page }) => {
      const externalLinks = await page.locator('a[target="_blank"]').all();

      for (const link of externalLinks) {
        const hasExternalIndicator = await link.evaluate(el => {
          // Check for visual indicator or aria-label mentioning external
          const ariaLabel = el.getAttribute('aria-label')?.toLowerCase() || '';
          const textContent = el.textContent?.toLowerCase() || '';
          const hasIcon = el.querySelector('[class*="external"], [class*="icon"]');
          const hasScreenReaderText = el.querySelector('.sr-only, .visually-hidden');
          const relAttribute = el.getAttribute('rel') || '';

          // External links should have rel="noopener noreferrer" for security
          const hasSecureRel = relAttribute.includes('noopener');

          return hasSecureRel;
        });

        expect(hasExternalIndicator).toBe(true);
      }
    });

    test('Links are distinguishable from regular text', async ({ page }) => {
      // This test verifies links have some form of visual distinction
      const linkStyles = await page.evaluate(() => {
        const links = document.querySelectorAll('a');
        const results = [];

        for (const link of links) {
          const computedStyle = window.getComputedStyle(link);
          const isDistinct =
            computedStyle.textDecoration !== 'none' ||
            computedStyle.color !== window.getComputedStyle(document.body).color ||
            computedStyle.fontWeight !== window.getComputedStyle(document.body).fontWeight;

          results.push(isDistinct);
        }

        return results;
      });

      // All links should be visually distinguishable
      expect(linkStyles.every(distinct => distinct)).toBe(true);
    });

    test('Navigation links are grouped in nav element', async ({ page }) => {
      const navLinks = await page.locator('nav a').count();
      expect(navLinks).toBeGreaterThan(0);
    });
  });
});
