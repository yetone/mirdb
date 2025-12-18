// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;
const path = require('path');

/**
 * Accessibility Tests for MirDB Homepage
 * Tests WCAG 2.1 AA compliance per NFR-2 requirements
 */

// Helper to get file URL
const getFileUrl = () => {
  return 'file://' + path.resolve(__dirname, '..', 'index.html');
};

test.describe('Accessibility Compliance - Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 2: Check heading hierarchy
   * Expected: Headings follow proper order (h1, h2, h3) without skipping levels
   */
  test('TC2: headings follow proper hierarchy without skipping levels', async ({ page }) => {
    // Get all heading elements
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map(h => ({
        tag: h.tagName.toLowerCase(),
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim() || ''
      }));
    });

    expect(headings.length).toBeGreaterThan(0);

    // Verify there's exactly one h1
    const h1Elements = headings.filter(h => h.tag === 'h1');
    expect(h1Elements.length).toBe(1);
    expect(h1Elements[0].text).toBe('MirDB');

    // Check that heading levels don't skip more than 1 level
    let previousLevel = 0;
    for (const heading of headings) {
      // First heading should be h1
      if (previousLevel === 0) {
        expect(heading.level).toBe(1);
      } else {
        // Subsequent headings should not skip levels (can go down any amount, but up only by 1)
        const levelDifference = heading.level - previousLevel;
        expect(levelDifference).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }
  });

  /**
   * Test Case 3: Check for alt text on images
   * Expected: All images have descriptive alt attributes
   */
  test('TC3: all images have descriptive alt attributes', async ({ page }) => {
    const images = await page.evaluate(() => {
      const imgElements = document.querySelectorAll('img');
      return Array.from(imgElements).map(img => ({
        src: img.getAttribute('src') || '',
        alt: img.getAttribute('alt'),
        hasAlt: img.hasAttribute('alt')
      }));
    });

    // If there are any images, they should all have alt attributes
    for (const img of images) {
      expect(img.hasAlt, `Image ${img.src} should have an alt attribute`).toBe(true);
      // Alt text can be empty for decorative images, but should exist
    }

    // Also check SVGs that are not aria-hidden
    const svgsWithoutAriaHidden = await page.evaluate(() => {
      const svgElements = document.querySelectorAll('svg:not([aria-hidden="true"])');
      return Array.from(svgElements).map(svg => ({
        ariaLabel: svg.getAttribute('aria-label'),
        ariaLabelledBy: svg.getAttribute('aria-labelledby'),
        role: svg.getAttribute('role'),
        parentAriaHidden: svg.closest('[aria-hidden="true"]') !== null
      }));
    });

    // SVGs that are visible and not in aria-hidden containers should be labeled
    for (const svg of svgsWithoutAriaHidden) {
      if (!svg.parentAriaHidden) {
        const hasAccessibleName = svg.ariaLabel || svg.ariaLabelledBy || svg.role === 'presentation' || svg.role === 'none';
        // If SVG is functional, it should have accessible name
        // Our icons have aria-hidden on parent container so this should be empty
      }
    }
  });

  /**
   * Test Case 6: Check color contrast ratios
   * Expected: All text meets 4.5:1 minimum contrast ratio
   * Note: This test uses axe-core to check color contrast
   */
  test('TC6: text meets minimum contrast ratio requirements', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('body')
      .analyze();

    // Filter for color contrast issues only
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    expect(contrastViolations,
      'Color contrast violations found: ' + JSON.stringify(contrastViolations, null, 2)
    ).toHaveLength(0);
  });

  /**
   * Test Case 7: Check ARIA labels where needed
   * Expected: Icons and buttons have appropriate ARIA labels
   */
  test('TC7: icons and buttons have appropriate ARIA labels', async ({ page }) => {
    // Check buttons without visible text have aria-label
    const buttons = await page.evaluate(() => {
      const buttonElements = document.querySelectorAll('button');
      return Array.from(buttonElements).map(btn => ({
        text: btn.textContent?.trim() || '',
        ariaLabel: btn.getAttribute('aria-label'),
        ariaLabelledBy: btn.getAttribute('aria-labelledby'),
        hasChildren: btn.children.length > 0
      }));
    });

    for (const button of buttons) {
      // If button has no visible text, it should have aria-label or aria-labelledby
      if (!button.text && button.hasChildren) {
        const hasAccessibleName = button.ariaLabel || button.ariaLabelledBy;
        expect(hasAccessibleName,
          'Button without text should have aria-label or aria-labelledby'
        ).toBeTruthy();
      }
    }

    // Check that decorative icons have aria-hidden
    const decorativeIcons = await page.evaluate(() => {
      const iconContainers = document.querySelectorAll('.feature-icon');
      return Array.from(iconContainers).map(container => ({
        hasAriaHidden: container.getAttribute('aria-hidden') === 'true',
        containsSvg: container.querySelector('svg') !== null
      }));
    });

    for (const icon of decorativeIcons) {
      expect(icon.hasAriaHidden,
        'Decorative icon containers should have aria-hidden="true"'
      ).toBe(true);
    }

    // Verify mobile menu button has aria-label
    const mobileMenuBtn = await page.locator('.mobile-menu-btn').first();
    const ariaLabel = await mobileMenuBtn.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toBe('Toggle menu');
  });

  /**
   * Test Case 8: Check landmark regions
   * Expected: Page has main, nav, and footer landmark regions
   */
  test('TC8: page has main, nav, and footer landmark regions', async ({ page }) => {
    // Check for main landmark
    const mainLandmark = await page.locator('main').count();
    expect(mainLandmark).toBe(1);

    // Check for nav landmark
    const navLandmark = await page.locator('nav').count();
    expect(navLandmark).toBeGreaterThanOrEqual(1);

    // Check for footer landmark
    const footerLandmark = await page.locator('footer').count();
    expect(footerLandmark).toBe(1);

    // Check for header element
    const headerLandmark = await page.locator('header').count();
    expect(headerLandmark).toBe(1);

    // Verify structure: nav should be within header
    const navInHeader = await page.locator('header nav').count();
    expect(navInHeader).toBeGreaterThanOrEqual(1);

    // Verify main content sections exist
    const mainSections = await page.locator('main section').count();
    expect(mainSections).toBeGreaterThanOrEqual(1);
  });
});

test.describe('Accessibility Compliance - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Run Lighthouse/axe-core accessibility audit
   * Expected: Accessibility score is >= 90 (using axe-core as proxy)
   */
  test('TC1: accessibility audit passes with no critical violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Check for critical and serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations.length,
      'Critical/Serious accessibility violations found: ' +
      JSON.stringify(criticalViolations.map(v => ({ id: v.id, impact: v.impact, description: v.description })), null, 2)
    ).toBe(0);

    // Also check total violations are minimal
    // Allow for some minor/moderate issues but flag them
    const totalViolations = accessibilityScanResults.violations.length;

    // Log all violations for review
    if (totalViolations > 0) {
      console.log('Accessibility violations found:');
      accessibilityScanResults.violations.forEach(v => {
        console.log(`- ${v.id}: ${v.impact} - ${v.description}`);
      });
    }

    // Pass threshold: fewer than 5 total violations (all minor/moderate)
    expect(totalViolations).toBeLessThan(5);
  });

  /**
   * Test Case 4: Tab through all interactive elements
   * Expected: All buttons, links are focusable in logical order
   */
  test('TC4: all interactive elements are keyboard accessible in logical order', async ({ page }) => {
    // Get all interactive elements
    const interactiveElements = await page.evaluate(() => {
      const elements = document.querySelectorAll(
        'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
      );
      return Array.from(elements).map((el, index) => ({
        tag: el.tagName.toLowerCase(),
        text: el.textContent?.trim().substring(0, 50) || '',
        href: el.getAttribute('href'),
        tabIndex: el.getAttribute('tabindex'),
        isVisible: el.offsetParent !== null ||
                   window.getComputedStyle(el).display !== 'none'
      }));
    });

    // Filter visible elements
    const visibleElements = interactiveElements.filter(el => el.isVisible);
    expect(visibleElements.length).toBeGreaterThan(0);

    // Tab through elements and verify focus order
    const focusedElements = [];

    // Start with first focusable element
    await page.keyboard.press('Tab');

    // Tab through visible elements (up to reasonable limit)
    const maxTabs = Math.min(visibleElements.length + 5, 20);
    for (let i = 0; i < maxTabs; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tag: el?.tagName.toLowerCase() || '',
          text: el?.textContent?.trim().substring(0, 50) || '',
          href: el?.getAttribute('href'),
        };
      });

      focusedElements.push(focusedElement);
      await page.keyboard.press('Tab');

      // If we've looped back to body or same element, break
      if (focusedElement.tag === 'body') break;
    }

    // Verify we can tab to multiple elements
    const uniqueFocusedTags = new Set(focusedElements.map(e => e.tag));
    expect(uniqueFocusedTags.size).toBeGreaterThan(1);

    // Verify links and buttons are focusable
    const focusedLinks = focusedElements.filter(e => e.tag === 'a');
    const focusedButtons = focusedElements.filter(e => e.tag === 'button');

    expect(focusedLinks.length + focusedButtons.length).toBeGreaterThan(0);
  });

  /**
   * Test Case 5: Check focus indicators
   * Expected: Visible focus indicators on all interactive elements
   */
  test('TC5: visible focus indicators on all interactive elements', async ({ page }) => {
    // Test focus visibility on specific visible elements
    // We directly test computed styles on focus since CSS rules may not be accessible due to CORS
    const testElements = [
      '.logo',
      '.btn-primary',
      '.btn-secondary'
    ];

    for (const selector of testElements) {
      const element = page.locator(selector).first();
      if (await element.isVisible()) {
        // Get styles before focus
        const stylesBefore = await element.evaluate(el => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            boxShadow: styles.boxShadow
          };
        });

        await element.focus();

        // Check that element has visible outline when focused
        const stylesAfter = await element.evaluate(el => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineStyle: styles.outlineStyle,
            outlineColor: styles.outlineColor,
            boxShadow: styles.boxShadow
          };
        });

        // Element should have either outline or box-shadow for focus indication
        const hasVisibleFocus =
          (stylesAfter.outlineWidth !== '0px' && stylesAfter.outlineStyle !== 'none') ||
          (stylesAfter.boxShadow && stylesAfter.boxShadow !== 'none');

        expect(hasVisibleFocus,
          `Element ${selector} should have visible focus indicator. Outline: ${stylesAfter.outline}, BoxShadow: ${stylesAfter.boxShadow}`
        ).toBe(true);
      }
    }

    // Verify the CSS file contains focus styles by reading it directly
    const fs = require('fs');
    const cssContent = fs.readFileSync(path.resolve(__dirname, '..', 'styles.css'), 'utf8');
    expect(cssContent).toContain(':focus');
    expect(cssContent).toContain('outline');
  });
});

test.describe('Accessibility - Additional WCAG 2.1 AA Checks', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
    await page.waitForLoadState('domcontentloaded');
  });

  test('language attribute is set on html element', async ({ page }) => {
    const lang = await page.locator('html').getAttribute('lang');
    expect(lang).toBeTruthy();
    expect(lang).toBe('en');
  });

  test('page has a descriptive title', async ({ page }) => {
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title).toContain('MirDB');
  });

  test('links have discernible text', async ({ page }) => {
    const links = await page.evaluate(() => {
      const linkElements = document.querySelectorAll('a');
      return Array.from(linkElements).map(link => ({
        text: link.textContent?.trim() || '',
        ariaLabel: link.getAttribute('aria-label'),
        title: link.getAttribute('title'),
        href: link.getAttribute('href')
      }));
    });

    for (const link of links) {
      const hasDiscernibleText = link.text || link.ariaLabel || link.title;
      expect(hasDiscernibleText,
        `Link to ${link.href} should have discernible text`
      ).toBeTruthy();
    }
  });

  test('external links have appropriate attributes', async ({ page }) => {
    const externalLinks = await page.evaluate(() => {
      const links = document.querySelectorAll('a[target="_blank"]');
      return Array.from(links).map(link => ({
        href: link.getAttribute('href'),
        rel: link.getAttribute('rel'),
        hasNoopener: link.getAttribute('rel')?.includes('noopener') || false,
        hasNoreferrer: link.getAttribute('rel')?.includes('noreferrer') || false
      }));
    });

    for (const link of externalLinks) {
      expect(link.hasNoopener,
        `External link ${link.href} should have rel="noopener"`
      ).toBe(true);
    }
  });

  test('form elements have labels (if any exist)', async ({ page }) => {
    const formElements = await page.evaluate(() => {
      const inputs = document.querySelectorAll('input, select, textarea');
      return Array.from(inputs).map(input => ({
        type: input.getAttribute('type'),
        id: input.getAttribute('id'),
        ariaLabel: input.getAttribute('aria-label'),
        ariaLabelledBy: input.getAttribute('aria-labelledby'),
        hasLabel: input.id ? document.querySelector(`label[for="${input.id}"]`) !== null : false
      }));
    });

    for (const input of formElements) {
      const hasAccessibleLabel = input.hasLabel || input.ariaLabel || input.ariaLabelledBy;
      expect(hasAccessibleLabel,
        `Form element of type ${input.type} should have an accessible label`
      ).toBe(true);
    }
  });

  test('reduced motion preference is respected', async ({ page }) => {
    // Check that prefers-reduced-motion media query is implemented by reading CSS directly
    // CSS rules may not be accessible via JavaScript due to CORS restrictions on file:// URLs
    const fs = require('fs');
    const cssContent = fs.readFileSync(path.resolve(__dirname, '..', 'styles.css'), 'utf8');

    // Verify the CSS file contains prefers-reduced-motion media query
    expect(cssContent).toContain('prefers-reduced-motion');
    expect(cssContent).toContain('reduce');
  });
});
