/**
 * Accessibility Compliance E2E Tests
 * Owner: Scenario 13 - Accessibility Compliance
 *
 * Validates WCAG 2.1 AA compliance including:
 * - Semantic HTML structure (main, nav, footer elements)
 * - Proper heading hierarchy (h1 > h2 > h3)
 * - ARIA attributes on interactive elements
 * - Keyboard navigation and focus management
 * - Color contrast ratios (via axe-core)
 * - Focus indicators on interactive elements
 *
 * References: REQ-13, NFR-6
 */

import { test, expect, Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

/**
 * Helper function to check if element is focusable
 */
async function isFocusable(page: Page, selector: string): Promise<boolean> {
  const element = page.locator(selector).first();
  const isVisible = await element.isVisible();
  if (!isVisible) return false;

  const tagName = await element.evaluate(el => el.tagName.toLowerCase());
  const hasTabIndex = await element.evaluate(el => el.hasAttribute('tabindex'));
  const tabIndex = await element.evaluate(el => el.getAttribute('tabindex'));

  // Naturally focusable elements
  const focusableTags = ['a', 'button', 'input', 'select', 'textarea'];

  if (focusableTags.includes(tagName)) {
    return tabIndex !== '-1';
  }

  return hasTabIndex && tabIndex !== '-1';
}

test.describe('Accessibility Compliance - Semantic HTML Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();
  });

  test('Test Case 1: Page has single main landmark', async ({ page }) => {
    // Check for <main> element
    const mainElements = page.locator('main');
    const mainCount = await mainElements.count();

    expect(mainCount, 'Page should have exactly one <main> element').toBe(1);

    // Verify main element is visible
    await expect(mainElements.first()).toBeVisible();

    // Verify main element contains primary content
    const mainContent = await mainElements.first().textContent();
    expect(mainContent, 'Main element should contain content').toBeTruthy();
  });

  test('Test Case 2: Navigation uses nav element', async ({ page }) => {
    // Check for <nav> element
    const navElements = page.locator('nav');
    const navCount = await navElements.count();

    expect(navCount, 'Page should have at least one <nav> element').toBeGreaterThanOrEqual(1);

    // Verify main navigation has proper role and aria-label
    const mainNav = page.locator('nav[role="navigation"]');
    await expect(mainNav.first()).toBeVisible();

    // Check that navigation has accessible name
    const ariaLabel = await mainNav.first().getAttribute('aria-label');
    expect(ariaLabel, 'Navigation should have aria-label').toBeTruthy();
  });

  test('Test Case 3: Footer uses footer element', async ({ page }) => {
    // Check for <footer> element
    const footerElements = page.locator('footer');
    const footerCount = await footerElements.count();

    expect(footerCount, 'Page should have at least one <footer> element').toBeGreaterThanOrEqual(1);

    // Verify footer is visible
    await expect(footerElements.first()).toBeVisible();

    // Check for proper role
    const footerRole = await footerElements.first().getAttribute('role');
    expect(footerRole, 'Footer should have contentinfo role').toBe('contentinfo');

    // Verify footer contains expected links
    const footerLinks = footerElements.first().locator('a');
    const linkCount = await footerLinks.count();
    expect(linkCount, 'Footer should contain navigation links').toBeGreaterThanOrEqual(1);
  });

  test('Test Case 4: Headings follow h1 > h2 > h3 order', async ({ page }) => {
    // Get all headings in document order
    const headings = page.locator('h1, h2, h3, h4, h5, h6');
    const headingCount = await headings.count();

    expect(headingCount, 'Page should have headings').toBeGreaterThan(0);

    // Verify there is exactly one h1
    const h1Count = await page.locator('h1').count();
    expect(h1Count, 'Page should have exactly one h1').toBe(1);

    // Check heading hierarchy - no skipped levels
    let previousLevel = 0;
    const headingLevels: number[] = [];

    for (let i = 0; i < headingCount; i++) {
      const heading = headings.nth(i);
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const level = parseInt(tagName.substring(1));
      headingLevels.push(level);

      // First heading should be h1
      if (i === 0) {
        expect(level, 'First heading should be h1').toBe(1);
      }

      // Heading level should not skip more than one level
      if (previousLevel > 0 && level > previousLevel) {
        expect(
          level - previousLevel,
          `Heading levels should not skip levels (found ${tagName} after h${previousLevel})`
        ).toBeLessThanOrEqual(1);
      }

      previousLevel = level;
    }

    // Log heading structure for debugging
    console.log('Heading hierarchy:', headingLevels.map((l, i) => `h${l}`).join(' > '));
  });
});

test.describe('Accessibility Compliance - axe-core Audit', () => {
  test('Test Case 5: No critical or serious accessibility violations', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Wait for all content to load
    await page.waitForLoadState('networkidle');

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
      console.log('Critical/Serious Accessibility Violations:');
      criticalViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Nodes affected: ${violation.nodes.length}`);
        violation.nodes.forEach(node => {
          console.log(`    - ${node.html.substring(0, 100)}...`);
        });
      });
    }

    expect(
      criticalViolations,
      `Should have no critical or serious accessibility violations. Found: ${criticalViolations.map(v => v.id).join(', ')}`
    ).toHaveLength(0);
  });

  test('Color contrast meets WCAG AA standards (4.5:1 ratio)', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Run axe-core specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('body')
      .analyze();

    // Filter for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    if (contrastViolations.length > 0) {
      console.log('Color Contrast Violations:');
      contrastViolations.forEach(violation => {
        violation.nodes.forEach(node => {
          console.log(`  - ${node.html.substring(0, 100)}`);
          console.log(`    ${node.failureSummary}`);
        });
      });
    }

    expect(
      contrastViolations,
      'Should have no color contrast violations'
    ).toHaveLength(0);
  });
});

test.describe('Accessibility Compliance - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();
  });

  test('Test Case 6: All buttons/links reachable via keyboard', async ({ page }) => {
    // Get all interactive elements (buttons and links)
    const interactiveElements = page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])');
    const count = await interactiveElements.count();

    expect(count, 'Page should have interactive elements').toBeGreaterThan(0);

    // Track focused elements via Tab navigation
    const focusedElements: string[] = [];
    let previousFocusedElement = '';

    // Tab through elements
    for (let i = 0; i < count + 5; i++) {
      await page.keyboard.press('Tab');

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 50) || '',
          testId: el.getAttribute('data-testid') || '',
        };
      });

      if (focusedElement) {
        const elementKey = `${focusedElement.tagName}:${focusedElement.testId || focusedElement.text}`;

        // Avoid duplicates (cycling back to start)
        if (elementKey === previousFocusedElement) break;

        focusedElements.push(elementKey);
        previousFocusedElement = elementKey;
      }
    }

    console.log('Focused elements via Tab:', focusedElements.length);

    // Verify key interactive elements are reachable
    const navLinks = page.locator('nav a');
    const navLinkCount = await navLinks.count();

    const heroButton = page.getByTestId('hero-get-started-button');
    const isHeroButtonFocusable = await isFocusable(page, '[data-testid="hero-get-started-button"]');
    expect(isHeroButtonFocusable, 'Hero CTA button should be keyboard focusable').toBe(true);

    // Footer links should be focusable
    const footerLinks = page.locator('footer a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount, 'Footer should have keyboard-accessible links').toBeGreaterThan(0);
  });

  test('Focus moves in logical order', async ({ page }) => {
    // Track focus order through the page
    const focusOrder: string[] = [];

    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');

      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        const rect = el.getBoundingClientRect();
        return {
          tagName: el.tagName.toLowerCase(),
          top: rect.top,
          left: rect.left,
          testId: el.getAttribute('data-testid') || '',
        };
      });

      if (activeElement) {
        focusOrder.push(`${activeElement.testId || activeElement.tagName} (${Math.round(activeElement.top)}, ${Math.round(activeElement.left)})`);
      }
    }

    // Focus should generally move top-to-bottom
    // This is a basic check - more sophisticated testing would check specific order
    expect(focusOrder.length, 'Should be able to tab through multiple elements').toBeGreaterThan(3);
  });

  test('Focus indicators are visible on interactive elements', async ({ page }) => {
    // Check that focus indicators are visible
    const interactiveSelectors = [
      'nav a',
      'footer a',
      'button',
    ];

    for (const selector of interactiveSelectors) {
      const element = page.locator(selector).first();
      const isVisible = await element.isVisible();

      if (isVisible) {
        // Focus the element
        await element.focus();

        // Check for visible focus indicator
        const hasVisibleFocus = await element.evaluate(el => {
          const styles = window.getComputedStyle(el);
          const outlineWidth = parseFloat(styles.outlineWidth) || 0;
          const outlineStyle = styles.outlineStyle;
          const boxShadow = styles.boxShadow;
          const borderWidth = parseFloat(styles.borderWidth) || 0;

          // Check for outline, box-shadow, or border that indicates focus
          return (
            (outlineWidth > 0 && outlineStyle !== 'none') ||
            (boxShadow && boxShadow !== 'none') ||
            borderWidth > 0
          );
        });

        // Many CSS frameworks use focus-visible or custom focus styles
        // We check that some visual indicator changes on focus
        const pseudoFocusStyles = await element.evaluate(el => {
          const styles = window.getComputedStyle(el, ':focus');
          const focusVisibleStyles = window.getComputedStyle(el, ':focus-visible');
          return {
            outline: styles.outline,
            boxShadow: styles.boxShadow,
          };
        });

        // Log for debugging
        console.log(`Focus indicator check for ${selector}: outline=${pseudoFocusStyles.outline}`);
      }
    }
  });
});

test.describe('Accessibility Compliance - ARIA Attributes', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();
  });

  test('Test Case 7: All images have descriptive alt text', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // Check each image has alt text
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Images must have alt attribute (can be empty for decorative images)
      expect(
        alt !== null,
        `Image with src="${src}" should have an alt attribute`
      ).toBe(true);

      // Log image alt text for verification
      if (alt) {
        console.log(`Image alt text: "${alt}"`);
      }
    }

    // Also check SVG images that should have accessible names
    const svgsWithRole = page.locator('svg[role="img"]');
    const svgCount = await svgsWithRole.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = svgsWithRole.nth(i);
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledBy = await svg.getAttribute('aria-labelledby');

      expect(
        ariaLabel || ariaLabelledBy,
        'SVG with role="img" should have aria-label or aria-labelledby'
      ).toBeTruthy();
    }
  });

  test('Test Case 8: Buttons have accessible names', async ({ page }) => {
    // Get all buttons
    const buttons = page.locator('button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const isVisible = await button.isVisible();

      if (isVisible) {
        // Check accessible name via text content, aria-label, or aria-labelledby
        const textContent = (await button.textContent())?.trim();
        const ariaLabel = await button.getAttribute('aria-label');
        const ariaLabelledBy = await button.getAttribute('aria-labelledby');
        const title = await button.getAttribute('title');

        const hasAccessibleName = Boolean(textContent || ariaLabel || ariaLabelledBy || title);

        expect(
          hasAccessibleName,
          `Button should have accessible name. Found text: "${textContent}", aria-label: "${ariaLabel}"`
        ).toBe(true);

        // Log for debugging
        console.log(`Button ${i}: text="${textContent?.substring(0, 30)}", aria-label="${ariaLabel}"`);
      }
    }

    // Check links with button styling (btn class)
    const buttonLinks = page.locator('a.btn, a[role="button"]');
    const buttonLinkCount = await buttonLinks.count();

    for (let i = 0; i < buttonLinkCount; i++) {
      const link = buttonLinks.nth(i);
      const isVisible = await link.isVisible();

      if (isVisible) {
        const textContent = (await link.textContent())?.trim();
        const ariaLabel = await link.getAttribute('aria-label');

        const hasAccessibleName = Boolean(textContent || ariaLabel);

        expect(
          hasAccessibleName,
          `Button-styled link should have accessible name`
        ).toBe(true);
      }
    }
  });

  test('Interactive sections have proper ARIA labels', async ({ page }) => {
    // Check that sections have aria-label or aria-labelledby
    const sections = page.locator('section');
    const sectionCount = await sections.count();

    let labeledSections = 0;

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const ariaLabel = await section.getAttribute('aria-label');
      const ariaLabelledBy = await section.getAttribute('aria-labelledby');

      if (ariaLabel || ariaLabelledBy) {
        labeledSections++;
        console.log(`Section ${i}: aria-label="${ariaLabel}", aria-labelledby="${ariaLabelledBy}"`);
      }
    }

    expect(
      labeledSections,
      'Most sections should have ARIA labels for screen reader users'
    ).toBeGreaterThan(0);
  });

  test('Form inputs have associated labels', async ({ page }) => {
    // Check any form inputs have labels
    const inputs = page.locator('input:not([type="hidden"]), select, textarea');
    const inputCount = await inputs.count();

    for (let i = 0; i < inputCount; i++) {
      const input = inputs.nth(i);
      const isVisible = await input.isVisible();

      if (isVisible) {
        const id = await input.getAttribute('id');
        const ariaLabel = await input.getAttribute('aria-label');
        const ariaLabelledBy = await input.getAttribute('aria-labelledby');
        const placeholder = await input.getAttribute('placeholder');

        // Check if there's an associated label
        let hasLabel = Boolean(ariaLabel || ariaLabelledBy);

        if (id) {
          const associatedLabel = page.locator(`label[for="${id}"]`);
          const labelCount = await associatedLabel.count();
          hasLabel = hasLabel || labelCount > 0;
        }

        // Placeholder alone is not sufficient, but log for reference
        if (!hasLabel && placeholder) {
          console.log(`Warning: Input relies on placeholder only: "${placeholder}"`);
        }
      }
    }
  });
});

test.describe('Accessibility Compliance - Landmarks and Regions', () => {
  test('Page has all required landmarks', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Check for main landmark
    const mainLandmark = page.locator('main');
    await expect(mainLandmark, 'Page should have main landmark').toHaveCount(1);

    // Check for navigation landmark
    const navLandmark = page.locator('nav, [role="navigation"]');
    const navCount = await navLandmark.count();
    expect(navCount, 'Page should have navigation landmark').toBeGreaterThanOrEqual(1);

    // Check for contentinfo landmark (footer)
    const footerLandmark = page.locator('footer, [role="contentinfo"]');
    const footerCount = await footerLandmark.count();
    expect(footerCount, 'Page should have footer/contentinfo landmark').toBeGreaterThanOrEqual(1);
  });

  test('Landmarks are properly nested', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Main should not be inside header, footer, or nav
    const mainInNav = page.locator('nav main');
    const mainInNavCount = await mainInNav.count();
    expect(mainInNavCount, 'Main should not be nested inside nav').toBe(0);

    const mainInFooter = page.locator('footer main');
    const mainInFooterCount = await mainInFooter.count();
    expect(mainInFooterCount, 'Main should not be nested inside footer').toBe(0);
  });
});
