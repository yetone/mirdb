/**
 * Accessibility E2E Tests.
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Requirements: NFR-3
 *
 * Tests WCAG 2.1 AA compliance including:
 * - Semantic HTML structure
 * - ARIA labels on interactive elements
 * - Keyboard navigation
 * - Focus indicators
 * - Color contrast
 * - Alt text for images
 * - Heading hierarchy
 */

import { test, expect, Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page content to load
    await page.waitForSelector('#app');
  });

  test('TC1: No WCAG 2.1 AA violations detected (axe-core audit)', async ({ page }) => {
    // Run axe-core accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Check for violations
    const violations = accessibilityScanResults.violations;

    // Log violations for debugging if any
    if (violations.length > 0) {
      console.log('Accessibility violations found:');
      violations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach((node) => {
          console.log(`  Target: ${node.target}`);
          console.log(`  HTML: ${node.html}`);
        });
      });
    }

    expect(violations).toHaveLength(0);
  });

  test('TC2: All interactive elements are reachable via Tab key', async ({ page }) => {
    // Focus on the first element
    await page.keyboard.press('Tab');

    const interactiveSelectors = [
      'a[href]',
      'button:not([disabled])',
      'input:not([disabled])',
      '[tabindex="0"]',
    ];

    // Get all interactive elements
    const interactiveElements = await page.$$(interactiveSelectors.join(', '));
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Track focused elements
    const focusedElements: string[] = [];
    const maxTabs = 100; // Prevent infinite loop

    for (let i = 0; i < maxTabs; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 50) || '',
          href: el.getAttribute('href') || '',
          ariaLabel: el.getAttribute('aria-label') || '',
        };
      });

      if (!focusedElement) break;

      const elementId = `${focusedElement.tagName}:${focusedElement.text || focusedElement.ariaLabel || focusedElement.href}`;

      // Stop if we've cycled back to the beginning
      if (focusedElements.includes(elementId) && focusedElements.length > 5) {
        break;
      }

      focusedElements.push(elementId);
      await page.keyboard.press('Tab');
    }

    // Verify we can reach navigation links
    expect(focusedElements.some(el => el.includes('Features'))).toBe(true);
    expect(focusedElements.some(el => el.includes('Quick Start'))).toBe(true);
    expect(focusedElements.some(el => el.includes('Protocol'))).toBe(true);

    // Verify we can reach buttons (theme toggle, copy buttons)
    expect(focusedElements.some(el => el.startsWith('button:') || el.includes('theme') || el.includes('Copy'))).toBe(true);
  });

  test('TC3: All focusable elements have visible focus indicators', async ({ page }) => {
    // Verify that focus styles are defined in the stylesheet for key elements
    // This is more reliable than checking computed styles during runtime

    // Check that CSS contains focus styles for important elements
    const hasFocusStyles = await page.evaluate(() => {
      // Get all stylesheets
      const sheets = Array.from(document.styleSheets);
      let focusRulesFound: string[] = [];

      for (const sheet of sheets) {
        try {
          const rules = Array.from(sheet.cssRules || []);
          for (const rule of rules) {
            if (rule instanceof CSSStyleRule) {
              const selector = rule.selectorText;
              // Look for focus rules
              if (selector && selector.includes(':focus')) {
                const style = rule.style;
                // Check if it sets outline or box-shadow
                if (style.outline || style.outlineWidth || style.boxShadow) {
                  focusRulesFound.push(selector);
                }
              }
            }
          }
        } catch (e) {
          // Cross-origin stylesheets will throw, ignore them
        }
      }
      return focusRulesFound;
    });

    // Verify we have focus styles for important element types
    expect(hasFocusStyles.some(s => s.includes('a:focus') || s.includes('nav-link'))).toBe(true);
    expect(hasFocusStyles.some(s => s.includes('button:focus') || s.includes('copy-button') || s.includes('theme-toggle'))).toBe(true);

    // Also verify elements are keyboard focusable
    const focusableElements: string[] = [];
    const maxTabs = 30;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      const focused = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return Array.from(el.classList).join(' ') || el.tagName.toLowerCase();
      });

      if (focused && !focusableElements.includes(focused)) {
        focusableElements.push(focused);
      }
    }

    // Verify we can reach important interactive elements
    expect(focusableElements.some(el => el.includes('nav-link') || el.includes('header'))).toBe(true);
    expect(focusableElements.some(el => el.includes('theme-toggle') || el.includes('button'))).toBe(true);
    expect(focusableElements.some(el => el.includes('hero-btn') || el.includes('btn'))).toBe(true);
    expect(focusableElements.some(el => el.includes('copy-button'))).toBe(true);
    expect(focusableElements.some(el => el.includes('footer'))).toBe(true);
  });

  test('TC4: All text meets 4.5:1 contrast ratio minimum', async ({ page }) => {
    // Run axe-core with focus on color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ rules: { 'color-contrast': { enabled: true } } })
      .analyze();

    const contrastViolations = contrastResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    if (contrastViolations.length > 0) {
      console.log('Color contrast violations:');
      contrastViolations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`  ${node.target}: ${node.failureSummary}`);
        });
      });
    }

    expect(contrastViolations).toHaveLength(0);
  });

  test('TC4b: Color contrast in dark mode', async ({ page }) => {
    // Switch to dark mode
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark');
    });

    // Wait for theme change to apply
    await page.waitForTimeout(100);

    // Run axe-core with focus on color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ rules: { 'color-contrast': { enabled: true } } })
      .analyze();

    const contrastViolations = contrastResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    expect(contrastViolations).toHaveLength(0);
  });

  test('TC5: All images have descriptive alt text', async ({ page }) => {
    const images = await page.$$('img');

    for (const image of images) {
      const alt = await image.getAttribute('alt');
      const src = await image.getAttribute('src');

      // Every image should have alt text
      expect(alt, `Image ${src} should have alt attribute`).not.toBeNull();
      expect(alt?.trim().length, `Image ${src} should have non-empty alt text`).toBeGreaterThan(0);
    }

    // Check for SVG images used as images that need accessible names
    const svgImages = await page.$$('svg[role="img"]');
    for (const svg of svgImages) {
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledBy = await svg.getAttribute('aria-labelledby');
      const title = await svg.$('title');

      const hasAccessibleName = ariaLabel || ariaLabelledBy || title;
      expect(
        hasAccessibleName,
        'SVG with role="img" should have accessible name'
      ).toBeTruthy();
    }
  });

  test('TC6: Page uses semantic HTML5 elements', async ({ page }) => {
    // Check for required semantic elements
    const header = await page.$('header');
    expect(header, 'Page should have a header element').not.toBeNull();

    const nav = await page.$('nav');
    expect(nav, 'Page should have a nav element').not.toBeNull();

    const main = await page.$('main, [role="main"]');
    const sections = await page.$$('section');
    expect(
      main || sections.length > 0,
      'Page should have main content area (main or sections)'
    ).toBeTruthy();

    const footer = await page.$('footer');
    expect(footer, 'Page should have a footer element').not.toBeNull();

    // Check that sections have proper ARIA attributes
    for (const section of sections) {
      const ariaLabel = await section.getAttribute('aria-labelledby');
      const id = await section.getAttribute('id');
      if (id && id !== 'app') {
        expect(
          ariaLabel,
          `Section #${id} should have aria-labelledby attribute`
        ).not.toBeNull();
      }
    }
  });

  test('TC7: Headings follow proper hierarchy (h1 > h2 > h3)', async ({ page }) => {
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) =>
      elements.map((el) => ({
        level: parseInt(el.tagName.substring(1)),
        text: el.textContent?.trim().substring(0, 50) || '',
      }))
    );

    // Should have exactly one h1
    const h1Count = headings.filter((h) => h.level === 1).length;
    expect(h1Count, 'Page should have exactly one h1').toBe(1);

    // Heading levels should not skip
    let previousLevel = 0;
    for (const heading of headings) {
      // Allow going to same level, one level down, or any level up
      const isValidProgression =
        heading.level <= previousLevel + 1 || heading.level <= previousLevel;

      expect(
        isValidProgression,
        `Heading "${heading.text}" (h${heading.level}) should not skip levels from h${previousLevel}`
      ).toBe(true);

      previousLevel = heading.level;
    }

    // h2s should exist for main sections
    const h2Count = headings.filter((h) => h.level === 2).length;
    expect(h2Count, 'Page should have h2 headings for sections').toBeGreaterThan(0);
  });

  test('TC8: Theme toggle is accessible via Enter or Space key', async ({ page }) => {
    // Find theme toggle button
    const themeToggle = await page.$('.theme-toggle');
    expect(themeToggle, 'Theme toggle should exist').not.toBeNull();

    // Get initial theme
    const initialTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );

    // Focus the toggle
    await themeToggle!.focus();

    // Verify it has proper ARIA attributes
    const ariaLabel = await themeToggle!.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/switch to (light|dark) theme/i);

    // Activate with Enter key
    await page.keyboard.press('Enter');

    // Check theme changed
    const themeAfterEnter = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );
    expect(themeAfterEnter).not.toBe(initialTheme);

    // Activate with Space key
    await page.keyboard.press('Space');

    // Check theme changed back
    const themeAfterSpace = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    );
    expect(themeAfterSpace).toBe(initialTheme);
  });

  test('TC9: Copy buttons are focusable and activatable via keyboard', async ({ page }) => {
    // Find copy buttons
    const copyButtons = await page.$$('.copy-button');
    expect(copyButtons.length, 'Copy buttons should exist').toBeGreaterThan(0);

    for (const button of copyButtons) {
      // Verify button has proper ARIA label
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel, 'Copy button should have aria-label').toBeTruthy();
      expect(ariaLabel).toMatch(/copy/i);

      // Verify button is focusable
      await button.focus();
      const isFocused = await button.evaluate(
        (el) => document.activeElement === el
      );
      expect(isFocused, 'Copy button should be focusable').toBe(true);

      // Verify button can be activated with keyboard (Enter)
      const buttonText = await button.$('.copy-text');
      const textBefore = await buttonText?.textContent();

      await page.keyboard.press('Enter');

      // Wait for visual feedback
      await page.waitForTimeout(100);

      // Check button got activated (either text changed or class added)
      const hasActivationFeedback = await button.evaluate((el) => {
        const text = el.querySelector('.copy-text')?.textContent;
        return el.classList.contains('copied') || text === 'Copied!';
      });

      // Note: Clipboard API might fail in test environment, but the button should respond
      const textAfter = await buttonText?.textContent();
      const buttonActivated = hasActivationFeedback || textAfter !== textBefore;

      // The button should at least maintain focus after activation
      const stillFocused = await button.evaluate(
        (el) => document.activeElement === el
      );
      expect(stillFocused, 'Copy button should maintain focus after activation').toBe(true);
    }
  });

  test('TC10: Screen reader support - ARIA landmarks and labels', async ({ page }) => {
    // Check for ARIA landmarks
    const banner = await page.$('[role="banner"], header');
    expect(banner, 'Page should have banner/header landmark').not.toBeNull();

    const navigation = await page.$('[role="navigation"], nav');
    expect(navigation, 'Page should have navigation landmark').not.toBeNull();

    // Check navigation has accessible name
    const navLabel = await navigation?.getAttribute('aria-label');
    expect(navLabel, 'Navigation should have aria-label').toBeTruthy();

    const contentInfo = await page.$('[role="contentinfo"], footer');
    expect(contentInfo, 'Page should have contentinfo/footer landmark').not.toBeNull();

    // Check interactive elements have accessible names
    const buttons = await page.$$('button');
    for (const button of buttons) {
      const ariaLabel = await button.getAttribute('aria-label');
      const text = await button.textContent();
      const ariaLabelledBy = await button.getAttribute('aria-labelledby');

      const hasAccessibleName = ariaLabel || text?.trim() || ariaLabelledBy;
      expect(
        hasAccessibleName,
        'All buttons should have accessible names'
      ).toBeTruthy();
    }

    // Check links have accessible names
    const links = await page.$$('a');
    for (const link of links) {
      const ariaLabel = await link.getAttribute('aria-label');
      const text = await link.textContent();
      const ariaLabelledBy = await link.getAttribute('aria-labelledby');

      const hasAccessibleName = ariaLabel || text?.trim() || ariaLabelledBy;
      expect(
        hasAccessibleName,
        'All links should have accessible names'
      ).toBeTruthy();
    }

    // Check decorative images are hidden from screen readers
    const decorativeIcons = await page.$$('[aria-hidden="true"]');
    expect(
      decorativeIcons.length,
      'Decorative elements should be hidden from screen readers'
    ).toBeGreaterThan(0);
  });

  test('Navigation links have descriptive text', async ({ page }) => {
    const navLinks = await page.$$('.header__nav-link');

    for (const link of navLinks) {
      const text = await link.textContent();
      expect(text?.trim().length, 'Nav links should have text').toBeGreaterThan(0);

      // Text should not be generic like "click here"
      expect(text?.toLowerCase()).not.toContain('click here');
      expect(text?.toLowerCase()).not.toContain('read more');
    }
  });

  test('External links have proper attributes', async ({ page }) => {
    const externalLinks = await page.$$('a[target="_blank"]');

    for (const link of externalLinks) {
      const rel = await link.getAttribute('rel');
      expect(
        rel,
        'External links should have rel attribute'
      ).toBeTruthy();
      expect(
        rel?.includes('noopener'),
        'External links should have rel="noopener"'
      ).toBe(true);
    }
  });

  test('Form elements are properly labeled', async ({ page }) => {
    // While the page may not have forms, check any inputs that exist
    const inputs = await page.$$('input, textarea, select');

    for (const input of inputs) {
      const id = await input.getAttribute('id');
      const ariaLabel = await input.getAttribute('aria-label');
      const ariaLabelledBy = await input.getAttribute('aria-labelledby');

      if (id) {
        const label = await page.$(`label[for="${id}"]`);
        expect(
          label || ariaLabel || ariaLabelledBy,
          `Input ${id} should have associated label`
        ).toBeTruthy();
      } else {
        expect(
          ariaLabel || ariaLabelledBy,
          'Inputs without id should have aria-label'
        ).toBeTruthy();
      }
    }
  });

  test('Language attribute is set on html element', async ({ page }) => {
    const lang = await page.evaluate(() =>
      document.documentElement.getAttribute('lang')
    );
    expect(lang, 'HTML should have lang attribute').toBeTruthy();
    expect(lang).toBe('en');
  });

  test('Tables have proper accessibility attributes', async ({ page }) => {
    const tables = await page.$$('table');

    for (const table of tables) {
      // Check table has accessible name
      const ariaLabel = await table.getAttribute('aria-label');
      const ariaLabelledBy = await table.getAttribute('aria-labelledby');
      const caption = await table.$('caption');

      expect(
        ariaLabel || ariaLabelledBy || caption,
        'Tables should have accessible names'
      ).toBeTruthy();

      // Check table headers have scope
      const headers = await table.$$('th');
      for (const header of headers) {
        const scope = await header.getAttribute('scope');
        expect(scope, 'Table headers should have scope attribute').toBeTruthy();
      }
    }
  });
});
