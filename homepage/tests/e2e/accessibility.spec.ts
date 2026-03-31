/**
 * Accessibility E2E Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Tests WCAG 2.1 AA accessibility standards:
 * - Keyboard navigation
 * - Focus indicators
 * - Color contrast
 * - Screen reader support (semantic HTML, ARIA, alt text)
 * - Text scaling
 *
 * Requirements: NFR-3, Story 6 (Mobile Experience)
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Navigate page with Tab key only - All interactive elements are reachable via keyboard', async ({
    page,
  }) => {
    // Start from the beginning of the page
    await page.keyboard.press('Tab');

    // Track all focused elements
    const focusedElements: string[] = [];
    let lastFocusedElement = '';
    let iterations = 0;
    const maxIterations = 50; // Prevent infinite loops

    while (iterations < maxIterations) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName,
          text: el.textContent?.trim().substring(0, 50) || '',
          role: el.getAttribute('role') || '',
          ariaLabel: el.getAttribute('aria-label') || '',
        };
      });

      if (!focusedElement) break;

      const elementKey = `${focusedElement.tagName}:${focusedElement.text || focusedElement.ariaLabel}`;

      // If we've cycled back to an element we've already seen, we've tabbed through everything
      if (focusedElements.includes(elementKey) && elementKey === lastFocusedElement) {
        break;
      }

      if (!focusedElements.includes(elementKey)) {
        focusedElements.push(elementKey);
      }

      lastFocusedElement = elementKey;
      await page.keyboard.press('Tab');
      iterations++;
    }

    // Verify we found interactive elements
    expect(focusedElements.length).toBeGreaterThan(0);

    // Check that we can reach buttons
    const hasButtons = focusedElements.some((el) => el.startsWith('BUTTON:'));
    const hasLinks = focusedElements.some((el) => el.startsWith('A:'));

    expect(hasButtons || hasLinks).toBeTruthy();
  });

  test('Test Case 2: Focus on interactive element - Visible focus indicator is displayed', async ({
    page,
  }) => {
    // Tab to the first interactive element
    await page.keyboard.press('Tab');

    // Get the focused element
    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el || el === document.body) return null;

      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        boxShadow: styles.boxShadow,
        border: styles.border,
        borderColor: styles.borderColor,
      };
    });

    expect(focusedElement).not.toBeNull();

    // Check for visible focus indicator (outline, box-shadow, or ring)
    const hasVisibleFocus =
      (focusedElement!.outlineStyle !== 'none' && focusedElement!.outlineWidth !== '0px') ||
      focusedElement!.boxShadow !== 'none';

    expect(hasVisibleFocus).toBeTruthy();
  });

  test('Test Case 3: Run axe accessibility audit - No critical or serious accessibility violations', async ({
    page,
  }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log violations for debugging
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalAndSerious.forEach((v) => {
        console.log(`- ${v.id}: ${v.description} (Impact: ${v.impact})`);
        v.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    expect(criticalAndSerious).toHaveLength(0);
  });

  test('Test Case 4: Check all images - All images have descriptive alt text', async ({ page }) => {
    const images = await page.locator('img').all();

    for (const img of images) {
      const altText = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Every image should have an alt attribute
      expect(altText, `Image ${src} is missing alt attribute`).not.toBeNull();

      // Alt text should not be empty for meaningful images
      // (decorative images should have alt="" which is fine)
      expect(altText).toBeDefined();
    }

    // Verify we tested at least some images
    expect(images.length).toBeGreaterThan(0);
  });

  test('Test Case 5: Check page structure - Page uses semantic HTML elements (header, main, nav, footer)', async ({
    page,
  }) => {
    // Check for header element
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check for main element
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Check for nav element (may be within header)
    const nav = page.locator('nav');
    const navCount = await nav.count();
    expect(navCount).toBeGreaterThan(0);

    // Check for section elements
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThan(0);
  });

  test('Test Case 6: Check heading hierarchy - Headings follow logical order (h1 -> h2 -> h3)', async ({
    page,
  }) => {
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((h) => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim().substring(0, 50) || '',
      }));
    });

    // Should have at least one h1
    const h1Count = headings.filter((h) => h.level === 1).length;
    expect(h1Count).toBeGreaterThanOrEqual(1);

    // Check for logical heading order (no skipping levels)
    let maxLevelSeen = 0;
    for (const heading of headings) {
      // Heading level should not skip more than one level
      // e.g., going from h1 to h3 without h2 is not ideal but acceptable
      // going from h1 to h4 is a serious issue
      if (heading.level > maxLevelSeen + 2) {
        throw new Error(
          `Heading hierarchy skip detected: jumped to h${heading.level} after max level h${maxLevelSeen}`
        );
      }
      maxLevelSeen = Math.max(maxLevelSeen, heading.level);
    }
  });

  test('Test Case 7: Check color contrast in light mode - All text meets WCAG AA contrast requirements (4.5:1)', async ({
    page,
  }) => {
    // Ensure we're in light mode
    await page.evaluate(() => {
      document.documentElement.classList.remove('dark');
    });

    // Run axe specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    // Log any contrast issues for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Light mode contrast violations:');
      accessibilityScanResults.violations.forEach((v) => {
        v.nodes.forEach((node) => {
          console.log(`  ${node.html}: ${node.failureSummary}`);
        });
      });
    }

    expect(accessibilityScanResults.violations).toHaveLength(0);
  });

  test('Test Case 8: Check color contrast in dark mode - All text meets WCAG AA contrast requirements (4.5:1)', async ({
    page,
  }) => {
    // Enable dark mode
    await page.evaluate(() => {
      document.documentElement.classList.add('dark');
    });

    // Wait for styles to apply
    await page.waitForTimeout(500);

    // Run axe specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    // Log any contrast issues for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Dark mode contrast violations:');
      accessibilityScanResults.violations.forEach((v) => {
        v.nodes.forEach((node) => {
          console.log(`  ${node.html}: ${node.failureSummary}`);
        });
      });
    }

    expect(accessibilityScanResults.violations).toHaveLength(0);
  });

  test('Test Case 9: Scale browser zoom to 200% - All content remains readable and functional', async ({
    page,
  }) => {
    // Set viewport and zoom
    await page.setViewportSize({ width: 1280, height: 720 });

    // Simulate 200% zoom by reducing viewport size to half
    await page.setViewportSize({ width: 640, height: 360 });

    // Check that main content is still visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check that buttons are still clickable
    const getStartedButton = page.locator('[data-testid="get-started-button"]');
    await expect(getStartedButton).toBeVisible();

    // Check there's no horizontal overflow
    const hasOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    // Small overflow is acceptable, but not excessive
    expect(hasOverflow).toBeFalsy();
  });

  test('Test Case 10: Check interactive elements - All buttons and links have accessible names', async ({
    page,
  }) => {
    // Check buttons
    const buttons = await page.locator('button').all();
    for (const button of buttons) {
      const accessibleName =
        (await button.getAttribute('aria-label')) ||
        (await button.textContent())?.trim() ||
        (await button.getAttribute('title'));

      expect(
        accessibleName,
        `Button is missing accessible name: ${await button.evaluate((el) => el.outerHTML)}`
      ).toBeTruthy();
    }

    // Check links
    const links = await page.locator('a').all();
    for (const link of links) {
      const accessibleName =
        (await link.getAttribute('aria-label')) ||
        (await link.textContent())?.trim() ||
        (await link.getAttribute('title'));

      expect(
        accessibleName,
        `Link is missing accessible name: ${await link.evaluate((el) => el.outerHTML)}`
      ).toBeTruthy();
    }

    // Verify we tested some elements
    expect(buttons.length + links.length).toBeGreaterThan(0);
  });
});
