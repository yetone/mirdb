/**
 * Accessibility E2E Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests accessibility requirements per WCAG guidelines and NFR-2:
 * - Minimum Lighthouse Accessibility score of 90
 * - All interactive elements keyboard accessible
 * - WCAG AA contrast ratios (4.5:1 for normal text, 3:1 for large text)
 * - Proper alt text for images
 * - Correct heading hierarchy
 */
import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Run accessibility audit - should score 90 or higher', async ({ page }) => {
    // Use axe-core to run accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Calculate approximate accessibility score based on violations
    // Axe-core doesn't provide a Lighthouse-style score, but we can approximate
    // by checking for violations. Zero critical/serious violations indicates high compliance
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical'
    );
    const seriousViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'serious'
    );

    // Log all violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log(
        'Accessibility violations:',
        JSON.stringify(accessibilityScanResults.violations, null, 2)
      );
    }

    // No critical violations allowed
    expect(criticalViolations).toHaveLength(0);

    // No serious violations allowed for score >= 90
    expect(seriousViolations).toHaveLength(0);

    // Total violations should be minimal (allowing minor/moderate)
    expect(accessibilityScanResults.violations.length).toBeLessThanOrEqual(3);
  });

  test('Test Case 2: Tab through all interactive elements - all should be focusable via keyboard', async ({
    page,
  }) => {
    // Start from the document body
    await page.keyboard.press('Tab');

    // Track focused elements
    const focusedElements: string[] = [];

    // Tab through elements and collect focused elements
    for (let i = 0; i < 30; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return null;
        const tagName = el.tagName.toLowerCase();
        const role = el.getAttribute('role');
        const ariaLabel = el.getAttribute('aria-label');
        const text = el.textContent?.trim().substring(0, 50);
        const testId = el.getAttribute('data-testid');
        return { tagName, role, ariaLabel, text, testId };
      });

      if (focusedElement) {
        focusedElements.push(JSON.stringify(focusedElement));
      }

      await page.keyboard.press('Tab');

      // Check if we've cycled back to the beginning
      const currentFocused = await page.evaluate(
        () => document.activeElement?.tagName
      );
      if (currentFocused === 'BODY' || i === 29) break;
    }

    // Verify that we can focus on key interactive elements
    // Check for skip link (should be first focusable)
    const skipLink = await page.locator('[data-testid="skip-link"]').count();
    expect(skipLink).toBeGreaterThan(0);

    // Check for navigation links
    const navLinks = await page.locator('nav a').count();
    expect(navLinks).toBeGreaterThan(0);

    // Check for buttons
    const buttons = await page.locator('button').count();
    expect(buttons).toBeGreaterThan(0);

    // Verify all buttons are focusable
    const allButtons = await page.locator('button').all();
    for (const button of allButtons) {
      const isVisible = await button.isVisible();
      if (isVisible) {
        await button.focus();
        const isFocused = await button.evaluate(
          (el) => document.activeElement === el
        );
        expect(isFocused).toBeTruthy();
      }
    }

    // Verify all links are focusable
    const allLinks = await page.locator('a[href]').all();
    for (const link of allLinks) {
      const isVisible = await link.isVisible();
      if (isVisible) {
        await link.focus();
        const isFocused = await link.evaluate(
          (el) => document.activeElement === el
        );
        expect(isFocused).toBeTruthy();
      }
    }
  });

  test('Test Case 3: Check skip link functionality - should be present and functional', async ({
    page,
  }) => {
    // Skip link should exist
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeAttached();

    // Skip link should become visible on focus
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();
    await expect(skipLink).toBeVisible();

    // Skip link should have proper text
    const skipLinkText = await skipLink.textContent();
    expect(skipLinkText?.toLowerCase()).toContain('skip');

    // Click skip link and verify focus moves to main content
    await skipLink.click();

    // Main content should receive focus
    const mainContent = page.locator('#main-content, main');
    const isFocused = await mainContent.evaluate((el) => {
      return (
        document.activeElement === el ||
        el.contains(document.activeElement) ||
        el.getAttribute('tabindex') === '-1'
      );
    });
    expect(isFocused).toBeTruthy();
  });

  test('Test Case 4: Verify logo image alt text - should describe MirDB branding', async ({
    page,
  }) => {
    // Check for logo with aria-label (since it's a div with text, not an img)
    const logoElement = page.locator('[data-testid="hero-logo"]');
    await expect(logoElement).toBeAttached();

    // The logo should have accessible text either via aria-label or visible text
    const ariaLabel = await logoElement.getAttribute('aria-label');
    const roleImg = await logoElement.getAttribute('role');

    // If it's marked as an image role, it needs aria-label
    if (roleImg === 'img') {
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toContain('mir');
    }

    // Check navbar logo
    const navLogo = page.locator('[aria-label*="MirDB"], [aria-label*="Home"]');
    const navLogoCount = await navLogo.count();
    expect(navLogoCount).toBeGreaterThan(0);

    // Verify any actual images have alt text
    const images = await page.locator('img').all();
    for (const img of images) {
      const altText = await img.getAttribute('alt');
      const ariaHidden = await img.getAttribute('aria-hidden');

      // Images must have alt text unless decorative (aria-hidden)
      if (ariaHidden !== 'true') {
        expect(altText).not.toBeNull();
        expect(altText).not.toBe('');
      }
    }
  });

  test('Test Case 5: Check heading hierarchy - should have proper h1, h2, h3 without skipped levels', async ({
    page,
  }) => {
    // Get all headings in order
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((h) => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim().substring(0, 50),
      }));
    });

    // Should have at least one h1
    const h1Count = headings.filter((h) => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Check for skipped heading levels
    let previousLevel = 0;
    for (const heading of headings) {
      // Heading level should not skip more than one level down
      // (e.g., h1 -> h3 is bad, h1 -> h2 is good)
      if (previousLevel > 0) {
        const levelDiff = heading.level - previousLevel;
        // Allow going up any amount but not skipping levels going down
        expect(levelDiff).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }

    // Verify h1 exists and contains meaningful content
    const h1Text = headings.find((h) => h.level === 1)?.text;
    expect(h1Text).toBeTruthy();
    expect(h1Text!.length).toBeGreaterThan(0);
  });

  test('Test Case 6: Verify form labels - any form inputs should have associated labels', async ({
    page,
  }) => {
    // Get all form inputs
    const inputs = await page
      .locator('input, select, textarea')
      .all();

    for (const input of inputs) {
      const inputId = await input.getAttribute('id');
      const inputType = await input.getAttribute('type');
      const ariaLabel = await input.getAttribute('aria-label');
      const ariaLabelledby = await input.getAttribute('aria-labelledby');

      // Skip hidden inputs
      if (inputType === 'hidden') continue;

      // Input should have either:
      // 1. An associated label element
      // 2. An aria-label attribute
      // 3. An aria-labelledby attribute
      const hasLabel =
        ariaLabel ||
        ariaLabelledby ||
        (inputId && (await page.locator(`label[for="${inputId}"]`).count()) > 0);

      expect(hasLabel).toBeTruthy();
    }
  });

  test('Test Case 7: Check color contrast in light mode - should pass WCAG AA requirements', async ({
    page,
  }) => {
    // Ensure we're in light mode
    await page.evaluate(() => {
      document.documentElement.removeAttribute('data-theme');
    });

    // Run axe-core specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({
        rules: {
          'color-contrast': { enabled: true },
        },
      })
      .analyze();

    // Filter for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    // Log violations for debugging
    if (contrastViolations.length > 0) {
      console.log(
        'Color contrast violations (light mode):',
        JSON.stringify(contrastViolations, null, 2)
      );
    }

    expect(contrastViolations).toHaveLength(0);
  });

  test('Test Case 8: Check color contrast in dark mode - should pass WCAG AA requirements', async ({
    page,
  }) => {
    // Enable dark mode
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark');
    });

    // Wait for theme transition
    await page.waitForTimeout(300);

    // Run axe-core specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({
        rules: {
          'color-contrast': { enabled: true },
        },
      })
      .analyze();

    // Filter for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    // Log violations for debugging
    if (contrastViolations.length > 0) {
      console.log(
        'Color contrast violations (dark mode):',
        JSON.stringify(contrastViolations, null, 2)
      );
    }

    expect(contrastViolations).toHaveLength(0);
  });
});
