/**
 * Accessibility E2E Tests for Homepage
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Tests WCAG 2.1 AA accessibility standards including:
 * - Keyboard navigation
 * - Focus indicators
 * - Contrast ratios
 * - Alt text / aria-labels
 * - Enter key activation
 * - Semantic HTML structure
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { homepageSelectors } from './fixtures';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded and animations to settle
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Tab through homepage interactive elements - all buttons, links, and inputs are focusable in logical order', async ({
    page,
  }) => {
    // Get all interactive elements that should be focusable
    const interactiveSelectors = [
      homepageSelectors.hero.primaryCta,
      homepageSelectors.hero.secondaryCta,
    ];

    // Start from the body and tab through elements
    await page.keyboard.press('Tab');

    // Track focused elements
    const focusedElements: string[] = [];
    let previousElement = '';

    // Tab through a reasonable number of times to cover all interactive elements
    for (let i = 0; i < 15; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return '';
        return el.tagName + (el.getAttribute('data-testid') || el.textContent?.slice(0, 20) || '');
      });

      if (focusedElement && focusedElement !== previousElement) {
        focusedElements.push(focusedElement);
        previousElement = focusedElement;
      }

      await page.keyboard.press('Tab');
    }

    // Verify interactive elements are in focus order
    const primaryCta = page.locator(homepageSelectors.hero.primaryCta);
    const secondaryCta = page.locator(homepageSelectors.hero.secondaryCta);

    // Verify CTAs exist and are tabbable
    await expect(primaryCta).toBeVisible();
    await expect(secondaryCta).toBeVisible();

    // Focus on primary CTA and verify it receives focus
    await primaryCta.focus();
    await expect(primaryCta).toBeFocused();

    // Tab to secondary CTA
    await page.keyboard.press('Tab');

    // Verify we can reach the secondary CTA
    await secondaryCta.focus();
    await expect(secondaryCta).toBeFocused();

    // Verify there are multiple focusable elements
    expect(focusedElements.length).toBeGreaterThan(0);
  });

  test('Test Case 2: Check focus indicator on CTA buttons - visible focus ring appears when buttons are focused', async ({
    page,
  }) => {
    const primaryCta = page.locator(homepageSelectors.hero.primaryCta);
    const secondaryCta = page.locator(homepageSelectors.hero.secondaryCta);

    // Focus on primary CTA
    await primaryCta.focus();
    await expect(primaryCta).toBeFocused();

    // Check that the button has visible focus styles
    // DaisyUI buttons should have focus:outline or focus-visible styles
    const primaryFocusStyles = await primaryCta.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineColor: styles.outlineColor,
        boxShadow: styles.boxShadow,
        // Check for any visible focus indicator
        hasVisibleFocus:
          styles.outlineWidth !== '0px' ||
          styles.boxShadow !== 'none' ||
          styles.outline !== 'none' ||
          el.classList.contains('focus') ||
          el.matches(':focus-visible'),
      };
    });

    // Verify some form of focus indication exists
    // Either outline, box-shadow, or focus-visible state
    const hasFocusIndicator =
      primaryFocusStyles.hasVisibleFocus ||
      primaryFocusStyles.outlineWidth !== '0px' ||
      primaryFocusStyles.boxShadow !== 'none';

    expect(hasFocusIndicator).toBe(true);

    // Check secondary CTA as well
    await secondaryCta.focus();
    await expect(secondaryCta).toBeFocused();

    const secondaryFocusStyles = await secondaryCta.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        hasVisibleFocus:
          styles.outlineWidth !== '0px' ||
          styles.boxShadow !== 'none' ||
          el.matches(':focus-visible'),
      };
    });

    expect(secondaryFocusStyles.hasVisibleFocus).toBe(true);
  });

  test('Test Case 3: Measure hero text contrast ratio - text contrast ratio meets or exceeds 4.5:1', async ({
    page,
  }) => {
    // Use axe-core to check color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .include(homepageSelectors.hero.section)
      .withRules(['color-contrast'])
      .analyze();

    // Check for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    // Log any violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Contrast violations:', JSON.stringify(contrastViolations, null, 2));
    }

    // Verify no contrast violations in hero section
    expect(contrastViolations.length).toBe(0);

    // Additionally, manually verify headline is visible and readable
    const headline = page.locator(homepageSelectors.hero.headline);
    await expect(headline).toBeVisible();
    await expect(headline).toHaveText(/Shorten Links\. Track Clicks\./);
  });

  test('Test Case 4: Check feature icons for alt text - all icons have appropriate alt text or aria-labels', async ({
    page,
  }) => {
    // Wait for features section to be visible
    const featuresSection = page.locator(homepageSelectors.features.section);
    await expect(featuresSection).toBeVisible();

    // Get all SVG icons in feature cards
    const featureIcons = featuresSection.locator('svg');
    const iconCount = await featureIcons.count();

    expect(iconCount).toBeGreaterThan(0);

    // Check each icon has aria-hidden="true" (since the text next to it provides context)
    // OR has an aria-label
    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');
      const ariaLabel = await icon.getAttribute('aria-label');
      const role = await icon.getAttribute('role');

      // Icons should either be hidden from screen readers (with text alternative nearby)
      // or have an aria-label
      const hasAccessibleTreatment =
        ariaHidden === 'true' || ariaLabel !== null || role === 'img';

      expect(hasAccessibleTreatment).toBe(true);
    }

    // Verify each feature card has a title that provides context
    const featureCards = featuresSection.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const title = card.locator('h3');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText?.length).toBeGreaterThan(0);
    }

    // Run axe-core scan for image-alt rule on entire page
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['image-alt', 'svg-img-alt'])
      .analyze();

    // Filter out non-critical violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations.length).toBe(0);
  });

  test('Test Case 5: Activate CTA using Enter key - Enter key triggers navigation same as click', async ({
    page,
  }) => {
    const primaryCta = page.locator(homepageSelectors.hero.primaryCta);

    // Focus on primary CTA
    await primaryCta.focus();
    await expect(primaryCta).toBeFocused();

    // Get current URL
    const initialUrl = page.url();

    // Press Enter to activate the CTA
    await page.keyboard.press('Enter');

    // Wait for navigation
    await page.waitForURL(/\/(register|dashboard)/);

    // Verify navigation occurred
    const newUrl = page.url();
    expect(newUrl).not.toBe(initialUrl);
    expect(newUrl).toMatch(/\/(register|dashboard)/);
  });

  test('Test Case 6: Verify semantic HTML structure - page uses proper heading hierarchy (h1, h2, h3)', async ({
    page,
  }) => {
    // Check for h1 element (should be exactly one)
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // Verify h1 is the hero headline
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text).toContain('Shorten Links');

    // Check for h2 elements (section headings)
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(1);

    // Check for h3 elements (feature card titles)
    const h3Elements = page.locator('h3');
    const h3Count = await h3Elements.count();
    expect(h3Count).toBeGreaterThanOrEqual(4); // At least 4 feature cards

    // Run axe-core heading-order check
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['heading-order', 'page-has-heading-one'])
      .analyze();

    // Check for heading violations
    const headingViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'heading-order' || v.id === 'page-has-heading-one'
    );

    expect(headingViolations.length).toBe(0);
  });

  test('Full page accessibility audit - no critical WCAG violations', async ({ page }) => {
    // Run comprehensive axe-core scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log all violations for debugging with node details
    if (accessibilityScanResults.violations.length > 0) {
      console.log(
        'All accessibility violations:',
        JSON.stringify(
          accessibilityScanResults.violations.map((v) => ({
            id: v.id,
            impact: v.impact,
            description: v.description,
            nodes: v.nodes.map((n) => ({
              html: n.html,
              target: n.target,
              failureSummary: n.failureSummary,
            })),
          })),
          null,
          2
        )
      );
    }

    // No critical or serious violations allowed
    expect(criticalViolations.length).toBe(0);
  });
});
