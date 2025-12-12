import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

/**
 * Accessibility - Color Contrast E2E Tests
 *
 * These tests verify that color contrast meets WCAG AA standards as per NFR-4:
 * - Normal text: minimum 4.5:1 contrast ratio
 * - Large text (18pt+ or 14pt+ bold): minimum 3:1 contrast ratio
 *
 * Uses axe-core accessibility testing engine via @axe-core/playwright
 */
test.describe('Accessibility - Color Contrast (NFR-4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: Body text contrast ratio is at least 4.5:1', async ({ page }) => {
    // Run axe-core scan specifically for color contrast on text elements
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('[data-testid="hero-subheadline"]')
      .include('.footer-link')
      .include('.footer-address')
      .include('.footer-copyright')
      .analyze();

    // Filter for color-contrast violations specifically
    const contrastViolations = accessibilityScanResults.violations.filter(
      (violation) => violation.id === 'color-contrast'
    );

    // Verify no contrast violations for body text
    expect(contrastViolations).toHaveLength(0);
  });

  test('Test Case 2: Headings contrast ratio meets WCAG AA requirements', async ({ page }) => {
    // Run axe-core scan specifically for heading elements
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('[data-testid="hero-headline"]')
      .include('.footer-section-title')
      .analyze();

    // Filter for color-contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (violation) => violation.id === 'color-contrast'
    );

    // Verify headings have sufficient contrast
    expect(contrastViolations).toHaveLength(0);
  });

  test('Test Case 3: CTA button text contrast is at least 4.5:1', async ({ page }) => {
    // Run axe-core scan specifically for CTA button
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('[data-testid="hero-cta"]')
      .analyze();

    // Filter for color-contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (violation) => violation.id === 'color-contrast'
    );

    // Verify CTA button text has sufficient contrast
    expect(contrastViolations).toHaveLength(0);
  });

  test('Test Case 4: Navigation link contrast meets WCAG AA', async ({ page }) => {
    // Run axe-core scan specifically for navigation links
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('.navigation-link')
      .include('.navigation-logo')
      .analyze();

    // Filter for color-contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (violation) => violation.id === 'color-contrast'
    );

    // Verify navigation links have sufficient contrast
    expect(contrastViolations).toHaveLength(0);
  });

  test('Full page accessibility scan for color contrast violations', async ({ page }) => {
    // Run comprehensive axe-core scan for all WCAG 2.1 AA color contrast rules
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for color-contrast violations only
    const contrastViolations = accessibilityScanResults.violations.filter(
      (violation) => violation.id === 'color-contrast'
    );

    // Log any violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      contrastViolations.forEach((violation) => {
        console.log(`  - ${violation.description}`);
        violation.nodes.forEach((node) => {
          console.log(`    Element: ${node.html}`);
          console.log(`    Failure: ${node.failureSummary}`);
        });
      });
    }

    // Verify no color contrast violations across the entire page
    expect(contrastViolations).toHaveLength(0);
  });

  test('Hero section text elements meet contrast requirements', async ({ page }) => {
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero headline is visible and readable
    const headline = page.locator('[data-testid="hero-headline"]');
    await expect(headline).toBeVisible();

    // Get computed styles for manual verification
    const headlineColor = await headline.evaluate((el) =>
      window.getComputedStyle(el).color
    );
    const heroBackground = await heroSection.evaluate((el) =>
      window.getComputedStyle(el).background
    );

    // Headline should have white text (#ffffff)
    expect(headlineColor).toContain('255');

    // Background should be dark gradient
    expect(heroBackground).toBeTruthy();

    // Run axe-core specifically on hero section
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('[data-testid="hero-section"]')
      .analyze();

    const contrastViolations = results.violations.filter(
      (v) => v.id === 'color-contrast'
    );
    expect(contrastViolations).toHaveLength(0);
  });

  test('Footer text elements meet contrast requirements', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer content is visible
    const footerContent = page.locator('[data-testid="footer-content"]');
    await expect(footerContent).toBeVisible();

    // Run axe-core specifically on footer section
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('[data-testid="footer"]')
      .analyze();

    const contrastViolations = results.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    // If there are violations, log them for debugging
    if (contrastViolations.length > 0) {
      console.log('Footer contrast violations:');
      contrastViolations.forEach((v) => {
        v.nodes.forEach((node) => {
          console.log(`  - ${node.html}: ${node.failureSummary}`);
        });
      });
    }

    expect(contrastViolations).toHaveLength(0);
  });

  test('Navigation elements meet contrast requirements', async ({ page }) => {
    const navigation = page.locator('[data-testid="main-navigation"]');
    await expect(navigation).toBeVisible();

    // Get navigation link colors for verification
    const linkColor = await page.locator('.navigation-link').first().evaluate((el) =>
      window.getComputedStyle(el).color
    );

    // Navigation links should have visible text color
    expect(linkColor).toBeTruthy();

    // Run axe-core specifically on navigation
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('[data-testid="main-navigation"]')
      .analyze();

    const contrastViolations = results.violations.filter(
      (v) => v.id === 'color-contrast'
    );
    expect(contrastViolations).toHaveLength(0);
  });
});
