/**
 * Accessibility Integration Tests
 * Owner: Scenario 11 - Accessibility Standards Compliance
 *
 * Comprehensive axe-core accessibility testing:
 * - WCAG 2.1 AA compliance
 * - Automated accessibility violation detection
 * - Section-by-section analysis
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Integration Tests - Scenario 11', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 7: Run axe-core accessibility scan
   * Expected: No critical or serious accessibility violations
   */
  test('TC7: Full page axe-core accessibility scan - no critical or serious violations', async ({ page }) => {
    // Run comprehensive axe-core accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalAndSeriousViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log all violations for debugging
    if (criticalAndSeriousViolations.length > 0) {
      console.log('\n=== Critical/Serious Accessibility Violations ===');
      criticalAndSeriousViolations.forEach((v) => {
        console.log(`\n[${v.impact?.toUpperCase()}] ${v.id}: ${v.description}`);
        console.log(`Help: ${v.helpUrl}`);
        v.nodes.forEach((node, index) => {
          console.log(`  Node ${index + 1}:`);
          console.log(`    Target: ${JSON.stringify(node.target)}`);
          console.log(`    HTML: ${node.html.substring(0, 150)}...`);
          console.log(`    Failure: ${node.failureSummary}`);
        });
      });
    }

    // Expect no critical or serious violations
    expect(
      criticalAndSeriousViolations,
      `Found ${criticalAndSeriousViolations.length} critical/serious accessibility violations`
    ).toHaveLength(0);
  });

  test('Hero section passes accessibility scan', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('[data-testid="hero-section"]')
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);
  });

  test('Features section passes accessibility scan', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('#features')
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);
  });

  test('Quick Start section passes accessibility scan', async ({ page }) => {
    const quickstartSection = page.getByTestId('quickstart-section');
    await expect(quickstartSection).toBeVisible();

    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('[data-testid="quickstart-section"]')
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);
  });

  test('Performance section passes accessibility scan', async ({ page }) => {
    const performanceSection = page.getByTestId('performance-section');
    await expect(performanceSection).toBeVisible();

    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('[data-testid="performance-section"]')
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);
  });

  test('Documentation section passes accessibility scan', async ({ page }) => {
    const documentationSection = page.locator('#documentation');
    await expect(documentationSection).toBeVisible();

    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('#documentation')
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);
  });

  test('Contributing section passes accessibility scan', async ({ page }) => {
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeVisible();

    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('#contributing')
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);
  });

  test('Footer passes accessibility scan', async ({ page }) => {
    const footer = page.getByTestId('footer-section');
    await expect(footer).toBeVisible();

    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('[data-testid="footer-section"]')
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);
  });

  test('Page has no landmark violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const landmarkViolations = accessibilityScanResults.violations.filter(
      (v) => v.id.includes('landmark') || v.id.includes('region')
    );

    expect(landmarkViolations).toHaveLength(0);
  });

  test('Page has no link violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const linkViolations = accessibilityScanResults.violations.filter(
      (v) => v.id.includes('link')
    );

    if (linkViolations.length > 0) {
      console.log('Link violations:', JSON.stringify(linkViolations, null, 2));
    }

    // Filter for critical/serious only
    const criticalLinkViolations = linkViolations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalLinkViolations).toHaveLength(0);
  });

  test('Page has no button violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const buttonViolations = accessibilityScanResults.violations.filter(
      (v) => v.id.includes('button')
    );

    // Filter for critical/serious only
    const criticalButtonViolations = buttonViolations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalButtonViolations).toHaveLength(0);
  });

  test('Color contrast passes for all text elements', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    const contrastViolations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    if (contrastViolations.length > 0) {
      console.log('\n=== Color Contrast Violations ===');
      contrastViolations.forEach((v) => {
        v.nodes.forEach((node) => {
          console.log(`Target: ${JSON.stringify(node.target)}`);
          console.log(`HTML: ${node.html.substring(0, 100)}`);
          console.log(`Issue: ${node.failureSummary}\n`);
        });
      });
    }

    expect(contrastViolations).toHaveLength(0);
  });

  test('All form controls have accessible labels', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const labelViolations = accessibilityScanResults.violations.filter(
      (v) => v.id.includes('label') || v.id.includes('form')
    );

    const criticalLabelViolations = labelViolations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalLabelViolations).toHaveLength(0);
  });

  test('ARIA attributes are used correctly', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const ariaViolations = accessibilityScanResults.violations.filter(
      (v) => v.id.includes('aria')
    );

    const criticalAriaViolations = ariaViolations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalAriaViolations).toHaveLength(0);
  });

  test('Best practices compliance', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['best-practice'])
      .analyze();

    // Log best practice warnings (not failures, just info)
    if (accessibilityScanResults.violations.length > 0) {
      console.log('\n=== Best Practice Warnings ===');
      accessibilityScanResults.violations.forEach((v) => {
        console.log(`[${v.impact}] ${v.id}: ${v.description}`);
      });
    }

    // Filter for critical/serious only
    const criticalBestPracticeViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalBestPracticeViolations).toHaveLength(0);
  });
});
