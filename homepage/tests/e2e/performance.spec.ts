/**
 * E2E tests for Performance section.
 * Owner: Scenario 5 - Performance Section Display
 *
 * Tests:
 * - TC1: Check Performance section exists with heading
 * - TC2: Verify performance metrics are displayed
 * - TC3: Verify context information (hardware specs, methodology)
 */
import { test, expect } from '@playwright/test';

test.describe('Performance Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Performance section exists with heading', async ({ page }) => {
    // Test case 1: Check Performance section is present with heading
    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();

    const heading = performanceSection.locator('h2');
    await expect(heading).toHaveText('Performance');
  });

  test('Performance section has correct data-testid', async ({ page }) => {
    // Additional validation for section identification
    const performanceSection = page.locator('[data-testid="performance-section"]');
    await expect(performanceSection).toBeVisible();
  });

  test('Performance metrics are displayed', async ({ page }) => {
    // Test case 2: Verify performance benchmarks or characteristics are shown
    const performanceSection = page.locator('#performance');

    // Check performance chart exists
    const performanceChart = performanceSection.locator('[data-testid="performance-chart"]');
    await expect(performanceChart).toBeVisible();

    // Check that metrics are displayed
    const metrics = performanceSection.locator('[data-testid="performance-metric"]');
    const metricCount = await metrics.count();
    expect(metricCount).toBeGreaterThan(0);

    // Verify at least one metric value is displayed
    const metricValues = performanceSection.locator('[data-testid="metric-value"]');
    await expect(metricValues.first()).toBeVisible();
  });

  test('Performance metrics show relevant benchmarks', async ({ page }) => {
    // Verify specific benchmark metrics are present
    const performanceSection = page.locator('#performance');

    // Check for common performance metric labels
    await expect(performanceSection).toContainText('ops/sec');
    await expect(performanceSection).toContainText('ms');
  });

  test('Performance metrics have visual bars', async ({ page }) => {
    // Verify visual representation of metrics
    const performanceSection = page.locator('#performance');

    // Check for metric bars
    const metricBars = performanceSection.locator('[data-testid="metric-bar"]');
    const barCount = await metricBars.count();
    expect(barCount).toBeGreaterThan(0);

    // Verify bars have width (indicating they're displaying data)
    const firstBar = metricBars.first();
    const width = await firstBar.evaluate((el) => {
      return window.getComputedStyle(el).width;
    });
    expect(width).not.toBe('0px');
  });

  test('Context information is provided', async ({ page }) => {
    // Test case 3: Performance data includes hardware specs or test methodology
    const performanceSection = page.locator('#performance');

    // Check context box exists
    const contextBox = performanceSection.locator('[data-testid="performance-context"]');
    await expect(contextBox).toBeVisible();

    // Check hardware specs are displayed
    const hardwareSpecs = performanceSection.locator('[data-testid="hardware-specs"]');
    await expect(hardwareSpecs).toBeVisible();
    const hardwareText = await hardwareSpecs.textContent();
    expect(hardwareText).toBeTruthy();
    expect(hardwareText!.length).toBeGreaterThan(10);

    // Check test methodology is displayed
    const methodology = performanceSection.locator('[data-testid="test-methodology"]');
    await expect(methodology).toBeVisible();
    const methodologyText = await methodology.textContent();
    expect(methodologyText).toBeTruthy();
    expect(methodologyText!.length).toBeGreaterThan(10);
  });

  test('Context includes hardware specifications', async ({ page }) => {
    // Verify hardware context contains relevant information
    const performanceSection = page.locator('#performance');
    const hardwareSpecs = performanceSection.locator('[data-testid="hardware-specs"]');

    const hardwareText = await hardwareSpecs.textContent();
    // Should contain hardware-related terms
    expect(
      hardwareText?.toLowerCase().includes('core') ||
      hardwareText?.toLowerCase().includes('cpu') ||
      hardwareText?.toLowerCase().includes('intel') ||
      hardwareText?.toLowerCase().includes('amd') ||
      hardwareText?.toLowerCase().includes('gb') ||
      hardwareText?.toLowerCase().includes('ssd')
    ).toBeTruthy();
  });

  test('Context includes test methodology', async ({ page }) => {
    // Verify methodology context contains relevant information
    const performanceSection = page.locator('#performance');
    const methodology = performanceSection.locator('[data-testid="test-methodology"]');

    const methodologyText = await methodology.textContent();
    // Should contain methodology-related terms
    expect(
      methodologyText?.toLowerCase().includes('benchmark') ||
      methodologyText?.toLowerCase().includes('test') ||
      methodologyText?.toLowerCase().includes('key-value') ||
      methodologyText?.toLowerCase().includes('memcached') ||
      methodologyText?.toLowerCase().includes('pairs')
    ).toBeTruthy();
  });

  test('Performance note is displayed', async ({ page }) => {
    // Check that a disclaimer/note about results is shown
    const performanceSection = page.locator('#performance');
    const note = performanceSection.locator('[data-testid="performance-note"]');

    await expect(note).toBeVisible();
    const noteText = await note.textContent();
    expect(noteText).toBeTruthy();
    expect(noteText!.toLowerCase()).toContain('vary');
  });

  test('Performance section is accessible via anchor link', async ({ page }) => {
    // Navigate directly to performance section
    await page.goto('/#performance');

    const performanceSection = page.locator('#performance');
    await expect(performanceSection).toBeVisible();
  });

  test('Performance section has proper accessibility attributes', async ({ page }) => {
    const performanceSection = page.locator('#performance');

    // Check aria-labelledby is set correctly
    const ariaLabelledBy = await performanceSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('performance-heading');

    // Check heading has correct id
    const heading = performanceSection.locator('#performance-heading');
    await expect(heading).toBeVisible();
  });
});
