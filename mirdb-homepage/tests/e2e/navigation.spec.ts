/**
 * Navigation E2E Tests
 * Owner: Shared across Scenarios 6, 7, 15
 *
 * Navigation and link tests:
 * - Documentation links (Scenario 6)
 * - Contributing/GitHub links (Scenario 7)
 * - Link validation (Scenario 15)
 */

import { test, expect } from '@playwright/test';

test.describe('Documentation Links - Scenario 6', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture documentation link is present and clickable', async ({ page }) => {
    const architectureLink = page.getByTestId('doc-link-architecture');

    await expect(architectureLink).toBeVisible();
    await expect(architectureLink).toHaveAttribute('href', 'https://docs.mirdb.dev/architecture');

    // Verify the link is clickable (has proper attributes)
    await expect(architectureLink).toHaveAttribute('target', '_blank');
    await expect(architectureLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label is correct
    const label = architectureLink.locator('.documentation__link-label');
    await expect(label).toHaveText('Architecture');
  });

  test('TC2: API Reference documentation link is present and clickable', async ({ page }) => {
    const apiRefLink = page.getByTestId('doc-link-api-reference');

    await expect(apiRefLink).toBeVisible();
    await expect(apiRefLink).toHaveAttribute('href', 'https://docs.mirdb.dev/api');

    // Verify the link is clickable (has proper attributes)
    await expect(apiRefLink).toHaveAttribute('target', '_blank');
    await expect(apiRefLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label is correct
    const label = apiRefLink.locator('.documentation__link-label');
    await expect(label).toHaveText('API Reference');
  });

  test('TC3: Configuration documentation link is present and clickable', async ({ page }) => {
    const configLink = page.getByTestId('doc-link-configuration');

    await expect(configLink).toBeVisible();
    await expect(configLink).toHaveAttribute('href', 'https://docs.mirdb.dev/configuration');

    // Verify the link is clickable (has proper attributes)
    await expect(configLink).toHaveAttribute('target', '_blank');
    await expect(configLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label is correct
    const label = configLink.locator('.documentation__link-label');
    await expect(label).toHaveText('Configuration');
  });

  test('TC4: Architecture link has correct navigation URL', async ({ page }) => {
    const architectureLink = page.getByTestId('doc-link-architecture');

    // Verify the href points to the correct architecture documentation page
    const href = await architectureLink.getAttribute('href');
    expect(href).toBe('https://docs.mirdb.dev/architecture');

    // Verify the link opens in a new tab (external link)
    await expect(architectureLink).toHaveAttribute('target', '_blank');
  });

  test('TC5: API Reference link has correct navigation URL', async ({ page }) => {
    const apiRefLink = page.getByTestId('doc-link-api-reference');

    // Verify the href points to the correct API reference documentation page
    const href = await apiRefLink.getAttribute('href');
    expect(href).toBe('https://docs.mirdb.dev/api');

    // Verify the link opens in a new tab (external link)
    await expect(apiRefLink).toHaveAttribute('target', '_blank');
  });

  test('TC6: Configuration link has correct navigation URL', async ({ page }) => {
    const configLink = page.getByTestId('doc-link-configuration');

    // Verify the href points to the correct configuration documentation page
    const href = await configLink.getAttribute('href');
    expect(href).toBe('https://docs.mirdb.dev/configuration');

    // Verify the link opens in a new tab (external link)
    await expect(configLink).toHaveAttribute('target', '_blank');
  });

  test('All three documentation links are present in the documentation section', async ({ page }) => {
    const documentationSection = page.locator('#documentation');

    await expect(documentationSection).toBeVisible();

    // Verify all three links are present
    const architectureLink = page.getByTestId('doc-link-architecture');
    const apiRefLink = page.getByTestId('doc-link-api-reference');
    const configLink = page.getByTestId('doc-link-configuration');

    await expect(architectureLink).toBeVisible();
    await expect(apiRefLink).toBeVisible();
    await expect(configLink).toBeVisible();
  });

  test('Documentation section has proper accessibility attributes', async ({ page }) => {
    const documentationSection = page.locator('#documentation');

    // Verify section has aria-labelledby
    await expect(documentationSection).toHaveAttribute('aria-labelledby', 'documentation-heading');

    // Verify navigation has aria-label
    const nav = page.locator('.documentation__nav');
    await expect(nav).toHaveAttribute('aria-label', 'Documentation navigation');

    // Verify heading is present
    const heading = page.locator('#documentation-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Documentation');
  });

  test('Documentation links have descriptions', async ({ page }) => {
    const architectureLink = page.getByTestId('doc-link-architecture');
    const apiRefLink = page.getByTestId('doc-link-api-reference');
    const configLink = page.getByTestId('doc-link-configuration');

    // Verify each link has a description
    const archDescription = architectureLink.locator('.documentation__link-description');
    const apiDescription = apiRefLink.locator('.documentation__link-description');
    const configDescription = configLink.locator('.documentation__link-description');

    await expect(archDescription).toBeVisible();
    await expect(apiDescription).toBeVisible();
    await expect(configDescription).toBeVisible();

    // Verify descriptions contain meaningful text
    await expect(archDescription).toContainText('architecture');
    await expect(apiDescription).toContainText('API');
    await expect(configDescription).toContainText('Configuration');
  });
});
