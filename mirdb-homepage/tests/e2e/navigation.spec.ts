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

test.describe('Contributing Section and GitHub Links - Scenario 7', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: GitHub repository link is present', async ({ page }) => {
    const githubLink = page.getByTestId('contributing-link-github');

    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb');

    // Verify link label is correct
    const label = githubLink.locator('.contributing__link-label');
    await expect(label).toHaveText('GitHub Repository');
  });

  test('TC2: Contribution guidelines link is present', async ({ page }) => {
    const contributingLink = page.getByTestId('contributing-link-contributing-guidelines');

    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb/blob/main/CONTRIBUTING.md');

    // Verify link label is correct
    const label = contributingLink.locator('.contributing__link-label');
    await expect(label).toHaveText('Contribution Guidelines');
  });

  test('TC3: GitHub link opens in new tab with rel noopener', async ({ page }) => {
    const githubLink = page.getByTestId('contributing-link-github');

    await expect(githubLink).toBeVisible();

    // Verify the link opens in a new tab with proper security attributes
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('TC4: Contribution guidelines link navigates to guidelines document', async ({ page }) => {
    const contributingLink = page.getByTestId('contributing-link-contributing-guidelines');

    await expect(contributingLink).toBeVisible();

    // Verify the href points to the CONTRIBUTING.md file
    const href = await contributingLink.getAttribute('href');
    expect(href).toBe('https://github.com/mirdb/mirdb/blob/main/CONTRIBUTING.md');

    // Verify the link opens in a new tab with proper security attributes
    await expect(contributingLink).toHaveAttribute('target', '_blank');
    await expect(contributingLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Contributing section has proper accessibility attributes', async ({ page }) => {
    const contributingSection = page.locator('#contributing');

    // Verify section exists and is visible
    await expect(contributingSection).toBeVisible();

    // Verify section has aria-labelledby
    await expect(contributingSection).toHaveAttribute('aria-labelledby', 'contributing-heading');

    // Verify navigation has aria-label
    const nav = page.locator('.contributing__nav');
    await expect(nav).toHaveAttribute('aria-label', 'Contributing navigation');

    // Verify heading is present
    const heading = page.locator('#contributing-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Contribute');
  });

  test('All contributing links have descriptions', async ({ page }) => {
    const githubLink = page.getByTestId('contributing-link-github');
    const contributingLink = page.getByTestId('contributing-link-contributing-guidelines');

    // Verify each link has a description
    const githubDescription = githubLink.locator('.contributing__link-description');
    const contributingDescription = contributingLink.locator('.contributing__link-description');

    await expect(githubDescription).toBeVisible();
    await expect(contributingDescription).toBeVisible();

    // Verify descriptions contain meaningful text
    await expect(githubDescription).toContainText('source code');
    await expect(contributingDescription).toContainText('contribute');
  });

  test('Issues link is present and has correct attributes', async ({ page }) => {
    const issuesLink = page.getByTestId('contributing-link-issues');

    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb/issues');
    await expect(issuesLink).toHaveAttribute('target', '_blank');
    await expect(issuesLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label
    const label = issuesLink.locator('.contributing__link-label');
    await expect(label).toHaveText('Report Issues');
  });

  test('Discussions link is present and has correct attributes', async ({ page }) => {
    const discussionsLink = page.getByTestId('contributing-link-discussions');

    await expect(discussionsLink).toBeVisible();
    await expect(discussionsLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb/discussions');
    await expect(discussionsLink).toHaveAttribute('target', '_blank');
    await expect(discussionsLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify link label
    const label = discussionsLink.locator('.contributing__link-label');
    await expect(label).toHaveText('Discussions');
  });
});
