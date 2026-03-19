/**
 * E2E tests for Project Status Section.
 * Owner: Scenario 4 - Project Status Section
 */

import { test, expect } from '@playwright/test';

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display the Project Status section', async ({ page }) => {
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();
  });

  test('should display implemented features with checkmarks', async ({ page }) => {
    const implementedFeaturesCard = page.getByTestId('implemented-features-card');
    await expect(implementedFeaturesCard).toBeVisible();

    // Check for implemented features
    await expect(page.getByText('Async networking with Tokio')).toBeVisible();
    await expect(page.getByText('Memtable with skip list')).toBeVisible();
    await expect(page.getByText('Minor compaction')).toBeVisible();
    await expect(page.getByText('Major compaction')).toBeVisible();
  });

  test('should display planned features with Coming Soon badge', async ({ page }) => {
    const plannedFeaturesCard = page.getByTestId('planned-features-card');
    await expect(plannedFeaturesCard).toBeVisible();

    // Check for Raft consensus
    await expect(page.getByText('Raft consensus')).toBeVisible();

    // Check for Coming Soon badge
    const comingSoonBadge = page.getByTestId('coming-soon-badge');
    await expect(comingSoonBadge).toBeVisible();
    await expect(comingSoonBadge).toHaveText('Coming Soon');
  });

  test('should display CircleCI badge with correct link', async ({ page }) => {
    const ciBadgeLink = page.getByTestId('ci-badge-link');
    const ciBadgeImage = page.getByTestId('ci-badge-image');

    await expect(ciBadgeLink).toBeVisible();
    await expect(ciBadgeImage).toBeVisible();

    // Check the link points to CircleCI
    await expect(ciBadgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
  });

  // Test Case 4: Click CI badge opens CircleCI pipeline page in new tab
  test('TC4: clicking CI badge opens CircleCI pipeline in new tab', async ({ page, context }) => {
    // Set up listener for new page (tab) before clicking
    const pagePromise = context.waitForEvent('page');

    // Click the CI badge link
    const ciBadgeLink = page.getByTestId('ci-badge-link');
    await ciBadgeLink.click();

    // Wait for the new page to open
    const newPage = await pagePromise;

    // Verify the new page URL is the CircleCI pipeline
    expect(newPage.url()).toContain('circleci.com/gh/yetone/mirdb');
  });

  test('CI badge link should have security attributes', async ({ page }) => {
    const ciBadgeLink = page.getByTestId('ci-badge-link');

    await expect(ciBadgeLink).toHaveAttribute('target', '_blank');
    await expect(ciBadgeLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('should be accessible via keyboard navigation', async ({ page }) => {
    // Tab to the status section
    await page.keyboard.press('Tab');

    // Keep tabbing until we reach the CI badge link
    let foundBadgeLink = false;
    for (let i = 0; i < 30; i++) {
      await page.keyboard.press('Tab');
      const focusedElement = page.locator(':focus');
      const testId = await focusedElement.getAttribute('data-testid');
      if (testId === 'ci-badge-link') {
        foundBadgeLink = true;
        break;
      }
    }

    expect(foundBadgeLink).toBe(true);
  });
});
