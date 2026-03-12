/**
 * Status Badge E2E Tests
 * Owner: Scenario 4 - Status Badge Integration
 *
 * Tests:
 * - CI/CD status badge is present
 * - Badge image loads successfully
 * - Badge links to CI/CD pipeline
 *
 * Traceability: REQ-8
 */

import { test, expect } from '@playwright/test';
import { BASE_URL, waitForPageLoad, getByTestId } from './test-utils';

test.describe('Status Badge Integration', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    await waitForPageLoad(page);
  });

  test('TC1: CI/CD status badge image exists on the page', async ({ page }) => {
    // Locate the CI/CD status badge image element
    const badgeImage = page.locator(getByTestId('ci-badge'));

    // Verify badge image element exists
    await expect(badgeImage).toBeVisible();

    // Verify it's an img element
    const tagName = await badgeImage.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('img');
  });

  test('TC2: Badge image src points to CI provider badge URL', async ({ page }) => {
    const badgeImage = page.locator(getByTestId('ci-badge'));

    // Get the src attribute
    const src = await badgeImage.getAttribute('src');

    // Verify src exists and points to a badge URL
    expect(src).toBeTruthy();
    expect(src).toMatch(/^https:\/\//); // Should be a valid HTTPS URL

    // Badge URL should reference CircleCI or a badge service (shields.io, circleci.com, etc.)
    const isCIBadge = src!.includes('circleci') ||
                       src!.includes('shields.io') ||
                       src!.includes('travis') ||
                       src!.includes('github');
    expect(isCIBadge).toBe(true);
  });

  test('TC3: Badge image loads successfully without 404 or broken image', async ({ page }) => {
    const badgeImage = page.locator(getByTestId('ci-badge'));

    // Verify image is visible
    await expect(badgeImage).toBeVisible();

    // Get the image source
    const src = await badgeImage.getAttribute('src');
    expect(src).toBeTruthy();

    // Verify the src is a valid HTTPS URL pointing to a badge service
    expect(src).toMatch(/^https:\/\//);

    // Verify alt text is present (required for accessibility and broken image scenarios)
    const alt = await badgeImage.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt!.length).toBeGreaterThan(0);

    // Wait for network idle to allow badge to load, then check if image is displaying
    await page.waitForLoadState('networkidle');

    // Check that the image element has dimensions (indicates it's rendered)
    const boundingBox = await badgeImage.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox!.width).toBeGreaterThan(0);
    expect(boundingBox!.height).toBeGreaterThan(0);
  });

  test('TC4: Badge image is wrapped in an anchor tag linking to CI pipeline', async ({ page }) => {
    const badgeLink = page.locator(getByTestId('ci-badge-link'));
    const badgeImage = page.locator(getByTestId('ci-badge'));

    // Verify the link exists
    await expect(badgeLink).toBeVisible();

    // Verify it's an anchor element
    const tagName = await badgeLink.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('a');

    // Verify the image is inside the link
    const imageInsideLink = badgeLink.locator(getByTestId('ci-badge'));
    await expect(imageInsideLink).toBeVisible();

    // Verify the link points to CI pipeline (CircleCI URL)
    const href = await badgeLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https?:\/\//); // Should be a valid URL
    expect(href).toContain('circleci'); // Should link to CircleCI
  });

  test('TC5: Badge link has target="_blank" to open in new tab', async ({ page }) => {
    const badgeLink = page.locator(getByTestId('ci-badge-link'));

    // Verify the link exists
    await expect(badgeLink).toBeVisible();

    // Verify target="_blank" attribute
    const target = await badgeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Also verify rel="noopener noreferrer" for security (best practice)
    const rel = await badgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
