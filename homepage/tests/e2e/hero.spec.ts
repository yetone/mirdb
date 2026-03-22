/**
 * Hero Section E2E Tests
 * Owners: Scenario 1 (Hero), Scenario 2 (Badges), Scenario 18 (Theme)
 */
import { test, expect } from '@playwright/test';

test.describe('Status Badges Display (Scenario 2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC1: At least one status badge is present on the page', async ({ page }) => {
    // Find the badges section
    const badgesSection = page.locator('#badges');
    await expect(badgesSection).toBeVisible();

    // Find all badge images/elements
    const badgeImages = badgesSection.locator('img');
    const badgeCount = await badgeImages.count();

    // Verify at least one badge is present
    expect(badgeCount).toBeGreaterThanOrEqual(1);

    // Verify badges are visible
    const firstBadge = badgeImages.first();
    await expect(firstBadge).toBeVisible();
  });

  test('TC2: Badge links to CircleCI or relevant CI/CD service', async ({ page }) => {
    // Find the badges section
    const badgesSection = page.locator('#badges');

    // Extract hrefs from badge parent anchor elements
    const badgeLinks = badgesSection.locator('a');
    const linkCount = await badgeLinks.count();

    // Verify at least one link exists
    expect(linkCount).toBeGreaterThanOrEqual(1);

    // Check that at least one badge links to CircleCI
    const hrefs: string[] = [];
    for (let i = 0; i < linkCount; i++) {
      const href = await badgeLinks.nth(i).getAttribute('href');
      if (href) {
        hrefs.push(href);
      }
    }

    // Verify CircleCI link is present
    const hasCircleCILink = hrefs.some(href =>
      href.includes('circleci.com') || href.includes('github.com')
    );
    expect(hasCircleCILink).toBe(true);

    // Verify the specific CircleCI pipeline link
    const circleCILink = hrefs.find(href => href.includes('circleci.com/gh/yetone/mirdb'));
    expect(circleCILink).toBeDefined();
  });

  test('TC3: Badge images have valid src attributes', async ({ page }) => {
    // Find badge images
    const badgesSection = page.locator('#badges');
    const badgeImages = badgesSection.locator('img');
    const imageCount = await badgeImages.count();

    expect(imageCount).toBeGreaterThanOrEqual(1);

    // Verify each badge image has a valid src
    for (let i = 0; i < imageCount; i++) {
      const src = await badgeImages.nth(i).getAttribute('src');
      expect(src).toBeTruthy();
      expect(src).toMatch(/^https?:\/\//); // Should be a URL
    }
  });

  test('TC3-integration: Badge image loads successfully with 200 status', async ({ page, request }) => {
    // Find badge images
    const badgesSection = page.locator('#badges');
    const badgeImages = badgesSection.locator('img');
    const imageCount = await badgeImages.count();

    expect(imageCount).toBeGreaterThanOrEqual(1);

    // Find a reliable badge to test (license badge from shields.io is always reliable)
    let testedSuccessfully = false;
    for (let i = 0; i < imageCount; i++) {
      const src = await badgeImages.nth(i).getAttribute('src');
      if (src && src.includes('img.shields.io/badge/license')) {
        // Test the license badge which is a static badge and always returns 200
        const response = await request.get(src);
        expect(response.status()).toBe(200);

        // Verify the response is an image
        const contentType = response.headers()['content-type'];
        expect(contentType).toMatch(/image\//);
        testedSuccessfully = true;
        break;
      }
    }

    // Ensure we tested at least one badge
    expect(testedSuccessfully).toBe(true);
  });

  test('Badges section has proper accessibility attributes', async ({ page }) => {
    const badgesSection = page.locator('#badges');

    // Check for aria-label on section
    const ariaLabel = await badgesSection.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();

    // Check that badge links have aria-labels
    const badgeLinks = badgesSection.locator('a');
    const linkCount = await badgeLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const linkAriaLabel = await badgeLinks.nth(i).getAttribute('aria-label');
      expect(linkAriaLabel).toBeTruthy();
    }

    // Check that badge images have alt text
    const badgeImages = badgesSection.locator('img');
    const imageCount = await badgeImages.count();

    for (let i = 0; i < imageCount; i++) {
      const alt = await badgeImages.nth(i).getAttribute('alt');
      expect(alt).toBeTruthy();
    }
  });

  test('Badge links open in new tab with security attributes', async ({ page }) => {
    const badgesSection = page.locator('#badges');
    const badgeLinks = badgesSection.locator('a');
    const linkCount = await badgeLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = badgeLinks.nth(i);

      // Check target="_blank"
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      // Check rel="noopener noreferrer"
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });
});
