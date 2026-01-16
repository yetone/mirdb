import { test, expect } from '@playwright/test';

test.describe('Social Proof Section - E2E Tests', () => {
  test('social proof section is visible and properly styled', async ({ page }) => {
    // Set desktop viewport BEFORE navigating
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    // Step 1: Navigate to social proof section (scroll to locate it)
    const section = page.locator('section#social-proof');
    await section.scrollIntoViewIfNeeded();
    await expect(section).toBeVisible();

    // Step 2: Verify statistics display (URLs shortened, clicks tracked)
    const urlsStat = page.getByTestId('stat-urls-shortened');
    const clicksStat = page.getByTestId('stat-clicks-tracked');

    await expect(urlsStat).toBeVisible();
    await expect(clicksStat).toBeVisible();

    // Verify stat values contain numbers
    const statValue1 = page.getByTestId('stat-value-1');
    const statValue2 = page.getByTestId('stat-value-2');

    await expect(statValue1).toBeVisible();
    await expect(statValue2).toBeVisible();

    const value1Text = await statValue1.textContent();
    const value2Text = await statValue2.textContent();

    expect(value1Text).toMatch(/\d/);
    expect(value2Text).toMatch(/\d/);

    // Step 3: Verify trust indicators (security, uptime)
    const securityIndicator = page.getByTestId('trust-indicator-security');
    const uptimeIndicator = page.getByTestId('trust-indicator-uptime');

    await expect(securityIndicator).toBeVisible();
    await expect(uptimeIndicator).toBeVisible();

    // Verify trust indicator icons are displayed
    const trustIcon1 = page.getByTestId('trust-icon-1');
    const trustIcon2 = page.getByTestId('trust-icon-2');

    await expect(trustIcon1).toBeVisible();
    await expect(trustIcon2).toBeVisible();

    // Verify section heading
    const heading = section.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText(/trusted/i);

    // Verify grid layout
    const grid = page.getByTestId('social-proof-grid');
    await expect(grid).toBeVisible();
    await expect(grid).toHaveClass(/grid/);
  });

  test('social proof section is properly positioned between features and how-it-works', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');

    // Get bounding boxes to verify order
    const featuresSection = page.locator('section#features');
    const socialProofSection = page.locator('section#social-proof');
    const howItWorksSection = page.locator('section#how-it-works');

    await expect(featuresSection).toBeVisible();
    await expect(socialProofSection).toBeVisible();
    await expect(howItWorksSection).toBeVisible();

    const featuresBox = await featuresSection.boundingBox();
    const socialProofBox = await socialProofSection.boundingBox();
    const howItWorksBox = await howItWorksSection.boundingBox();

    expect(featuresBox).not.toBeNull();
    expect(socialProofBox).not.toBeNull();
    expect(howItWorksBox).not.toBeNull();

    // Social proof should be below features and above how-it-works
    expect(socialProofBox!.y).toBeGreaterThan(featuresBox!.y);
    expect(howItWorksBox!.y).toBeGreaterThan(socialProofBox!.y);
  });

  test('social proof section displays correctly on mobile', async ({ page }) => {
    // Set mobile viewport BEFORE navigating
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const section = page.locator('section#social-proof');
    await section.scrollIntoViewIfNeeded();
    await expect(section).toBeVisible();

    // Verify all statistics and trust indicators are visible on mobile
    const statValue1 = page.getByTestId('stat-value-1');
    const statValue2 = page.getByTestId('stat-value-2');
    const trustIcon1 = page.getByTestId('trust-icon-1');
    const trustIcon2 = page.getByTestId('trust-icon-2');

    await expect(statValue1).toBeVisible();
    await expect(statValue2).toBeVisible();
    await expect(trustIcon1).toBeVisible();
    await expect(trustIcon2).toBeVisible();

    // Verify grid layout adapts to mobile (items should be stacked)
    const grid = page.getByTestId('social-proof-grid');
    await expect(grid).toBeVisible();
    await expect(grid).toHaveClass(/grid-cols-1/);
  });
});
