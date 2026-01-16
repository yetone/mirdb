import { test, expect } from '@playwright/test';

test.describe('Social Proof Section - Display E2E Test', () => {
  test('social proof section is visible and properly styled', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Step 1: Navigate to social proof section by scrolling
    const section = page.locator('section#social-proof');
    await section.scrollIntoViewIfNeeded();
    await expect(section).toBeVisible();

    // Step 2: Verify section has proper structure
    const sectionTestId = page.getByTestId('social-proof-section');
    await expect(sectionTestId).toBeVisible();

    // Step 3: Verify heading is present
    const heading = page.locator('#social-proof-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText(/trusted|impact|numbers|proof/i);

    // Step 4: Verify statistics are displayed
    const statsContainer = page.getByTestId('stats-container');
    await expect(statsContainer).toBeVisible();

    // Verify individual stats
    const urlsStat = page.getByTestId('stat-urls-shortened');
    const clicksStat = page.getByTestId('stat-clicks-tracked');

    await expect(urlsStat).toBeVisible();
    await expect(urlsStat).toHaveText(/URLs shortened/i);

    await expect(clicksStat).toBeVisible();
    await expect(clicksStat).toHaveText(/clicks tracked/i);

    // Step 5: Verify trust indicators are displayed
    const trustContainer = page.getByTestId('trust-indicators-container');
    await expect(trustContainer).toBeVisible();

    // Check for at least one trust indicator
    const securityIndicator = page.getByTestId('trust-indicator-security');
    const uptimeIndicator = page.getByTestId('trust-indicator-uptime');
    const performanceIndicator = page.getByTestId('trust-indicator-performance');

    await expect(securityIndicator).toBeVisible();
    await expect(uptimeIndicator).toBeVisible();
    await expect(performanceIndicator).toBeVisible();

    // Verify trust indicator content
    await expect(securityIndicator).toHaveText(/secure|encrypted|ssl/i);
    await expect(uptimeIndicator).toHaveText(/uptime|99/i);
    await expect(performanceIndicator).toHaveText(/fast|response|ms/i);
  });

  test('social proof section displays all statistics cards', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    const section = page.locator('section#social-proof');
    await section.scrollIntoViewIfNeeded();

    // Verify all stat cards are present
    const statCards = page.locator('[data-testid^="stat-card-"]');
    await expect(statCards).toHaveCount(3);

    // Verify stat values are visible
    const statValues = page.locator('[data-testid^="stat-value-"]');
    await expect(statValues).toHaveCount(3);

    // Verify stat labels are visible
    const statLabels = page.locator('[data-testid^="stat-label-"]');
    await expect(statLabels).toHaveCount(3);
  });

  test('social proof section is responsive on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    const section = page.locator('section#social-proof');
    await section.scrollIntoViewIfNeeded();
    await expect(section).toBeVisible();

    // Verify section is visible on mobile
    const sectionTestId = page.getByTestId('social-proof-section');
    await expect(sectionTestId).toBeVisible();

    // Verify statistics are visible
    const statsContainer = page.getByTestId('stats-container');
    await expect(statsContainer).toBeVisible();

    // Verify trust indicators are visible
    const trustContainer = page.getByTestId('trust-indicators-container');
    await expect(trustContainer).toBeVisible();

    // On mobile, stats should stack vertically
    const statCards = page.locator('[data-testid^="stat-card-"]');
    const firstCard = statCards.nth(0);
    const secondCard = statCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();

    // On mobile with grid-cols-1, cards should be stacked vertically
    expect(secondBox!.y).toBeGreaterThan(firstBox!.y);
  });

  test('social proof section positioned between HowItWorks and FooterCTA', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Get bounding boxes for all relevant sections
    const howItWorks = page.locator('section#how-it-works');
    const socialProof = page.locator('section#social-proof');
    const footerCTA = page.locator('[data-testid="footer-cta"]');

    // Ensure all sections are in DOM
    await expect(howItWorks).toBeAttached();
    await expect(socialProof).toBeAttached();
    await expect(footerCTA).toBeAttached();

    const howItWorksBox = await howItWorks.boundingBox();
    const socialProofBox = await socialProof.boundingBox();
    const footerCTABox = await footerCTA.boundingBox();

    expect(howItWorksBox).not.toBeNull();
    expect(socialProofBox).not.toBeNull();
    expect(footerCTABox).not.toBeNull();

    // Social proof should be after HowItWorks
    expect(socialProofBox!.y).toBeGreaterThan(howItWorksBox!.y);

    // Social proof should be before FooterCTA
    expect(socialProofBox!.y).toBeLessThan(footerCTABox!.y);
  });
});
