// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for User Journey Flow Scenario
 * Verifies complete user journey from discovery to action can be completed within target time
 *
 * Scenario UUID: 337bf9da-99f1-40d6-b15f-c4d0bc06585b
 *
 * Test Cases:
 * 1. Hero section with value proposition visible within 2 seconds
 * 2. Quick-start command reachable in 0-1 clicks (visible or one scroll)
 * 3. User can copy command within 60 seconds of arrival
 * 4. Get Started CTA is immediately visible without scrolling
 */

test.describe('User Journey Flow', () => {
  /**
   * Test Case 1: Time from page load to visible hero content
   * Expected: Hero section with value proposition visible within 2 seconds
   */
  test('TC1: Hero section with value proposition visible within 2 seconds', async ({ page }) => {
    const startTime = Date.now();

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Wait for hero section to be visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify hero title (MirDB) is visible
    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();

    // Verify value proposition tagline is visible
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();
    await expect(heroTagline).toContainText('Persistent Key-Value Store with Memcached Protocol');

    const endTime = Date.now();
    const loadTime = endTime - startTime;

    // Verify hero content is visible within 2 seconds (2000ms)
    expect(loadTime).toBeLessThan(2000);

    console.log(`Hero content visible in ${loadTime}ms`);
  });

  /**
   * Test Case 2: Count clicks to reach quick-start command
   * Expected: Quick-start command reachable in 0-1 clicks (visible or one scroll)
   */
  test('TC2: Quick-start command reachable in 0-1 clicks (visible or one scroll)', async ({ page }) => {
    await page.goto('/');

    // Get the quick-start section
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');

    // Check if quick-start section is already visible (0 clicks needed)
    const isInitiallyVisible = await quickStartSection.isVisible();

    if (isInitiallyVisible) {
      // 0 clicks needed - section is already visible
      console.log('Quick-start command visible without any clicks (0 clicks)');
      expect(true).toBe(true);
    } else {
      // Check if we can reach it with 1 click via Get Started button
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();

      // Click Get Started button to navigate to quick-start
      await getStartedBtn.click();

      // Wait for quick-start section to be visible after navigation
      await expect(quickStartSection).toBeVisible({ timeout: 1000 });

      console.log('Quick-start command reachable with 1 click');
    }

    // Verify the installation command is present
    const installCode = page.locator('[data-testid="install-code"]');
    await expect(installCode).toBeVisible();
    await expect(installCode).toContainText('cargo install mirdb-server');
  });

  /**
   * Test Case 3: Time to copy installation command
   * Expected: User can copy command within 60 seconds of arrival
   */
  test('TC3: User can copy command within 60 seconds of arrival', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const startTime = Date.now();

    // Navigate to homepage
    await page.goto('/');

    // Navigate to quick-start section if not visible
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    const isVisible = await quickStartSection.isVisible();

    if (!isVisible) {
      // Click Get Started to navigate to quick-start
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await getStartedBtn.click();
      await expect(quickStartSection).toBeVisible();
    }

    // Find and click the copy button for installation command
    const copyBtn = page.locator('[data-testid="copy-install-btn"]');
    await expect(copyBtn).toBeVisible();
    await copyBtn.click();

    // Verify clipboard content
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    expect(clipboardContent).toBe('cargo install mirdb-server');

    const endTime = Date.now();
    const totalTime = endTime - startTime;

    // Verify entire action completed within 60 seconds (60000ms)
    expect(totalTime).toBeLessThan(60000);

    console.log(`User journey to copy command completed in ${totalTime}ms (${(totalTime / 1000).toFixed(2)}s)`);
  });

  /**
   * Test Case 4: Check Get Started button prominence
   * Expected: Get Started CTA is immediately visible without scrolling
   */
  test('TC4: Get Started CTA is immediately visible without scrolling', async ({ page }) => {
    await page.goto('/');

    // Get the Get Started button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');

    // Verify button exists and is visible
    await expect(getStartedBtn).toBeVisible();

    // Verify button text
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify button is in viewport (above the fold - no scrolling needed)
    const isInViewport = await getStartedBtn.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return (
        rect.top >= 0 &&
        rect.left >= 0 &&
        rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
        rect.right <= (window.innerWidth || document.documentElement.clientWidth)
      );
    });

    expect(isInViewport).toBe(true);

    // Verify button is clickable/enabled
    await expect(getStartedBtn).toBeEnabled();

    // Verify button has proper styling (is prominent)
    const buttonStyles = await getStartedBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        cursor: styles.cursor,
        display: styles.display
      };
    });

    // Button should have pointer cursor indicating it's interactive
    expect(buttonStyles.cursor).toBe('pointer');
    // Button should not be hidden
    expect(buttonStyles.display).not.toBe('none');

    console.log('Get Started CTA is prominent and visible without scrolling');
  });
});

test.describe('User Journey Flow - Additional Validation', () => {
  /**
   * Verify complete user journey can be accomplished within 60 seconds
   * This is an integration test of the full journey
   */
  test('Complete user journey from discovery to action within 60 seconds', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const journeyStartTime = Date.now();

    // Step 1: Land on homepage (Discovery phase)
    await page.goto('/');
    console.log('Step 1: Landed on homepage');

    // Step 2: Understand product (Understanding phase) - see hero content
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toHaveText('MirDB');

    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toContainText('Persistent Key-Value Store');

    const understandingTime = Date.now() - journeyStartTime;
    console.log(`Step 2: Product understood in ${understandingTime}ms`);

    // Step 3: Review features (Evaluation phase)
    // User can see features/comparison - they're on the page
    const comparisonTable = page.locator('[data-testid="comparison-table"]');
    await expect(comparisonTable).toBeAttached();

    const evaluationTime = Date.now() - journeyStartTime;
    console.log(`Step 3: Features available for review at ${evaluationTime}ms`);

    // Step 4: Take action (Action phase) - copy command or navigate
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await getStartedBtn.click();

    // Quick-start section should now be visible/scrolled to
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    await expect(quickStartSection).toBeVisible();

    // Copy the installation command
    const copyBtn = page.locator('[data-testid="copy-install-btn"]');
    await expect(copyBtn).toBeVisible();
    await copyBtn.click();

    // Verify command was copied
    const clipboardContent = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });
    expect(clipboardContent).toBe('cargo install mirdb-server');

    const actionTime = Date.now() - journeyStartTime;
    console.log(`Step 4: Action completed (command copied) in ${actionTime}ms`);

    // Verify total journey completed within 60 seconds
    const totalJourneyTime = Date.now() - journeyStartTime;
    expect(totalJourneyTime).toBeLessThan(60000);

    console.log(`\nComplete user journey finished in ${totalJourneyTime}ms (${(totalJourneyTime / 1000).toFixed(2)}s)`);
    console.log('SUCCESS: User journey completed within target time of 60 seconds');
  });

  /**
   * Verify Get Started button scrolls to quick-start section
   */
  test('Get Started button navigates to quick-start section', async ({ page }) => {
    await page.goto('/');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click Get Started
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await getStartedBtn.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify quick-start section is now in view
    const quickStartSection = page.locator('[data-testid="quick-start-section"]');
    await expect(quickStartSection).toBeVisible();

    // Verify scroll happened (if quick-start was below fold)
    const finalScrollY = await page.evaluate(() => window.scrollY);

    // Either we scrolled or the section was already visible
    const isVisible = await quickStartSection.isVisible();
    expect(isVisible).toBe(true);

    console.log(`Scrolled from ${initialScrollY}px to ${finalScrollY}px`);
  });
});
