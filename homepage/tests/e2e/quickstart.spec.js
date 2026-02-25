/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 5 - Quick Start Three-Step Guide
 *
 * Test cases:
 * - Three numbered steps displayed
 * - Step 1 contains installation info
 * - Step 2 shows server start command
 * - Step 3 shows client connection
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Exactly 3 numbered steps are displayed', async ({ page }) => {
    // Navigate to the Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify there are exactly 3 steps
    const steps = quickstartSection.locator('.step');
    await expect(steps).toHaveCount(3);

    // Verify steps are numbered 1, 2, 3
    const stepNumbers = quickstartSection.locator('.step__number');
    await expect(stepNumbers).toHaveCount(3);
    await expect(stepNumbers.nth(0)).toHaveText('1');
    await expect(stepNumbers.nth(1)).toHaveText('2');
    await expect(stepNumbers.nth(2)).toHaveText('3');
  });

  test('TC2: Step 1 contains installation instructions', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const step1 = quickstartSection.locator('.step').first();

    // Verify step 1 is visible
    await expect(step1).toBeVisible();

    // Check for installation-related content (download, build, cargo install)
    const step1Text = await step1.textContent();
    const hasInstallContent =
      step1Text.toLowerCase().includes('install') ||
      step1Text.toLowerCase().includes('download') ||
      step1Text.toLowerCase().includes('build') ||
      step1Text.toLowerCase().includes('cargo');

    expect(hasInstallContent).toBe(true);

    // Verify there's a code block with installation command
    const codeBlock = step1.locator('code, pre');
    await expect(codeBlock.first()).toBeVisible();
  });

  test('TC3: Step 2 contains server start command with port 12333', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const steps = quickstartSection.locator('.step');
    const step2 = steps.nth(1);

    // Verify step 2 is visible
    await expect(step2).toBeVisible();

    // Check for server start content
    const step2Text = await step2.textContent();
    const hasServerContent =
      step2Text.toLowerCase().includes('start') ||
      step2Text.toLowerCase().includes('run') ||
      step2Text.toLowerCase().includes('server');

    expect(hasServerContent).toBe(true);

    // Verify port 12333 is mentioned
    expect(step2Text).toContain('12333');
  });

  test('TC4: Step 3 contains client connection command', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const steps = quickstartSection.locator('.step');
    const step3 = steps.nth(2);

    // Verify step 3 is visible
    await expect(step3).toBeVisible();

    // Check for client connection content (memcat, telnet, or memcached client)
    const step3Text = await step3.textContent();
    const hasClientContent =
      step3Text.toLowerCase().includes('connect') ||
      step3Text.toLowerCase().includes('memcat') ||
      step3Text.toLowerCase().includes('telnet') ||
      step3Text.toLowerCase().includes('client');

    expect(hasClientContent).toBe(true);

    // Verify there's a code block with connection command
    const codeBlock = step3.locator('code, pre');
    await expect(codeBlock.first()).toBeVisible();
  });

  test('TC5: Default port 12333 and work directory /tmp/mirdb are shown', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get all text content from the quick start section
    const sectionText = await quickstartSection.textContent();

    // Verify default port 12333 is displayed
    expect(sectionText).toContain('12333');

    // Verify work directory /tmp/mirdb is displayed
    expect(sectionText).toContain('/tmp/mirdb');
  });

  test('Quick Start section has proper heading', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    const heading = quickstartSection.locator('h2');

    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Quick Start');
  });

  test('Quick Start section is accessible via anchor link', async ({ page }) => {
    // Click on the Get Started button which should navigate to #quickstart
    const getStartedBtn = page.locator('a[href="#quickstart"]').first();
    await getStartedBtn.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the quickstart section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });
});
