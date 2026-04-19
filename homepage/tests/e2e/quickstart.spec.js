/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start and Installation Section
 *
 * Tests for:
 * - Installation steps list visibility
 * - Pre-built binary download mention
 * - Cargo install command
 * - Run command (mirdb-server)
 */

import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC-1: should display installation steps list with at least 3 steps', async ({ page }) => {
    // Navigate to quick start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for installation steps list
    const stepsList = page.locator('.quickstart__steps');
    await expect(stepsList).toBeVisible();

    // Verify at least 3 installation steps are visible
    const steps = page.locator('.quickstart__step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(3);

    // Verify steps are numbered (check data-step attributes or CSS counters)
    for (let i = 0; i < Math.min(stepCount, 3); i++) {
      await expect(steps.nth(i)).toBeVisible();
    }
  });

  test('TC-2: should mention pre-built binary download option', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for pre-built binary mention
    const binaryText = quickstartSection.getByText(/pre-built binary|releases/i);
    await expect(binaryText.first()).toBeVisible();

    // Check for GitHub releases link
    const releasesLink = page.locator('a[href*="releases"]');
    await expect(releasesLink.first()).toBeVisible();
  });

  test('TC-3: should display cargo install command', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for cargo install command in code block
    const cargoCommand = quickstartSection.getByText(/cargo install/i);
    await expect(cargoCommand.first()).toBeVisible();

    // Verify it's in a code block or command element
    const codeBlocks = page.locator('.quickstart__command code, pre code');
    const commandTexts = await codeBlocks.allTextContents();
    const hasCargoInstall = commandTexts.some(text => text.includes('cargo install'));
    expect(hasCargoInstall).toBe(true);
  });

  test('TC-4: should display run command for mirdb-server', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for mirdb-server run command
    const serverCommand = quickstartSection.getByText(/mirdb-server/i);
    await expect(serverCommand.first()).toBeVisible();

    // Verify the command is in a code block
    const codeBlocks = page.locator('.quickstart__command code, pre code');
    const commandTexts = await codeBlocks.allTextContents();
    const hasMirdbServer = commandTexts.some(text => text.includes('mirdb-server'));
    expect(hasMirdbServer).toBe(true);
  });

  test('should have proper section heading', async ({ page }) => {
    const heading = page.locator('#quickstart-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText(/Quick Start/i);
  });

  test('should have accessible step list structure', async ({ page }) => {
    // Check for proper ARIA attributes
    const stepsList = page.locator('.quickstart__steps[role="list"]');
    await expect(stepsList).toBeVisible();

    // Each step should be visible and styled
    const steps = page.locator('.quickstart__step');
    const count = await steps.count();
    expect(count).toBeGreaterThan(0);

    // First step should be about downloading binary
    const firstStep = steps.first();
    await expect(firstStep).toContainText(/download|binary/i);
  });

  test('should link to GitHub repository', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for GitHub link in the next steps section
    const githubLink = quickstartSection.locator('a[href*="github.com"]');
    await expect(githubLink.first()).toBeVisible();
  });
});
