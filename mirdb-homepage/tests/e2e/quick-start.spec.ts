/**
 * Quick Start Section E2E Tests.
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests:
 * - Git clone command
 * - Build/run commands
 * - GitHub link presence and navigation
 * - Minimum 3 setup steps
 */

import { test, expect } from '@playwright/test';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have git clone command in a code block', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Check for git clone command
    const codeBlocks = quickStartSection.locator('pre code');
    const firstCodeBlock = codeBlocks.first();
    const codeText = await firstCodeBlock.textContent();

    expect(codeText).toContain('git clone');
    expect(codeText).toContain('github.com/yetone/mirdb');
  });

  test('should have cargo build or cargo run commands', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Get all code blocks text
    const codeBlocks = quickStartSection.locator('pre code');
    const count = await codeBlocks.count();

    let hasCargoCommand = false;
    for (let i = 0; i < count; i++) {
      const text = await codeBlocks.nth(i).textContent();
      if (text && (text.includes('cargo build') || text.includes('cargo run'))) {
        hasCargoCommand = true;
        break;
      }
    }

    expect(hasCargoCommand).toBe(true);
  });

  test('should have GitHub repository link', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Check for GitHub link
    const githubLink = quickStartSection.locator('a[href*="github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
  });

  test('should have external link attributes on GitHub link', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    const githubLink = quickStartSection.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    // Check that it opens in a new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Check for security attributes
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('should display at least 3 distinct setup steps', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Count the number of steps
    const steps = quickStartSection.locator('[data-testid^="quickstart-step-"]');
    const stepCount = await steps.count();

    expect(stepCount).toBeGreaterThanOrEqual(3);
  });

  test('should have proper section heading', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Check for section heading
    const heading = quickStartSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/quick start/i);
  });

  test('should have numbered steps with titles', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');

    // Check step 1 - Clone
    const step1 = quickStartSection.locator('[data-testid="quickstart-step-1"]');
    await expect(step1).toBeVisible();
    await expect(step1).toContainText(/clone/i);

    // Check step 2 - Configure
    const step2 = quickStartSection.locator('[data-testid="quickstart-step-2"]');
    await expect(step2).toBeVisible();
    await expect(step2).toContainText(/config/i);

    // Check step 3 - Run
    const step3 = quickStartSection.locator('[data-testid="quickstart-step-3"]');
    await expect(step3).toBeVisible();
    await expect(step3).toContainText(/run/i);
  });
});
