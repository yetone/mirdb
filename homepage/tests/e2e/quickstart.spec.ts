/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Section
 *
 * Test cases:
 * - Installation instructions present
 * - Server start command provided
 * - Client connection example present
 * - Steps are sequential/numbered
 * - Commands are displayed in code blocks
 */

import { test, expect } from '@playwright/test';
import { navigateToHomepage, selectors, scrollToSection } from './test-utils';

test.describe('Quick Start Section', () => {
  test.beforeEach(async ({ page }) => {
    await navigateToHomepage(page);
    await scrollToSection(page, 'quickstart');
  });

  test('displays installation instructions', async ({ page }) => {
    // Check the Quick Start section exists
    const section = page.locator(selectors.quickstart.section);
    await expect(section).toBeVisible();

    // Check for the first step with clone/build instructions
    const steps = page.locator(selectors.quickstart.steps);
    const firstStep = steps.nth(0);
    await expect(firstStep).toBeVisible();

    // Verify installation instructions content
    const firstStepContent = await firstStep.textContent();
    expect(firstStepContent).toBeTruthy();

    // Should contain clone and build commands
    expect(firstStepContent).toContain('git clone');
    expect(firstStepContent).toContain('make build');
  });

  test('displays server start command', async ({ page }) => {
    const steps = page.locator(selectors.quickstart.steps);
    const secondStep = steps.nth(1);
    await expect(secondStep).toBeVisible();

    // Verify server start command content
    const secondStepContent = await secondStep.textContent();
    expect(secondStepContent).toBeTruthy();

    // Should contain the mirdb command with port
    expect(secondStepContent).toContain('./mirdb');
    expect(secondStepContent).toContain('--port');
  });

  test('displays client connection example', async ({ page }) => {
    const steps = page.locator(selectors.quickstart.steps);
    const thirdStep = steps.nth(2);
    await expect(thirdStep).toBeVisible();

    // Verify client connection example content
    const thirdStepContent = await thirdStep.textContent();
    expect(thirdStepContent).toBeTruthy();

    // Should contain telnet or client connection example
    expect(thirdStepContent).toContain('telnet');
    expect(thirdStepContent).toContain('localhost');
  });

  test('steps are sequential and numbered', async ({ page }) => {
    const stepNumbers = page.locator(selectors.quickstart.stepNumber);
    const count = await stepNumbers.count();

    // Should have at least 3 sequential steps
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify step numbers are sequential (1, 2, 3)
    for (let i = 0; i < count; i++) {
      const stepNumber = stepNumbers.nth(i);
      await expect(stepNumber).toBeVisible();
      const numberText = await stepNumber.textContent();
      expect(numberText?.trim()).toBe(String(i + 1));
    }
  });

  test('commands are displayed in code blocks', async ({ page }) => {
    const section = page.locator(selectors.quickstart.section);
    const codeBlocks = section.locator('.code-block');

    // Should have code blocks for each step
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThanOrEqual(3);

    // Each code block should contain pre and code elements
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      const preElement = codeBlock.locator('pre');
      await expect(preElement).toBeVisible();

      const codeElement = codeBlock.locator('code');
      await expect(codeElement).toBeVisible();
    }
  });

  test('Quick Start section has proper heading', async ({ page }) => {
    const title = page.locator(selectors.quickstart.title);
    await expect(title).toBeVisible();
    await expect(title).toHaveText('Quick Start');
  });

  test('Quick Start section is accessible via navigation', async ({ page }) => {
    // Go back to top
    await page.goto('/');

    // Click on Quick Start navigation link
    const navLink = page.locator('a.nav-link[href="#quickstart"]');
    await navLink.click();

    // Verify section is now visible
    const section = page.locator(selectors.quickstart.section);
    await expect(section).toBeInViewport();
  });
});
