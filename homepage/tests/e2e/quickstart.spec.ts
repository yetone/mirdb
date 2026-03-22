/**
 * Quick Start Section E2E Tests
 * Owners: Scenario 5 (Quick Start), Scenario 6 (Roadmap)
 *
 * Test groups:
 * - Quick start section presence
 * - 3-step guide verification
 * - Code blocks with commands
 * - Roadmap/TODO section
 * - Feature completion status indicators
 */

import { test, expect } from '@playwright/test';
import { waitForLoad, scrollToSection } from './utils';

test.describe('Quick Start Guide (Scenario 5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: Quick start section exists with Get Started heading', async ({ page }) => {
    // Test Case 1: Locate quick start section by heading or identifier
    // Expected: Section with 'Get Started' or 'Quick Start' heading exists

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for the heading
    const heading = quickstartSection.locator('h2');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    expect(headingText).toBeTruthy();

    // Verify heading contains 'Get Started' or 'Quick Start'
    const hasCorrectHeading =
      headingText!.toLowerCase().includes('get started') ||
      headingText!.toLowerCase().includes('quick start');
    expect(hasCorrectHeading).toBe(true);
  });

  test('TC2: Exactly 3 steps are present in the quick start guide', async ({ page }) => {
    // Test Case 2: Count numbered or bulleted steps in quick start section
    // Expected: Exactly 3 steps are present in the guide

    const quickstartSection = page.locator('#quickstart');
    await scrollToSection(page, '#quickstart');

    // Find step items (looking for elements with step class or numbered list items)
    const stepItems = quickstartSection.locator('.quickstart-step');
    const stepCount = await stepItems.count();

    expect(stepCount).toBe(3);

    // Verify each step has a number indicator
    for (let i = 0; i < stepCount; i++) {
      const step = stepItems.nth(i);
      await expect(step).toBeVisible();

      // Check step has a number
      const stepNumber = step.locator('.step-number');
      const numberText = await stepNumber.textContent();
      expect(numberText).toContain(String(i + 1));
    }
  });

  test('TC3: At least one code block with copyable commands exists', async ({ page }) => {
    // Test Case 3: Check for code blocks within quick start section
    // Expected: At least one code block with copyable installation/run command

    const quickstartSection = page.locator('#quickstart');
    await scrollToSection(page, '#quickstart');

    // Find code blocks (pre or code elements)
    const codeBlocks = quickstartSection.locator('pre, code.command');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount).toBeGreaterThanOrEqual(1);

    // Verify at least one code block contains actual command text
    let hasCommand = false;
    for (let i = 0; i < codeBlockCount; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText && codeText.trim().length > 0) {
        // Check if it looks like a command (contains typical command patterns)
        if (
          codeText.includes('cargo') ||
          codeText.includes('git clone') ||
          codeText.includes('telnet') ||
          codeText.includes('npm') ||
          codeText.includes('curl') ||
          codeText.includes('./') ||
          codeText.includes('make')
        ) {
          hasCommand = true;
          break;
        }
      }
    }

    expect(hasCommand).toBe(true);
  });

  test('TC4: Steps follow logical order - install, configure/run, use', async ({ page }) => {
    // Test Case 4: Verify step sequence logic
    // Expected: Steps follow logical order: install -> configure/run -> use

    const quickstartSection = page.locator('#quickstart');
    await scrollToSection(page, '#quickstart');

    const stepItems = quickstartSection.locator('.quickstart-step');
    const stepCount = await stepItems.count();
    expect(stepCount).toBe(3);

    // Get step titles or descriptions
    const stepTitles: string[] = [];
    for (let i = 0; i < stepCount; i++) {
      const stepTitle = stepItems.nth(i).locator('.step-title, h3');
      const titleText = await stepTitle.textContent();
      stepTitles.push(titleText?.toLowerCase() || '');
    }

    // Step 1 should be about installing/cloning/building
    const step1Keywords = ['install', 'clone', 'download', 'build', 'get'];
    const hasInstallStep = step1Keywords.some(keyword => stepTitles[0].includes(keyword));
    expect(hasInstallStep).toBe(true);

    // Step 2 should be about running/starting/configuring
    const step2Keywords = ['run', 'start', 'configure', 'launch', 'build'];
    const hasRunStep = step2Keywords.some(keyword => stepTitles[1].includes(keyword));
    expect(hasRunStep).toBe(true);

    // Step 3 should be about using/connecting/testing
    const step3Keywords = ['use', 'connect', 'test', 'try', 'interact'];
    const hasUseStep = step3Keywords.some(keyword => stepTitles[2].includes(keyword));
    expect(hasUseStep).toBe(true);
  });

  test('Quick start section has proper accessibility attributes', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await scrollToSection(page, '#quickstart');

    // Check section has aria-labelledby pointing to heading
    const ariaLabelledBy = await quickstartSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBeTruthy();

    // Verify the heading exists
    const heading = page.locator(`#${ariaLabelledBy}`);
    await expect(heading).toBeVisible();
  });

  test('Code blocks have proper styling for readability', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await scrollToSection(page, '#quickstart');

    const codeBlock = quickstartSection.locator('pre').first();
    await expect(codeBlock).toBeVisible();

    // Check code block has distinguishing background
    const backgroundColor = await codeBlock.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should have a non-transparent background
    expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(backgroundColor).not.toBe('transparent');
  });

  test('Steps are visually distinct with step numbers', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await scrollToSection(page, '#quickstart');

    const stepNumbers = quickstartSection.locator('.step-number');
    const count = await stepNumbers.count();

    expect(count).toBe(3);

    // Verify each step number is visible
    for (let i = 0; i < count; i++) {
      await expect(stepNumbers.nth(i)).toBeVisible();
    }
  });
});
