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

test.describe('Roadmap/TODO Section (Scenario 6)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('should have a roadmap section with appropriate heading', async ({ page }) => {
    // Test Case 1: Locate roadmap/TODO section by heading
    await scrollToSection(page, '#roadmap');

    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Check for heading with 'Roadmap', 'TODO', or 'Features in Development'
    const heading = roadmapSection.locator('h2');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    const validHeadings = ['roadmap', 'todo', 'features in development'];
    const hasValidHeading = validHeadings.some((term) =>
      headingText?.toLowerCase().includes(term)
    );
    expect(hasValidHeading).toBeTruthy();
  });

  test('should have visual indicators distinguishing completed from pending items', async ({
    page,
  }) => {
    // Test Case 2: Check for visual indicators of completion status
    await scrollToSection(page, '#roadmap');

    // Check completed items have checkmarks
    const completedItems = page.locator('.roadmap-item-completed');
    await expect(completedItems.first()).toBeVisible();

    const completedCount = await completedItems.count();
    expect(completedCount).toBeGreaterThan(0);

    // Check for checkmark indicators on completed items
    const checkmarks = page.locator('.roadmap-checkmark');
    const checkmarkCount = await checkmarks.count();
    expect(checkmarkCount).toBeGreaterThan(0);

    // Check in-progress items have different visual indicator
    const inProgressItems = page.locator('.roadmap-item-in-progress');
    const inProgressCount = await inProgressItems.count();
    expect(inProgressCount).toBeGreaterThan(0);

    // Verify visual distinction with data-status attribute
    const completedStatus = await completedItems.first().getAttribute('data-status');
    expect(completedStatus).toBe('completed');

    const inProgressStatus = await inProgressItems.first().getAttribute('data-status');
    expect(inProgressStatus).toBe('in-progress');
  });

  test('should show Raft feature as in-progress/planned', async ({ page }) => {
    // Test Case 3: Verify Raft feature is listed as in-progress
    await scrollToSection(page, '#roadmap');

    // Find item containing 'Raft'
    const raftItem = page.locator('.roadmap-item', { hasText: /raft/i });
    await expect(raftItem).toBeVisible();

    // Verify it has in-progress status
    const raftStatus = await raftItem.getAttribute('data-status');
    expect(raftStatus).toBe('in-progress');

    // Check for 'In Development' badge
    const badge = raftItem.locator('.roadmap-badge');
    await expect(badge).toBeVisible();

    const badgeText = await badge.textContent();
    expect(badgeText?.toLowerCase()).toContain('development');
  });

  test('should display completed features: tokio, memtable, compaction', async ({
    page,
  }) => {
    // Additional test: Verify all required completed features are present
    await scrollToSection(page, '#roadmap');

    // Check for tokio
    const tokioItem = page.locator('.roadmap-item-completed', { hasText: /tokio/i });
    await expect(tokioItem).toBeVisible();

    // Check for memtable
    const memtableItem = page.locator('.roadmap-item-completed', {
      hasText: /memtable/i,
    });
    await expect(memtableItem).toBeVisible();

    // Check for minor compaction
    const minorCompaction = page.locator('.roadmap-item-completed', {
      hasText: /minor.*compaction/i,
    });
    await expect(minorCompaction).toBeVisible();

    // Check for major compaction
    const majorCompaction = page.locator('.roadmap-item-completed', {
      hasText: /major.*compaction/i,
    });
    await expect(majorCompaction).toBeVisible();
  });

  test('should have proper ARIA labels for accessibility', async ({ page }) => {
    // Accessibility check for roadmap section
    await scrollToSection(page, '#roadmap');

    const roadmapSection = page.locator('#roadmap');
    const ariaLabelledBy = await roadmapSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('roadmap-heading');

    // Check that lists have aria-label
    const completedList = page.locator('.roadmap-list[aria-label*="Completed"]');
    await expect(completedList).toBeVisible();

    const progressList = page.locator('.roadmap-list[aria-label*="progress"]');
    await expect(progressList).toBeVisible();
  });
});
