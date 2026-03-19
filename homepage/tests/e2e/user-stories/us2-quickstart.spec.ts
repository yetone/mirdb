/**
 * E2E Tests for US-2: Getting Started Quickly
 * Owner: Scenario 18 - User Story - Getting Started Quickly
 *
 * Verifies that Morgan (Developer implementing solution) can find and copy
 * installation commands to try MirDB immediately.
 *
 * Test Cases:
 * 1. Quick Start section is easily findable via navigation or scrolling
 * 2. Step-by-step commands for clone, cd, and cargo run are visible
 * 3. Commands can be copied to clipboard with visual confirmation
 * 4. Memcached protocol connection example is displayed
 *
 * Requirements: REQ-6, REQ-7
 */

import { test, expect } from '@playwright/test';

test.describe('US-2: Getting Started Quickly', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC-1: Quick Start section is easily findable via navigation or scrolling', async ({ page }) => {
    // Verify navigation contains Quick Start link
    const navLink = page.locator('nav a[href="#quick-start"], header a[href="#quick-start"]');
    await expect(navLink.first()).toBeVisible();
    await expect(navLink.first()).toContainText('Quick Start');

    // Click navigation link to scroll to Quick Start section
    await navLink.first().click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify Quick Start section is now visible in viewport
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify section heading is present
    const sectionHeading = quickStartSection.locator('h2');
    await expect(sectionHeading).toContainText('Quick Start');
    await expect(sectionHeading).toBeVisible();

    // Verify section is visible in the viewport by checking if it's intersecting
    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return (
        rect.top >= 0 &&
        rect.top < window.innerHeight
      );
    });
    expect(isInViewport).toBe(true);
  });

  test('TC-2: Step-by-step commands for clone, cd, and cargo run are visible', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Verify installation section exists
    const installationSection = quickStartSection.locator('[data-testid="installation-section"]');
    await expect(installationSection).toBeVisible();

    // Verify Step 1: Clone command
    const stepClone = quickStartSection.locator('[data-testid="step-clone"]');
    await expect(stepClone).toBeVisible();
    const cloneCodeBlock = stepClone.locator('[data-testid="code-block"]');
    await expect(cloneCodeBlock).toBeVisible();
    const cloneContent = await cloneCodeBlock.locator('[data-testid="code-content"]').textContent();
    expect(cloneContent).toContain('git clone');
    expect(cloneContent).toContain('github.com/yetone/mirdb');

    // Verify Step 2: CD command
    const stepCd = quickStartSection.locator('[data-testid="step-cd"]');
    await expect(stepCd).toBeVisible();
    const cdCodeBlock = stepCd.locator('[data-testid="code-block"]');
    await expect(cdCodeBlock).toBeVisible();
    const cdContent = await cdCodeBlock.locator('[data-testid="code-content"]').textContent();
    expect(cdContent).toContain('cd mirdb');

    // Verify Step 3: Cargo run command
    const stepRun = quickStartSection.locator('[data-testid="step-run"]');
    await expect(stepRun).toBeVisible();
    const runCodeBlock = stepRun.locator('[data-testid="code-block"]');
    await expect(runCodeBlock).toBeVisible();
    const runContent = await runCodeBlock.locator('[data-testid="code-content"]').textContent();
    expect(runContent).toContain('cargo run');
    expect(runContent).toContain('--release');

    // Verify commands are sequential (step numbers visible)
    const stepLabels = await installationSection.locator('p').allTextContents();
    const stepTexts = stepLabels.filter(text => text.match(/^\d+\./));
    expect(stepTexts.length).toBeGreaterThanOrEqual(3);
  });

  test('TC-3: Commands successfully copied to clipboard with visual confirmation', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Find the first code block copy button
    const stepClone = quickStartSection.locator('[data-testid="step-clone"]');
    const copyButton = stepClone.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Verify initial state shows "Copy" text
    await expect(copyButton).toContainText('Copy');

    // Click copy button
    await copyButton.click();

    // Verify visual confirmation - button should show "Copied!"
    await expect(copyButton).toContainText('Copied!');

    // Verify the button has success styling (green color)
    const buttonClasses = await copyButton.getAttribute('class');
    expect(buttonClasses).toContain('text-green');

    // Verify clipboard contains the expected content (git clone command)
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('git clone');
    expect(clipboardContent).toContain('github.com/yetone/mirdb');

    // Test another code block to ensure all copy buttons work
    const stepRun = quickStartSection.locator('[data-testid="step-run"]');
    const runCopyButton = stepRun.locator('[data-testid="copy-button"]');
    await runCopyButton.click();

    // Verify second button shows "Copied!"
    await expect(runCopyButton).toContainText('Copied!');

    // Verify clipboard now contains cargo run command
    const newClipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(newClipboardContent).toContain('cargo run');
    expect(newClipboardContent).toContain('--release');
  });

  test('TC-4: Memcached protocol connection example is displayed', async ({ page }) => {
    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Verify usage section exists with memcached example
    const usageSection = quickStartSection.locator('[data-testid="usage-section"]');
    await expect(usageSection).toBeVisible();

    // Verify section heading mentions memcached protocol
    const usageHeading = usageSection.locator('h3');
    await expect(usageHeading).toContainText('Memcached Protocol');
    await expect(usageHeading).toBeVisible();

    // Verify connection example code block is present
    const codeBlock = usageSection.locator('[data-testid="code-block"]');
    await expect(codeBlock).toBeVisible();

    // Verify code content includes memcached commands
    const codeContent = await codeBlock.locator('[data-testid="code-content"]').textContent();
    expect(codeContent).toContain('telnet localhost 11211');
    expect(codeContent).toContain('set');
    expect(codeContent).toContain('get');

    // Verify the code block has copy functionality
    const copyButton = codeBlock.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toContainText('Copy');
  });

  test('Complete flow: Morgan can get started with MirDB quickly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Step 1: User lands on homepage
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Step 2: User clicks "Get Started" CTA or navigates to Quick Start
    const getStartedButton = page.locator('text=Get Started').first();
    await expect(getStartedButton).toBeVisible();
    await getStartedButton.click();

    // Step 3: Quick Start section is now in view
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Step 4: User sees prerequisites
    const prerequisitesSection = quickStartSection.locator('[data-testid="prerequisites-section"]');
    await expect(prerequisitesSection).toBeVisible();
    const prerequisiteItems = prerequisitesSection.locator('[data-testid="prerequisite-item"]');
    await expect(prerequisiteItems.first()).toBeVisible();

    // Step 5: User copies the clone command
    const stepClone = quickStartSection.locator('[data-testid="step-clone"]');
    const cloneCopyButton = stepClone.locator('[data-testid="copy-button"]');
    await cloneCopyButton.click();
    await expect(cloneCopyButton).toContainText('Copied!');

    // Verify copied content
    let clipboard = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboard).toContain('git clone');

    // Step 6: User copies the cd command
    const stepCd = quickStartSection.locator('[data-testid="step-cd"]');
    const cdCopyButton = stepCd.locator('[data-testid="copy-button"]');
    await cdCopyButton.click();
    await expect(cdCopyButton).toContainText('Copied!');

    clipboard = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboard).toContain('cd mirdb');

    // Step 7: User copies the run command
    const stepRun = quickStartSection.locator('[data-testid="step-run"]');
    const runCopyButton = stepRun.locator('[data-testid="copy-button"]');
    await runCopyButton.click();
    await expect(runCopyButton).toContainText('Copied!');

    clipboard = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboard).toContain('cargo run');

    // Step 8: User finds the connection example
    const usageSection = quickStartSection.locator('[data-testid="usage-section"]');
    await expect(usageSection).toBeVisible();
    const usageCodeContent = await usageSection.locator('[data-testid="code-content"]').textContent();
    expect(usageCodeContent).toContain('telnet');
    expect(usageCodeContent).toContain('set');
    expect(usageCodeContent).toContain('get');

    // All information needed to get started is accessible
  });
});
