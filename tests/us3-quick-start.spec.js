const { test, expect } = require('@playwright/test');

/**
 * User Story 3 (US-3) - Quick Start Validation Tests
 *
 * As a New Developer, I want to see how to get started with MirDB,
 * so that I can begin testing it quickly.
 *
 * Acceptance Criteria:
 * - Given I am on the homepage
 * - When I scroll to the Quick Start section
 * - Then I see clear installation instructions
 * - And I see example commands I can copy and run
 */

test.describe('US-3: Quick Start Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Quick start section contains clear installation instructions', async ({ page }) => {
    // Step 1: Navigate to quick start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Step 2: Verify section header exists and is clear
    const sectionHeader = quickstartSection.locator('h2');
    await expect(sectionHeader).toBeVisible();
    await expect(sectionHeader).toHaveText('Quick Start');

    // Step 3: Verify subheadline provides context
    const subheadline = quickstartSection.locator('.quickstart-header p');
    await expect(subheadline).toBeVisible();
    const subheadlineText = await subheadline.textContent();
    expect(subheadlineText.toLowerCase()).toContain('get');

    // Step 4: Verify installation instructions are present
    const quickstartSteps = quickstartSection.locator('.quickstart-step');
    const stepCount = await quickstartSteps.count();
    expect(stepCount).toBeGreaterThanOrEqual(3); // Should have at least 3 steps

    // Step 5: Verify Step 1 contains clone/build instructions
    const step1 = quickstartSteps.nth(0);
    const step1Header = step1.locator('h3');
    await expect(step1Header).toContainText('Clone');

    const step1CodeBlock = step1.locator('.code-block');
    await expect(step1CodeBlock).toBeVisible();
    const step1Code = await step1CodeBlock.textContent();
    expect(step1Code).toContain('git clone');
    expect(step1Code).toContain('cargo build');

    // Step 6: Verify Step 2 contains server start instructions
    const step2 = quickstartSteps.nth(1);
    const step2Header = step2.locator('h3');
    await expect(step2Header).toContainText('Server');

    const step2CodeBlock = step2.locator('.code-block');
    await expect(step2CodeBlock).toBeVisible();
    const step2Code = await step2CodeBlock.textContent();
    expect(step2Code).toContain('mirdb-server');

    // Step 7: Verify Step 3 contains usage examples
    const step3 = quickstartSteps.nth(2);
    const step3Header = step3.locator('h3');
    await expect(step3Header).toContainText('Connect');

    const step3CodeBlock = step3.locator('.code-block');
    await expect(step3CodeBlock).toBeVisible();
    const step3Code = await step3CodeBlock.textContent();
    expect(step3Code.toLowerCase()).toContain('set ');
    expect(step3Code.toLowerCase()).toContain('get ');
    expect(step3Code.toLowerCase()).toContain('delete ');

    // Step 8: Verify numbered steps for clarity
    const stepNumbers = quickstartSection.locator('.step-number');
    const stepNumberCount = await stepNumbers.count();
    expect(stepNumberCount).toBe(3);

    // Verify step numbers are sequential
    await expect(stepNumbers.nth(0)).toHaveText('1');
    await expect(stepNumbers.nth(1)).toHaveText('2');
    await expect(stepNumbers.nth(2)).toHaveText('3');
  });

  test('Test Case 2: Code blocks are selectable and can be copied', async ({ page }) => {
    // Step 1: Navigate to quick start section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Step 2: Find all code blocks
    const codeBlocks = quickstartSection.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Step 3: Verify code blocks have proper styling for selectability
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Verify code block is visible
      await expect(codeBlock).toBeVisible();

      // Verify code block uses pre tag (preserves formatting for copying)
      const tagName = await codeBlock.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('pre');

      // Verify code block contains code element
      const codeElement = codeBlock.locator('code');
      await expect(codeElement).toBeVisible();

      // Verify text content is not empty
      const textContent = await codeBlock.textContent();
      expect(textContent.trim().length).toBeGreaterThan(0);
    }

    // Step 4: Verify text is selectable by checking CSS properties
    const firstCodeBlock = codeBlocks.first();
    const userSelect = await firstCodeBlock.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.userSelect || style.webkitUserSelect || style.mozUserSelect;
    });
    // user-select should not be 'none' - should allow selection
    expect(userSelect).not.toBe('none');

    // Step 5: Verify code block has proper dimensions and is interactable
    const firstCodeBlockBox = await firstCodeBlock.boundingBox();
    expect(firstCodeBlockBox).not.toBeNull();
    expect(firstCodeBlockBox.width).toBeGreaterThan(0);
    expect(firstCodeBlockBox.height).toBeGreaterThan(0);

    // Step 6: Verify text selection is programmatically possible
    // (This verifies DOM structure supports selection, independent of browser UI behavior)
    const isSelectable = await firstCodeBlock.evaluate(el => {
      const range = document.createRange();
      const codeEl = el.querySelector('code');
      if (!codeEl || !codeEl.firstChild) return false;
      try {
        range.selectNodeContents(codeEl);
        return range.toString().trim().length > 0;
      } catch (e) {
        return false;
      }
    });
    expect(isSelectable).toBe(true);

    // Step 7: Verify code blocks have proper font-family for code
    const fontFamily = await firstCodeBlock.evaluate(el => {
      return window.getComputedStyle(el).fontFamily;
    });
    // Should use monospace font for code readability
    expect(fontFamily.toLowerCase()).toMatch(/monaco|menlo|monospace|courier/);

    // Step 8: Verify code blocks allow horizontal scrolling for long lines
    const overflow = await firstCodeBlock.evaluate(el => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(['auto', 'scroll', 'visible']).toContain(overflow);
  });

  test('Quick start section is accessible via navigation', async ({ page }) => {
    // Find the navigation link to Quick Start
    const navLink = page.locator('nav a[href="#quickstart"]');
    await expect(navLink).toBeVisible();
    await expect(navLink).toHaveText('Quick Start');

    // Click the navigation link
    await navLink.click();

    // Verify the quick start section is now in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Quick start section has proper heading hierarchy', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Main section heading should be h2
    const mainHeading = quickstartSection.locator('.quickstart-header h2');
    await expect(mainHeading).toBeVisible();

    // Step headings should be h3
    const stepHeadings = quickstartSection.locator('.quickstart-step h3');
    const stepHeadingCount = await stepHeadings.count();
    expect(stepHeadingCount).toBe(3);

    // Verify each step has a h3 heading
    for (let i = 0; i < stepHeadingCount; i++) {
      const heading = stepHeadings.nth(i);
      await expect(heading).toBeVisible();
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('h3');
    }
  });

  test('Hero CTA button links to quick start section', async ({ page }) => {
    // Find the "Get Started" button in hero section
    const getStartedBtn = page.locator('.hero-cta a.btn-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify it links to quickstart section
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click and verify navigation
    await getStartedBtn.click();

    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });
});
