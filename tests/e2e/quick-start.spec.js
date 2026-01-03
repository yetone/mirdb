// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Quick Start Section
 * Tests the quick start section for installation instructions, code examples, and documentation links.
 */

test.describe('Quick Start Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Verify quick start section contains installation content
   */
  test('should display quick start section with installation instructions', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Verify section title
    const sectionTitle = quickStartSection.locator('h2');
    await expect(sectionTitle).toHaveText('Quick Start');

    // Verify installation step exists
    const installationStep = quickStartSection.locator('.quick-start-step').first();
    await expect(installationStep).toBeVisible();

    // Check for installation heading
    const installationHeading = installationStep.locator('h3');
    await expect(installationHeading).toContainText('Installation');

    // Check for installation command (git clone or cargo)
    const codeBlock = installationStep.locator('pre code');
    await expect(codeBlock).toBeVisible();
    const codeContent = await codeBlock.textContent();
    expect(codeContent).toMatch(/(git clone|cargo|clone)/i);
  });

  /**
   * Test Case 2: Verify code blocks have syntax highlighting (Prism.js)
   */
  test('should have syntax highlighting applied to code blocks', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Check that code blocks have Prism.js language class
    const codeBlocks = quickStartSection.locator('pre code[class*="language-"]');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify at least one code block has bash/shell language class
    const bashCodeBlock = quickStartSection.locator('pre code.language-bash');
    await expect(bashCodeBlock.first()).toBeVisible();

    // Wait for Prism.js to load and apply syntax highlighting
    await page.waitForFunction(() => {
      const codeElements = document.querySelectorAll('#quickstart pre code');
      return codeElements.length > 0;
    });

    // Verify the code block container has styling applied
    const codeBlockContainer = quickStartSection.locator('.code-block').first();
    await expect(codeBlockContainer).toBeVisible();
  });

  /**
   * Test Case 3: Verify SET command example is present
   */
  test('should display SET command example', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Look for SET command in code blocks
    const codeBlocks = quickStartSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedContent = allCodeContent.join(' ');

    // Verify SET command is present
    expect(combinedContent).toMatch(/SET\s+mykey/i);

    // Find the specific step with SET command using the heading text
    const setStep = quickStartSection.locator('.quick-start-step:has(h3:has-text("Store Data"))');
    await expect(setStep).toBeVisible();

    // Verify it shows the command syntax or example
    const setCode = setStep.locator('pre code');
    const setCodeContent = await setCode.textContent();
    expect(setCodeContent).toContain('SET');
  });

  /**
   * Test Case 4: Verify GET command example is present
   */
  test('should display GET command example', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Look for GET command in code blocks
    const codeBlocks = quickStartSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedContent = allCodeContent.join(' ');

    // Verify GET command is present (with memcached GET command format)
    expect(combinedContent).toMatch(/GET\s+mykey/i);

    // Find the specific step with GET command using the heading text
    const getStep = quickStartSection.locator('.quick-start-step:has(h3:has-text("Retrieve Data"))');
    await expect(getStep).toBeVisible();

    // Verify it shows the command syntax or example
    const getCode = getStep.locator('pre code');
    const getCodeContent = await getCode.textContent();
    expect(getCodeContent).toContain('GET');
  });

  /**
   * Test Case 5: Verify documentation link is present and valid
   */
  test('should have documentation link present and valid', async ({ page }) => {
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Find documentation link
    const docsLink = quickStartSection.locator('#docs-link, .docs-link').first();
    await expect(docsLink).toBeVisible();

    // Verify link text indicates documentation
    const linkText = await docsLink.textContent();
    expect(linkText?.toLowerCase()).toMatch(/(documentation|docs|learn more|readme)/i);

    // Verify link has valid href
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https?:\/\//);

    // Verify the link points to GitHub (expected documentation location)
    expect(href).toContain('github.com');
  });

  /**
   * Additional test: Hero section CTA navigates to quick start
   */
  test('should navigate to quick start from hero CTA', async ({ page }) => {
    // Click on Get Started button
    const getStartedBtn = page.locator('a.btn-primary:has-text("Get Started")');
    await expect(getStartedBtn).toBeVisible();

    // Check that the href points to quickstart section
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click and verify navigation
    await getStartedBtn.click();

    // Verify quick start section is now in view (scrolled to)
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport();
  });
});
