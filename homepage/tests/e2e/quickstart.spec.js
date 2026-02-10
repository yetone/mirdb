/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 3 - Quick Start Guide Section
 *
 * End-to-end tests for the MirDB homepage quick start section.
 *
 * Expected test coverage:
 * - Quick start section contains installation steps (numbered/bulleted)
 * - Code block elements are present with example code
 * - Code example shows connection, set, and get operations
 * - Documentation link is present
 *
 * Requirements traced:
 * - REQ-3: Homepage shall provide quick start guide
 * - USR-3: New user wants to get started with MirDB quickly
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Guide Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check quick start section for installation steps
  test('section contains numbered or bulleted installation steps', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for ordered list (ol) or unordered list (ul) with list items
    const orderedList = quickstartSection.locator('ol');
    const unorderedList = quickstartSection.locator('ul');

    const hasOrderedList = await orderedList.count() > 0;
    const hasUnorderedList = await unorderedList.count() > 0;

    expect(hasOrderedList || hasUnorderedList).toBeTruthy();

    // Verify there are list items
    const listItems = quickstartSection.locator('li');
    const listItemCount = await listItems.count();
    expect(listItemCount).toBeGreaterThanOrEqual(2);

    // Verify installation-related content
    const sectionText = await quickstartSection.textContent();
    const lowerText = sectionText.toLowerCase();
    const hasInstallationContent =
      lowerText.includes('install') ||
      lowerText.includes('clone') ||
      lowerText.includes('build') ||
      lowerText.includes('start');

    expect(hasInstallationContent).toBeTruthy();
  });

  // Test Case 2: Check for code block elements
  test('at least one pre or code element with example code is present', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Check for pre or code elements
    const preElements = quickstartSection.locator('pre');
    const codeElements = quickstartSection.locator('code');

    const preCount = await preElements.count();
    const codeCount = await codeElements.count();

    expect(preCount > 0 || codeCount > 0).toBeTruthy();

    // Verify the code blocks contain actual code content
    if (preCount > 0) {
      const firstPre = preElements.first();
      const codeContent = await firstPre.textContent();
      expect(codeContent.trim().length).toBeGreaterThan(0);
    }
  });

  // Test Case 3: Verify code example contains basic operations
  test('code example shows connection, set, and get operations', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get all code content from the section
    const codeBlocks = quickstartSection.locator('pre code, pre');
    const codeCount = await codeBlocks.count();

    let combinedCode = '';
    for (let i = 0; i < codeCount; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      combinedCode += ' ' + codeText.toLowerCase();
    }

    // Check for memcached basic operations
    // Connection: client, connect, localhost, port, etc.
    const hasConnection =
      combinedCode.includes('client') ||
      combinedCode.includes('connect') ||
      combinedCode.includes('localhost') ||
      combinedCode.includes('11211');

    // Set operation
    const hasSet =
      combinedCode.includes('.set') ||
      combinedCode.includes('set(') ||
      combinedCode.includes('set ');

    // Get operation
    const hasGet =
      combinedCode.includes('.get') ||
      combinedCode.includes('get(') ||
      combinedCode.includes('get ');

    expect(hasConnection).toBeTruthy();
    expect(hasSet).toBeTruthy();
    expect(hasGet).toBeTruthy();
  });

  // Test Case 5: Check for documentation link
  test('quick start section contains a documentation link', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Look for links that mention documentation
    const links = quickstartSection.locator('a');
    const linkCount = await links.count();

    let foundDocsLink = false;
    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const linkText = await link.textContent();
      const linkHref = await link.getAttribute('href');

      const lowerText = linkText.toLowerCase();
      const lowerHref = (linkHref || '').toLowerCase();

      if (
        lowerText.includes('documentation') ||
        lowerText.includes('docs') ||
        lowerText.includes('learn more') ||
        lowerHref.includes('doc') ||
        lowerHref.includes('readme')
      ) {
        foundDocsLink = true;
        break;
      }
    }

    expect(foundDocsLink).toBeTruthy();
  });

  // Additional test: Verify quick start section is navigable
  test('quick start section is navigable from header', async ({ page }) => {
    // Click the Quick Start link in navigation
    const navLink = page.locator('nav a[href="#quickstart"]');
    await expect(navLink).toBeVisible();
    await navLink.click();

    // Verify the section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });
});
