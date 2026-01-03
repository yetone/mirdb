// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for JavaScript Error Handling on MirDB Homepage
 *
 * Scenario: Verify the page loads without JavaScript errors and degrades gracefully
 *
 * Verifies:
 * - No JavaScript errors in browser console
 * - Page is usable with JavaScript disabled
 * - No unhandled promise rejections
 * - Scripts load in correct order
 */

test.describe('JavaScript Error Handling', () => {
  /**
   * Test Case 1: Load page and check console
   * Expected: No JavaScript errors in browser console
   */
  test('TC1: page loads without JavaScript errors in browser console', async ({ page }) => {
    const consoleErrors = [];
    const pageErrors = [];

    // Capture console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push({
          text: msg.text(),
          location: msg.location(),
          type: msg.type()
        });
      }
    });

    // Capture page errors (uncaught exceptions)
    page.on('pageerror', (error) => {
      pageErrors.push({
        message: error.message,
        stack: error.stack
      });
    });

    // Navigate to the page
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for any async operations to complete
    await page.waitForTimeout(1000);

    // Verify no JavaScript errors in console
    expect(consoleErrors).toHaveLength(0);

    // Verify no uncaught page errors
    expect(pageErrors).toHaveLength(0);

    // Verify page loaded successfully
    await expect(page.locator('.hero h1')).toBeVisible();
    await expect(page.locator('.hero h1')).toHaveText('MirDB');
  });

  /**
   * Test Case 2: Disable JavaScript and load page
   * Expected: Core content is visible and readable without JS
   */
  test('TC2: core content is visible and readable with JavaScript disabled', async ({ browser }) => {
    // Create a context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    await page.goto('/');

    // Verify hero section is visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify main heading is visible
    const heading = page.locator('.hero h1');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.hero .tagline');
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible
    const primaryCTA = page.locator('.btn-primary');
    await expect(primaryCTA).toBeVisible();

    const secondaryCTA = page.locator('.btn-secondary');
    await expect(secondaryCTA).toBeVisible();

    // Verify features section is visible
    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Verify quick start section is visible
    const quickstart = page.locator('#quickstart');
    await expect(quickstart).toBeVisible();

    // Verify code blocks are readable
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify first code block content is visible
    const firstCodeContent = await codeBlocks.first().locator('pre code').textContent();
    expect(firstCodeContent).toBeTruthy();
    expect(firstCodeContent.length).toBeGreaterThan(10);

    // Verify commands section is visible
    const commands = page.locator('#commands');
    await expect(commands).toBeVisible();

    // Verify roadmap section is visible
    const roadmap = page.locator('#roadmap');
    await expect(roadmap).toBeVisible();

    // Verify config section is visible
    const config = page.locator('#config');
    await expect(config).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify links are functional (they should have href attributes)
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThan(0);

    for (let i = 0; i < footerLinkCount; i++) {
      const href = await footerLinks.nth(i).getAttribute('href');
      expect(href).toBeTruthy();
    }

    await context.close();
  });

  /**
   * Test Case 3: Check for unhandled promise rejections
   * Expected: No unhandled promise rejection errors
   */
  test('TC3: no unhandled promise rejection errors', async ({ page }) => {
    const unhandledRejections = [];
    const consoleErrors = [];

    // Capture console messages for unhandled rejections
    page.on('console', (msg) => {
      const text = msg.text().toLowerCase();
      if (text.includes('unhandled') && text.includes('rejection')) {
        unhandledRejections.push({
          text: msg.text(),
          type: msg.type()
        });
      }
      if (msg.type() === 'error') {
        consoleErrors.push({
          text: msg.text(),
          type: msg.type()
        });
      }
    });

    // Capture page errors which may include unhandled rejections
    page.on('pageerror', (error) => {
      const message = error.message.toLowerCase();
      if (message.includes('unhandled') || message.includes('rejection')) {
        unhandledRejections.push({
          text: error.message,
          type: 'pageerror'
        });
      }
    });

    // Navigate and wait for full load
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for all async operations to complete
    await page.waitForTimeout(2000);

    // Interact with the page to trigger any lazy-loaded promises
    await page.evaluate(() => {
      // Scroll through the page to trigger any lazy operations
      window.scrollTo(0, document.body.scrollHeight);
    });

    await page.waitForTimeout(500);

    await page.evaluate(() => {
      window.scrollTo(0, 0);
    });

    await page.waitForTimeout(500);

    // Verify no unhandled promise rejections
    expect(unhandledRejections).toHaveLength(0);

    // Also verify no general console errors that might indicate promise issues
    const promiseRelatedErrors = consoleErrors.filter(
      err => err.text.toLowerCase().includes('promise') ||
             err.text.toLowerCase().includes('reject') ||
             err.text.toLowerCase().includes('unhandled')
    );
    expect(promiseRelatedErrors).toHaveLength(0);
  });

  /**
   * Test Case 4 (Unit test converted to E2E): Verify script loading order
   * Expected: Scripts load in correct order without race conditions
   */
  test('TC4: scripts load in correct order without race conditions', async ({ page }) => {
    const scriptLoadOrder = [];
    const scriptErrors = [];

    // Track script loading order
    page.on('response', (response) => {
      const url = response.url();
      if (url.includes('.js') && response.status() === 200) {
        scriptLoadOrder.push({
          url: url,
          status: response.status()
        });
      }
    });

    // Capture any script errors
    page.on('pageerror', (error) => {
      scriptErrors.push({
        message: error.message,
        stack: error.stack
      });
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify no script loading errors occurred
    expect(scriptErrors).toHaveLength(0);

    // Verify scripts are positioned correctly in the DOM (end of body for non-blocking)
    const scriptPositions = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script[src]');
      return Array.from(scripts).map(script => ({
        src: script.src,
        inHead: script.parentElement?.tagName === 'HEAD',
        hasAsync: script.async,
        hasDefer: script.defer,
        isAfterMainContent: (() => {
          // Check if script is after the main content (end of body)
          const main = document.querySelector('main');
          if (main && script.compareDocumentPosition) {
            return (script.compareDocumentPosition(main) & Node.DOCUMENT_POSITION_PRECEDING) !== 0;
          }
          return false;
        })()
      }));
    });

    // Verify all scripts are either:
    // 1. In body (not head) - loaded at the end
    // 2. Have async or defer attribute
    for (const script of scriptPositions) {
      const isNonBlocking = script.hasAsync || script.hasDefer || !script.inHead || script.isAfterMainContent;
      expect(isNonBlocking).toBe(true);
    }

    // Verify Prism.js dependencies load correctly (main script before components)
    const prismScripts = scriptLoadOrder.filter(s => s.url.includes('prism'));
    if (prismScripts.length > 1) {
      // If both prism.min.js and prism-bash.min.js are loaded,
      // the main prism script should come first in the HTML
      const scriptSrcs = await page.evaluate(() => {
        const scripts = document.querySelectorAll('script[src*="prism"]');
        return Array.from(scripts).map(s => s.src);
      });

      const mainPrismIndex = scriptSrcs.findIndex(s => s.includes('prism.min.js'));
      const bashPrismIndex = scriptSrcs.findIndex(s => s.includes('prism-bash.min.js'));

      // Main prism should come before component scripts in DOM order
      if (mainPrismIndex !== -1 && bashPrismIndex !== -1) {
        expect(mainPrismIndex).toBeLessThan(bashPrismIndex);
      }
    }

    // Verify syntax highlighting was applied (indicating scripts loaded correctly)
    const hasPrism = await page.evaluate(() => {
      return typeof window.Prism !== 'undefined';
    });

    // Prism should be available
    expect(hasPrism).toBe(true);

    // Check that code highlighting was applied
    const highlightedCode = page.locator('pre code.language-bash');
    const count = await highlightedCode.count();
    expect(count).toBeGreaterThan(0);
  });

  /**
   * Additional test: Verify no 404 errors for scripts
   */
  test('TC_bonus: no 404 errors for script resources', async ({ page }) => {
    const failedRequests = [];

    page.on('response', (response) => {
      if (response.url().includes('.js') && response.status() >= 400) {
        failedRequests.push({
          url: response.url(),
          status: response.status()
        });
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify no script resources failed to load
    expect(failedRequests).toHaveLength(0);
  });

  /**
   * Additional test: Navigation still works without JS
   */
  test('TC_bonus: navigation links work without JavaScript', async ({ browser }) => {
    const context = await browser.newContext({
      javaScriptEnabled: false
    });
    const page = await context.newPage();

    await page.goto('/');

    // Check that anchor links have proper href
    const getStartedLink = page.locator('a.btn-primary[href="#quickstart"]');
    await expect(getStartedLink).toBeVisible();

    const href = await getStartedLink.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click should navigate to section (anchor navigation works without JS)
    await getStartedLink.click();

    // URL should include the anchor
    await expect(page).toHaveURL(/#quickstart$/);

    // Quickstart section should be visible
    await expect(page.locator('#quickstart')).toBeVisible();

    await context.close();
  });
});
