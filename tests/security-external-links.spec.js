// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');
const htmlFilePath = path.join(__dirname, '..', 'index.html');

test.describe('Security - External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  // TC1: Check GitHub external links have rel='noopener noreferrer' or target='_self'
  test('TC1: GitHub links have rel="noopener noreferrer" or target="_self"', async ({ page }) => {
    // Find all GitHub links
    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    // Ensure there are GitHub links to test
    expect(count).toBeGreaterThan(0);

    // Check each GitHub link
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // If target="_blank", must have rel with noopener noreferrer
      if (target === '_blank') {
        expect(rel).not.toBeNull();
        const hasNoopener = rel?.includes('noopener');
        const hasNoreferrer = rel?.includes('noreferrer');
        expect(hasNoopener && hasNoreferrer).toBeTruthy();
      }
      // If no target="_blank" (opens in same tab), that's also secure
      // Links that don't open in new tab don't need rel attributes
    }
  });

  // TC2: Check all external links for security attributes
  test('TC2: All external links opening in new tab have rel="noopener"', async ({ page }) => {
    // Find all external links (links with target="_blank")
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    // Ensure there are external links to test
    expect(count).toBeGreaterThan(0);

    // Check each external link has appropriate rel attribute
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');

      // rel should contain 'noopener' to prevent tabnabbing attacks
      expect(rel).not.toBeNull();
      expect(rel).toContain('noopener');
    }
  });

  // TC3: Verify no inline JavaScript event handlers
  test('TC3: No onclick or other inline event handlers in HTML', async ({ page }) => {
    // Read the HTML file directly to check for inline event handlers
    const htmlContent = fs.readFileSync(htmlFilePath, 'utf-8');

    // List of inline event handlers to check
    const inlineEventHandlers = [
      'onclick',
      'onmouseover',
      'onmouseout',
      'onmouseenter',
      'onmouseleave',
      'onmousedown',
      'onmouseup',
      'ondblclick',
      'onkeydown',
      'onkeyup',
      'onkeypress',
      'onfocus',
      'onblur',
      'onchange',
      'oninput',
      'onsubmit',
      'onreset',
      'onload',
      'onerror',
      'onscroll',
      'onresize',
      'oncontextmenu',
      'ondrag',
      'ondrop',
      'ontouchstart',
      'ontouchend',
      'ontouchmove'
    ];

    // Check that no inline event handlers exist in the HTML
    for (const handler of inlineEventHandlers) {
      // Use regex to find attribute pattern like: onclick="..." or onclick='...'
      const regex = new RegExp(`\\s${handler}\\s*=\\s*["']`, 'i');
      const hasInlineHandler = regex.test(htmlContent);
      expect(hasInlineHandler, `Found inline event handler: ${handler}`).toBeFalsy();
    }

    // Also verify using Playwright by checking elements
    const elementsWithOnclick = await page.locator('[onclick]').count();
    const elementsWithOnmouseover = await page.locator('[onmouseover]').count();
    const elementsWithOnerror = await page.locator('[onerror]').count();
    const elementsWithOnload = await page.locator('[onload]').count();
    const elementsWithOnfocus = await page.locator('[onfocus]').count();
    const elementsWithOnblur = await page.locator('[onblur]').count();

    expect(elementsWithOnclick).toBe(0);
    expect(elementsWithOnmouseover).toBe(0);
    expect(elementsWithOnerror).toBe(0);
    expect(elementsWithOnload).toBe(0);
    expect(elementsWithOnfocus).toBe(0);
    expect(elementsWithOnblur).toBe(0);
  });

  // Additional security test: Verify external links with http:// are upgraded to https://
  test('All external anchor links use HTTPS protocol', async ({ page }) => {
    const allLinks = page.locator('a[href^="http://"]');
    const count = await allLinks.count();

    // There should be no http:// links (only https://)
    expect(count).toBe(0);
  });

  // Additional security test: Verify no javascript: protocol links
  test('No javascript: protocol links exist', async ({ page }) => {
    const jsLinks = page.locator('a[href^="javascript:"]');
    const count = await jsLinks.count();

    // There should be no javascript: protocol links
    expect(count).toBe(0);
  });

  // Additional security test: Verify specific GitHub links have proper security attributes
  test('Specific GitHub link verification', async ({ page }) => {
    // Test navigation GitHub link
    const navGithubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
    await expect(navGithubLink).toHaveAttribute('target', '_blank');
    await expect(navGithubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Test hero section GitHub button
    const heroGithubLink = page.locator('.cta-buttons a[href="https://github.com/yetone/mirdb"]');
    await expect(heroGithubLink).toHaveAttribute('target', '_blank');
    await expect(heroGithubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Test footer GitHub link
    const footerGithubLink = page.locator('.footer a[href="https://github.com/yetone/mirdb"]');
    await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    await expect(footerGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
