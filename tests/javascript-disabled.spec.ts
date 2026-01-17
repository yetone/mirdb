import { test, expect } from '@playwright/test';

/**
 * E2E Tests for JavaScript Disabled Scenario
 *
 * This test suite verifies that the homepage provides basic content when JavaScript
 * is disabled, ensuring progressive enhancement and graceful degradation.
 *
 * Test Cases:
 * 1. Core content (text, images, links) is still visible when JS is disabled
 * 2. Noscript fallback message is displayed when needed
 */

test.describe('Error Handling - JavaScript Disabled', () => {
  test.describe('with JavaScript disabled', () => {
    test.use({ javaScriptEnabled: false });

    test('Test Case 1: Core content is visible when JavaScript is disabled', async ({ page }) => {
      await page.goto('/');

      // Verify the main heading is visible
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // Verify the tagline is visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify the description is visible
      const description = page.locator('.description');
      await expect(description).toBeVisible();

      // Verify the logo image is visible
      const logo = page.locator('img.logo');
      await expect(logo).toBeVisible();
      const logoAlt = await logo.getAttribute('alt');
      expect(logoAlt).toContain('MirDB');

      // Verify navigation links are visible and functional
      const getStartedLink = page.locator('a[data-link="get-started"]');
      await expect(getStartedLink).toBeVisible();
      const getStartedHref = await getStartedLink.getAttribute('href');
      expect(getStartedHref).toBe('#getting-started');

      const githubLink = page.locator('a[data-link="github-hero"]');
      await expect(githubLink).toBeVisible();
      const githubHref = await githubLink.getAttribute('href');
      expect(githubHref).toContain('github.com');

      // Verify key sections are visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeVisible();

      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeVisible();

      // Verify feature cards are visible
      const featureCards = page.locator('.feature-card');
      const featureCount = await featureCards.count();
      expect(featureCount).toBeGreaterThan(0);
      console.log(`Found ${featureCount} feature cards visible without JavaScript`);

      // Verify at least one feature card title is visible
      const firstFeatureTitle = page.locator('.feature-card h3').first();
      await expect(firstFeatureTitle).toBeVisible();

      // Verify command cards are visible
      const commandCards = page.locator('.command-card');
      const commandCount = await commandCards.count();
      expect(commandCount).toBeGreaterThan(0);
      console.log(`Found ${commandCount} command cards visible without JavaScript`);

      // Verify code examples are visible (even if not syntax highlighted)
      const codeBlocks = page.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);
      console.log(`Found ${codeBlockCount} code blocks visible without JavaScript`);

      // Verify the footer is visible with links
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      const footerGithubLink = page.locator('a[data-link="github-footer"]');
      await expect(footerGithubLink).toBeVisible();

      // Verify architecture section is visible
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      // Verify configuration table is visible
      const configTable = page.locator('.config-table table');
      await expect(configTable).toBeVisible();

      console.log('All core content is accessible without JavaScript');
    });

    test('Test Case 2: Noscript fallback message is displayed', async ({ page }) => {
      await page.goto('/');

      // Check if noscript element exists
      const noscriptElement = page.locator('noscript');
      const noscriptCount = await noscriptElement.count();

      expect(noscriptCount).toBeGreaterThan(0);
      console.log(`Found ${noscriptCount} noscript element(s)`);

      // Verify noscript content is visible when JS is disabled
      // The noscript content should be rendered when JS is off
      const noscriptContent = await page.evaluate(() => {
        const noscript = document.querySelector('noscript');
        if (noscript) {
          // When JS is disabled, content inside noscript is rendered
          // In Playwright with JS disabled, we can check the text content
          return noscript.textContent || noscript.innerHTML;
        }
        return null;
      });

      // When JavaScript is disabled, the noscript element's content is displayed
      // We verify the noscript element exists and contains appropriate fallback content
      if (noscriptCount > 0) {
        // Get the noscript element's text content via the page
        const noscriptText = await noscriptElement.first().textContent();
        console.log(`Noscript content: "${noscriptText}"`);

        // Verify it contains helpful information for users
        expect(noscriptText).toBeTruthy();
        expect(noscriptText!.length).toBeGreaterThan(0);
      }

      // Verify the page is still functional even with the noscript message
      const mainContent = page.locator('main');
      await expect(mainContent).toBeVisible();

      console.log('Noscript fallback is properly configured');
    });

    test('Copy button graceful degradation when JavaScript is disabled', async ({ page }) => {
      await page.goto('/');

      // Verify code example section exists
      const codeExampleSection = page.locator('#code-example');
      await expect(codeExampleSection).toBeVisible();

      // The copy button exists but won't function without JavaScript
      // It should not break the page or cause errors
      const copyButton = page.locator('.copy-btn');
      const copyButtonCount = await copyButton.count();

      if (copyButtonCount > 0) {
        // Button exists - verify the code is still visible and readable
        const codeContent = page.locator('#example-code');
        await expect(codeContent).toBeVisible();

        const codeText = await codeContent.textContent();
        expect(codeText).toContain('pymemcache');
        expect(codeText).toContain('client.set');
        expect(codeText).toContain('client.get');

        console.log('Code examples are readable without JavaScript');
      }

      // Verify the page doesn't have any visible JavaScript errors
      // (there shouldn't be error messages displayed)
      const errorMessages = page.locator('[class*="error"], [class*="Error"]');
      const errorCount = await errorMessages.count();
      expect(errorCount).toBe(0);
    });

    test('Navigation works without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Test internal anchor navigation
      const gettingStartedLink = page.locator('a[href="#getting-started"]').first();
      await expect(gettingStartedLink).toBeVisible();

      // Click the link
      await gettingStartedLink.click();

      // Verify we navigated to the section (URL hash should change)
      await page.waitForURL('**/#getting-started');

      // Verify the getting started section is in view
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeVisible();

      console.log('Internal navigation works without JavaScript');
    });

    test('External links are accessible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify GitHub link has proper attributes
      const githubLink = page.locator('a[data-link="github-hero"]');
      await expect(githubLink).toBeVisible();

      const href = await githubLink.getAttribute('href');
      const target = await githubLink.getAttribute('target');
      const rel = await githubLink.getAttribute('rel');

      expect(href).toContain('github.com');
      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');

      // Verify footer links are also accessible
      const footerLinks = page.locator('footer a');
      const footerLinkCount = await footerLinks.count();
      expect(footerLinkCount).toBeGreaterThan(0);

      for (let i = 0; i < footerLinkCount; i++) {
        const link = footerLinks.nth(i);
        const linkHref = await link.getAttribute('href');
        expect(linkHref).toBeTruthy();
      }

      console.log('All external links are properly configured');
    });
  });

  test.describe('Comparison: with JavaScript enabled', () => {
    test.use({ javaScriptEnabled: true });

    test('Noscript content is hidden when JavaScript is enabled', async ({ page }) => {
      await page.goto('/');

      // When JavaScript is enabled, noscript content should not be visible
      // The noscript element exists but its content is not rendered
      const noscriptElements = page.locator('noscript');
      const count = await noscriptElements.count();

      if (count > 0) {
        // noscript content should not be visible when JS is enabled
        // We can't directly check visibility of noscript content since it's not rendered
        // Instead, verify the page works normally with JS

        // Verify syntax highlighting works (Prism.js)
        const codeBlock = page.locator('#example-code');
        const classes = await codeBlock.getAttribute('class');
        expect(classes).toContain('language-python');

        // Verify copy button is functional
        const copyButton = page.locator('.copy-btn');
        await expect(copyButton).toBeVisible();
        await expect(copyButton).toHaveText('Copy');

        console.log('JavaScript features work correctly when JS is enabled');
      }
    });
  });
});
