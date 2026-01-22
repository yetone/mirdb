// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * User Journey - Time to First Interaction
 *
 * This test suite verifies users can interact with key elements quickly.
 * Success metric: under 30 seconds to first interaction.
 *
 * Test Cases:
 * 1. Copy install command is accessible within 30 seconds of page load
 * 2. Get Started CTA smoothly scrolls to quick start section
 * 3. GitHub link is discoverable within 10 seconds
 */

test.describe('User Journey - Time to First Interaction', () => {
  test.beforeEach(async ({ page }) => {
    // Start timing from fresh page load
    await page.goto('/');
  });

  test.describe('TC1: Copy Install Command - Under 30 seconds', () => {
    test('Installation copy button is immediately accessible after page load', async ({ page }) => {
      // Record page load time
      const loadStartTime = Date.now();

      // Find and click the "Get Started" button to navigate to quick start
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');
      await expect(getStartedButton).toBeVisible();
      await getStartedButton.click();

      // Verify quick start section is visible
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Find the copy button
      const copyButton = page.locator('.copy-btn[data-copy="install"]');
      await expect(copyButton).toBeVisible();

      // Calculate time elapsed
      const timeElapsed = Date.now() - loadStartTime;

      // Verify we can reach the copy button in under 30 seconds (30000ms)
      // In practice this should be nearly instant in an automated test
      expect(timeElapsed).toBeLessThan(30000);
    });

    test('Copy button is clickable and functional', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Navigate to quick start section
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');
      await getStartedButton.click();

      // Find and click the copy button
      const copyButton = page.locator('.copy-btn[data-copy="install"]');
      await expect(copyButton).toBeVisible();
      await expect(copyButton).toBeEnabled();

      // Click the copy button
      await copyButton.click();

      // Verify visual feedback - button text changes to "Copied!"
      await expect(copyButton).toHaveText('Copied!');

      // Verify clipboard contains installation commands
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toContain('git clone https://github.com/yetone/mirdb');
    });

    test('Installation commands are readable without interaction', async ({ page }) => {
      // Navigate to quick start using the Get Started button
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');
      await getStartedButton.click();

      // Verify installation code block is visible and contains all commands
      const installCode = page.locator('#install-code');
      await expect(installCode).toBeVisible();

      const codeText = await installCode.textContent();

      // All three installation steps should be visible
      expect(codeText).toContain('git clone https://github.com/yetone/mirdb');
      expect(codeText).toContain('cd mirdb && cargo build --release');
      expect(codeText).toContain('./target/release/mirdb-server -c etc/mirdb.toml');
    });
  });

  test.describe('TC2: Get Started CTA - Immediate Navigation', () => {
    test('Get Started button smoothly scrolls to quick start section', async ({ page }) => {
      // Verify Get Started button is visible in hero section
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');
      await expect(getStartedButton).toBeVisible();
      await expect(getStartedButton).toHaveText('Get Started');

      // Click the Get Started button
      await getStartedButton.click();

      // Verify URL hash changes to #quick-start
      await expect(page).toHaveURL(/#quick-start$/);

      // Verify quick start section is immediately visible
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Verify the section title is visible
      const sectionTitle = page.locator('#quickstart-title');
      await expect(sectionTitle).toBeVisible();
      await expect(sectionTitle).toHaveText('Quick Start');
    });

    test('Quick start section is within viewport after clicking Get Started', async ({ page }) => {
      // Click Get Started
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');
      await getStartedButton.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(500);

      // Get viewport size
      const viewportSize = page.viewportSize();

      // Get quick start section position
      const quickStartSection = page.locator('#quick-start');
      const boundingBox = await quickStartSection.boundingBox();

      expect(boundingBox).not.toBeNull();

      // Verify the section is within the viewport
      // The top of the section should be visible (y < viewport height)
      expect(boundingBox.y).toBeLessThan(viewportSize.height);
      expect(boundingBox.y).toBeGreaterThanOrEqual(-100); // Allow small tolerance for sticky header
    });

    test('Get Started button is prominently positioned above the fold', async ({ page }) => {
      // Verify the button is immediately visible without scrolling
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');
      await expect(getStartedButton).toBeVisible();

      // Get viewport and button position
      const viewportSize = page.viewportSize();
      const boundingBox = await getStartedButton.boundingBox();

      expect(boundingBox).not.toBeNull();

      // Button should be entirely within the initial viewport
      expect(boundingBox.y).toBeLessThan(viewportSize.height);
      expect(boundingBox.y + boundingBox.height).toBeLessThan(viewportSize.height);
    });

    test('Get Started button has proper styling for visibility', async ({ page }) => {
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');

      // Verify it has the primary button class for prominence
      await expect(getStartedButton).toHaveClass(/btn-primary/);

      // Verify it's large enough to be easily clickable
      const boundingBox = await getStartedButton.boundingBox();
      expect(boundingBox.width).toBeGreaterThan(80); // Minimum width for readability
      expect(boundingBox.height).toBeGreaterThan(30); // Minimum height for touch targets
    });
  });

  test.describe('TC3: GitHub Link - Discoverable Within 10 Seconds', () => {
    test('GitHub link is visible in hero section without scrolling', async ({ page }) => {
      // Record page load time
      const loadStartTime = Date.now();

      // Find the GitHub button in hero section
      const heroGithubButton = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');
      await expect(heroGithubButton).toBeVisible();

      // Verify the button text clearly indicates GitHub
      await expect(heroGithubButton).toHaveText('View on GitHub');

      // Calculate time to find
      const timeElapsed = Date.now() - loadStartTime;

      // Should be discoverable in under 10 seconds
      expect(timeElapsed).toBeLessThan(10000);
    });

    test('GitHub link is visible in navigation bar', async ({ page }) => {
      // Find GitHub link in navigation
      const navGithubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
      await expect(navGithubLink).toBeVisible();

      // Navigation is sticky, so it's always accessible
      const boundingBox = await navGithubLink.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox.y).toBeLessThan(100); // Should be at the top
    });

    test('Multiple GitHub entry points are available', async ({ page }) => {
      // Count all GitHub links on the page
      const githubLinks = page.locator('a[href="https://github.com/yetone/mirdb"]');

      // Should have at least 3 GitHub links (nav, hero, footer)
      const count = await githubLinks.count();
      expect(count).toBeGreaterThanOrEqual(3);

      // Verify the hero button is clearly visible
      const heroGithubButton = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');
      await expect(heroGithubButton).toBeVisible();

      // Verify the navigation link is visible
      const navGithubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
      await expect(navGithubLink).toBeVisible();
    });

    test('GitHub link is positioned above the fold for immediate discovery', async ({ page }) => {
      const viewportSize = page.viewportSize();

      // Check hero GitHub button position
      const heroGithubButton = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');
      const heroBoundingBox = await heroGithubButton.boundingBox();

      expect(heroBoundingBox).not.toBeNull();
      expect(heroBoundingBox.y + heroBoundingBox.height).toBeLessThan(viewportSize.height);

      // Check navigation GitHub link position
      const navGithubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
      const navBoundingBox = await navGithubLink.boundingBox();

      expect(navBoundingBox).not.toBeNull();
      expect(navBoundingBox.y + navBoundingBox.height).toBeLessThan(viewportSize.height);
    });

    test('GitHub links have proper accessibility attributes', async ({ page }) => {
      // Check that GitHub links open in new tab with proper security
      const heroGithubButton = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');
      await expect(heroGithubButton).toHaveAttribute('target', '_blank');
      await expect(heroGithubButton).toHaveAttribute('rel', 'noopener noreferrer');

      const navGithubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
      await expect(navGithubLink).toHaveAttribute('target', '_blank');
      await expect(navGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  test.describe('Overall User Journey Metrics', () => {
    test('Page loads with all key interactive elements visible', async ({ page }) => {
      // Verify all key elements are immediately available after page load

      // Hero CTA buttons
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');
      const githubButton = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');

      await expect(getStartedButton).toBeVisible();
      await expect(githubButton).toBeVisible();

      // Navigation links
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Verify page is interactive (not blocked by loading)
      await expect(page.locator('.hero')).toBeVisible();
    });

    test('Complete user journey: page load to copy command under 30 seconds', async ({ page, context }) => {
      // Grant clipboard permissions upfront
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      // Start timing the complete journey
      const journeyStartTime = Date.now();

      // Step 1: Page loads
      await expect(page.locator('.hero')).toBeVisible();

      // Step 2: Find and click Get Started
      const getStartedButton = page.locator('.hero-ctas a[href="#quick-start"]');
      await getStartedButton.click();

      // Step 3: Wait for quick start to be visible
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Step 4: Find and click copy button
      const copyButton = page.locator('.copy-btn[data-copy="install"]');
      await copyButton.click();

      // Step 5: Verify copy succeeded
      await expect(copyButton).toHaveText('Copied!');

      // Calculate total journey time
      const totalJourneyTime = Date.now() - journeyStartTime;

      // Success metric: under 30 seconds
      expect(totalJourneyTime).toBeLessThan(30000);
    });

    test('User can reach GitHub within 10 seconds from page load', async ({ page }) => {
      const journeyStartTime = Date.now();

      // Page loads
      await expect(page.locator('.hero')).toBeVisible();

      // Find GitHub button (no scrolling needed - it's above the fold)
      const githubButton = page.locator('.hero-ctas a[href="https://github.com/yetone/mirdb"]');
      await expect(githubButton).toBeVisible();

      // Verify it's clickable
      await expect(githubButton).toBeEnabled();

      const totalTime = Date.now() - journeyStartTime;

      // Success metric: under 10 seconds
      expect(totalTime).toBeLessThan(10000);
    });
  });
});
