// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Broken Assets E2E Tests for MirDB Homepage
 *
 * Scenario: Verify graceful handling when external assets fail to load
 *
 * These tests verify that:
 * 1. The page handles blocked external badge URLs gracefully
 * 2. Broken images have appropriate fallbacks (alt text visible)
 * 3. Core content and navigation remain functional when external resources fail
 */

test.describe('Error Handling - Broken Assets', () => {
  /**
   * Test Case 1: Block CircleCI badge URL
   * Input: Block CircleCI badge URL
   * Expected: Page loads successfully, badge shows placeholder or alt text
   */
  test.describe('TC1: Block CircleCI badge URL', () => {
    test('should load page successfully when CircleCI badge URL is blocked', async ({ page }) => {
      // Block all requests to CircleCI domain to simulate network failure
      await page.route('**/circleci.com/**', route => route.abort());

      // Navigate to the page
      await page.goto('/');

      // Verify page loads successfully
      await expect(page).toHaveTitle(/MirDB/);

      // Verify hero section is visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify main heading is visible
      const mainHeading = page.locator('h1');
      await expect(mainHeading).toBeVisible();
      await expect(mainHeading).toContainText('MirDB');
    });

    test('should display alt text when CircleCI badge fails to load', async ({ page }) => {
      // Block requests to CircleCI badge
      await page.route('**/circleci.com/**', route => route.abort());

      // Navigate to the page
      await page.goto('/');

      // Find the badge image
      const badgeImg = page.locator('.badges img');
      await expect(badgeImg).toBeAttached();

      // Verify the image has proper alt text for accessibility when image fails
      const altText = await badgeImg.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.toLowerCase()).toContain('circleci');
    });

    test('should keep badges section visible even when image fails', async ({ page }) => {
      // Block CircleCI requests
      await page.route('**/circleci.com/**', route => route.abort());

      await page.goto('/');

      // The badges section should still be in the DOM
      const badgesSection = page.locator('.badges');
      await expect(badgesSection).toBeAttached();

      // The link should still be clickable
      const badgeLink = page.locator('.badges a');
      await expect(badgeLink).toBeAttached();
      await expect(badgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
    });
  });

  /**
   * Test Case 2: Verify page functionality with blocked external resources
   * Input: Verify page functionality with blocked external resources
   * Expected: Core content and navigation remain functional
   */
  test.describe('TC2: Verify page functionality with blocked external resources', () => {
    test('should maintain navigation functionality when external assets are blocked', async ({ page }) => {
      // Block all external image requests
      await page.route('**/circleci.com/**', route => route.abort());
      await page.route('**/raw.githubusercontent.com/**', route => route.abort());

      await page.goto('/');

      // Verify navigation bar is visible
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Verify all nav links are present and functional
      const navLinks = page.locator('.nav-links a');
      await expect(navLinks).toHaveCount(4);

      // Click Features link and verify navigation
      const featuresLink = navLinks.filter({ hasText: 'Features' });
      await featuresLink.click();
      await expect(page).toHaveURL(/#features/);

      // Verify the features section is visible after navigation
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    });

    test('should display all core content sections when external assets fail', async ({ page }) => {
      // Block all external resources
      await page.route('**/circleci.com/**', route => route.abort());
      await page.route('**/github.com/**', route => route.abort());
      await page.route('**/githubusercontent.com/**', route => route.abort());

      await page.goto('/');

      // Verify hero section
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify CTA buttons are present
      const ctaButtons = page.locator('.cta-buttons .btn');
      await expect(ctaButtons).toHaveCount(2);

      // Verify features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify all 6 feature cards
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(6);

      // Verify code example section
      const codeExampleSection = page.locator('#code-example');
      await expect(codeExampleSection).toBeVisible();

      // Verify quick start section
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      // Verify architecture section
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      // Verify footer
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('should allow CTA buttons to function when external assets are blocked', async ({ page }) => {
      // Block external resources
      await page.route('**/circleci.com/**', route => route.abort());

      await page.goto('/');

      // Verify Get Started button links to quickstart
      const getStartedBtn = page.locator('#cta-primary');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');

      // Verify GitHub button has correct link
      const githubBtn = page.locator('#cta-github');
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Click Get Started and verify navigation
      await getStartedBtn.click();
      await expect(page).toHaveURL(/#quickstart/);
    });

    test('should display code examples correctly when external resources fail', async ({ page }) => {
      // Block all external resources
      await page.route('**/circleci.com/**', route => route.abort());

      await page.goto('/');

      // Navigate to code example section
      const codeExampleSection = page.locator('#code-example');
      await expect(codeExampleSection).toBeVisible();

      // Verify code block is visible
      const codeBlock = page.locator('.code-block');
      await expect(codeBlock).toBeVisible();

      // Verify code content is visible
      const codeContent = page.locator('.code-content');
      await expect(codeContent).toBeVisible();

      // Verify actual code is present
      const codeText = await codeContent.textContent();
      expect(codeText).toContain('telnet');
      expect(codeText).toContain('set mykey');
      expect(codeText).toContain('STORED');
    });

    test('should handle local asset failures gracefully', async ({ page }) => {
      // Block local gif assets to simulate broken local images
      await page.route('**/assets/*.gif', route => route.abort());
      await page.route('**/circleci.com/**', route => route.abort());

      await page.goto('/');

      // Page should still load
      await expect(page).toHaveTitle(/MirDB/);

      // Hero section should be visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Logo img should have alt text for fallback
      const logoImg = page.locator('.hero-logo');
      await expect(logoImg).toBeAttached();
      const logoAlt = await logoImg.getAttribute('alt');
      expect(logoAlt).toBeTruthy();
      expect(logoAlt).toContain('MirDB');

      // Usage gif should have alt text for fallback
      const usageImg = page.locator('#code-example img');
      await expect(usageImg).toBeAttached();
      const usageAlt = await usageImg.getAttribute('alt');
      expect(usageAlt).toBeTruthy();
    });

    test('should preserve footer links when external assets fail', async ({ page }) => {
      // Block external resources
      await page.route('**/circleci.com/**', route => route.abort());

      await page.goto('/');

      // Verify footer links
      const footerLinks = page.locator('.footer-links a');
      await expect(footerLinks).toHaveCount(3);

      // Verify GitHub link
      const githubLink = footerLinks.nth(0);
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubLink).toHaveAttribute('target', '_blank');

      // Verify Issues link
      const issuesLink = footerLinks.nth(1);
      await expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');

      // Verify Documentation link
      const docsLink = footerLinks.nth(2);
      await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
    });

    test('should not cause JavaScript errors when assets fail to load', async ({ page }) => {
      // Collect any console errors
      const consoleErrors = [];
      page.on('console', msg => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      // Block external resources
      await page.route('**/circleci.com/**', route => route.abort());
      await page.route('**/assets/*.gif', route => route.abort());

      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Filter out expected network errors for blocked resources
      const jsErrors = consoleErrors.filter(error =>
        !error.includes('net::ERR_FAILED') &&
        !error.includes('Failed to load resource') &&
        !error.includes('circleci.com') &&
        !error.includes('assets/')
      );

      // There should be no actual JavaScript errors
      expect(jsErrors).toHaveLength(0);
    });
  });

  /**
   * Additional test: Complete page functionality with all external resources blocked
   */
  test('should render complete page layout when all external resources are blocked', async ({ page }) => {
    // Block all external requests using multiple route patterns
    await page.route('**/circleci.com/**', route => route.abort());
    await page.route('**/githubusercontent.com/**', route => route.abort());

    await page.goto('/');

    // Verify page structure is complete
    await expect(page.locator('nav')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#code-example')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('#architecture')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Verify the page is scrollable
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    const scrolledToBottom = await page.evaluate(() =>
      window.scrollY > 0 && (window.innerHeight + window.scrollY) >= document.body.scrollHeight - 10
    );
    expect(scrolledToBottom).toBe(true);
  });
});
