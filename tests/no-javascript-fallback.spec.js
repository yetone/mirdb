// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Scenario: No JavaScript Fallback
 *
 * Validates that core content is accessible without JavaScript enabled.
 * This tests graceful degradation and ensures the page remains functional
 * when JavaScript is disabled.
 */

test.describe('No JavaScript Fallback', () => {
  test.describe('Test Case 1: Load page with JS disabled - All text content and images are visible', () => {
    test.use({ javaScriptEnabled: false });

    test('hero section content is visible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify hero section is visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify hero logo image is visible
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();
      await expect(heroLogo).toHaveAttribute('alt', 'MirDB Logo');

      // Verify hero headline text is visible
      const heroHeadline = page.locator('.hero-headline');
      await expect(heroHeadline).toBeVisible();
      await expect(heroHeadline).toContainText('MirDB');

      // Verify hero subheadline text is visible
      const heroSubheadline = page.locator('.hero-subheadline');
      await expect(heroSubheadline).toBeVisible();
      await expect(heroSubheadline).toContainText('memcached');
    });

    test('value proposition section is visible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify value proposition section is visible
      const valuePropSection = page.locator('.value-prop');
      await expect(valuePropSection).toBeVisible();

      // Verify all three value proposition columns are visible
      const valuePropColumns = page.locator('.value-prop-column');
      await expect(valuePropColumns).toHaveCount(3);

      // Verify Memcached Protocol Support card
      const memcachedCard = valuePropColumns.filter({ hasText: 'Memcached Protocol Support' });
      await expect(memcachedCard).toBeVisible();

      // Verify Persistence card
      const persistenceCard = valuePropColumns.filter({ hasText: 'Persistence' });
      await expect(persistenceCard).toBeVisible();

      // Verify LSM Tree Architecture card
      const lsmTreeCard = valuePropColumns.filter({ hasText: 'LSM Tree Architecture' });
      await expect(lsmTreeCard).toBeVisible();
    });

    test('features section is visible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify features section is visible
      const featuresSection = page.locator('.features');
      await expect(featuresSection).toBeVisible();

      // Verify features header is visible
      const featuresHeader = page.locator('.features-header h2');
      await expect(featuresHeader).toBeVisible();
      await expect(featuresHeader).toContainText('Features');

      // Verify feature cards are visible
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();
      expect(count).toBeGreaterThanOrEqual(4); // At least 4 feature cards

      // Verify specific feature cards
      const asyncNetworking = featureCards.filter({ hasText: 'Async Networking' });
      await expect(asyncNetworking).toBeVisible();

      const memtable = featureCards.filter({ hasText: 'Memtable' });
      await expect(memtable).toBeVisible();
    });

    test('quick start section is visible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify quick start section is visible
      const quickstartSection = page.locator('.quickstart');
      await expect(quickstartSection).toBeVisible();

      // Verify quick start header
      const quickstartHeader = page.locator('.quickstart-header h2');
      await expect(quickstartHeader).toBeVisible();
      await expect(quickstartHeader).toContainText('Quick Start');

      // Verify all quickstart steps are visible
      const quickstartSteps = page.locator('.quickstart-step');
      await expect(quickstartSteps).toHaveCount(3);

      // Verify code blocks are readable
      const codeBlocks = page.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThanOrEqual(3);
    });

    test('demo section with usage gif is visible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify demo section is visible
      const demoSection = page.locator('.demo');
      await expect(demoSection).toBeVisible();

      // Verify demo header
      const demoHeader = page.locator('.demo-header h2');
      await expect(demoHeader).toBeVisible();
      await expect(demoHeader).toContainText('MirDB in Action');

      // Verify demo gif is visible
      const demoGif = page.locator('.demo-gif');
      await expect(demoGif).toBeVisible();
      await expect(demoGif).toHaveAttribute('alt', /usage demonstration/i);

      // Verify demo caption is visible
      const demoCaption = page.locator('.demo-caption');
      await expect(demoCaption).toBeVisible();
    });

    test('commands section is visible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify commands section is visible
      const commandsSection = page.locator('.commands');
      await expect(commandsSection).toBeVisible();

      // Verify commands header
      const commandsHeader = page.locator('.commands-header h2');
      await expect(commandsHeader).toBeVisible();
      await expect(commandsHeader).toContainText('Supported Commands');

      // Verify command categories are visible
      const commandCategories = page.locator('.command-category');
      const categoryCount = await commandCategories.count();
      expect(categoryCount).toBeGreaterThanOrEqual(3);
    });

    test('footer section is visible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify footer is visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Verify footer content
      const footerInfo = page.locator('.footer-info');
      await expect(footerInfo).toBeVisible();
      await expect(footerInfo).toContainText('MirDB');

      // Verify footer links exist
      const footerLinks = page.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(2);
    });

    test('all images have proper alt text and are visible', async ({ page }) => {
      await page.goto('/');

      // Get all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      expect(imageCount).toBeGreaterThanOrEqual(2); // At least logo and usage gif

      // Verify each image has alt text
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const altText = await img.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.length).toBeGreaterThan(0);
      }
    });
  });

  test.describe('Test Case 2: Check navigation without JS - Navigation links are clickable and functional', () => {
    test.use({ javaScriptEnabled: false });

    test('header navigation is visible and accessible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify header is visible
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Verify logo link is visible
      const logoLink = page.locator('.logo-text');
      await expect(logoLink).toBeVisible();
      await expect(logoLink).toHaveText('MirDB');
      await expect(logoLink).toHaveAttribute('href', '/');

      // Verify navigation exists
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Verify navigation links
      const navLinks = page.locator('nav ul li a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(3);
    });

    test('internal anchor links are clickable and navigate to correct sections', async ({ page }) => {
      await page.goto('/');

      // Test Features anchor link
      const featuresLink = page.locator('nav a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();

      // Verify URL has the anchor
      await expect(page).toHaveURL(/#features/);

      // Navigate to Quick Start
      const quickstartLink = page.locator('nav a[href="#quickstart"]');
      await expect(quickstartLink).toBeVisible();
      await quickstartLink.click();

      // Verify URL has the anchor
      await expect(page).toHaveURL(/#quickstart/);
    });

    test('external GitHub links have proper attributes', async ({ page }) => {
      await page.goto('/');

      // Find GitHub link in navigation
      const githubNavLink = page.locator('nav a[href*="github.com"]');
      await expect(githubNavLink).toBeVisible();

      // Verify external link attributes
      await expect(githubNavLink).toHaveAttribute('target', '_blank');
      await expect(githubNavLink).toHaveAttribute('rel', /noopener/);

      // Verify the link href is correct
      await expect(githubNavLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    test('CTA buttons in hero are clickable without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify Get Started button
      const getStartedBtn = page.locator('.hero-cta a.btn-primary');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');
      await expect(getStartedBtn).toContainText('Get Started');

      // Click Get Started and verify navigation
      await getStartedBtn.click();
      await expect(page).toHaveURL(/#quickstart/);

      // Verify View on GitHub button
      const githubBtn = page.locator('.hero-cta a.btn-secondary');
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubBtn).toHaveAttribute('target', '_blank');
    });

    test('footer navigation links are functional without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify footer links
      const footerLinks = page.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(2);

      // Verify GitHub link in footer
      const githubFooterLink = page.locator('.footer-links a[href*="github.com/yetone/mirdb"]').first();
      await expect(githubFooterLink).toBeVisible();
      await expect(githubFooterLink).toHaveAttribute('target', '_blank');

      // Verify Issues link in footer
      const issuesLink = page.locator('.footer-links a[href*="issues"]');
      await expect(issuesLink).toBeVisible();
      await expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');
    });

    test('skip to content link is functional without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Verify skip link exists
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toHaveAttribute('href', '#main-content');

      // Verify main content target exists
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeAttached();
    });

    test('all navigation links have accessible text', async ({ page }) => {
      await page.goto('/');

      // Get all links
      const allLinks = page.locator('a');
      const linkCount = await allLinks.count();

      // Verify each link has either text content or an aria-label
      for (let i = 0; i < linkCount; i++) {
        const link = allLinks.nth(i);
        const textContent = await link.textContent();
        const ariaLabel = await link.getAttribute('aria-label');

        // Link should have either text content or aria-label
        const hasAccessibleName = (textContent && textContent.trim().length > 0) || (ariaLabel && ariaLabel.length > 0);

        // If the link contains an image, verify the image has alt text
        const img = link.locator('img');
        const imgCount = await img.count();
        if (imgCount > 0) {
          const altText = await img.first().getAttribute('alt');
          expect(altText || textContent?.trim() || ariaLabel).toBeTruthy();
        } else {
          expect(hasAccessibleName).toBeTruthy();
        }
      }
    });
  });
});
