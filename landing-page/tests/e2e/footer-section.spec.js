/**
 * Footer Section E2E Tests
 * Owner: Scenario 7 - Footer Section
 * Tests for footer display, GitHub link functionality, license info, and author attribution
 */
const { test, expect } = require('@playwright/test');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Step 1: Footer Visibility', () => {
    test('Footer is visible when scrolling to bottom of page', async ({ page }) => {
      // Scroll to the footer section
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Verify footer is visible
      await expect(footer).toBeVisible();
    });

    test('Footer has proper structure', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Check content section exists
      const footerContent = page.locator('.footer__content');
      await expect(footerContent).toBeVisible();

      // Check bottom section exists
      const footerBottom = page.locator('.footer__bottom');
      await expect(footerBottom).toBeVisible();
    });
  });

  test.describe('Step 2: Footer Content Verification', () => {
    test('TC1: Footer contains GitHub link, license info, author attribution, and copyright notice', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // GitHub link exists
      const githubLink = page.locator('.footer__github-link');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // License info exists
      const licenseInfo = page.locator('.footer__license');
      await expect(licenseInfo).toBeVisible();
      await expect(licenseInfo).toContainText('License');

      // Author attribution exists
      const attribution = page.locator('.footer__attribution');
      await expect(attribution).toBeVisible();
      await expect(attribution).toContainText('yetone');

      // Copyright notice exists
      const copyright = page.locator('.footer__copyright');
      await expect(copyright).toBeVisible();
      await expect(copyright).toContainText('MirDB');
    });

    test('TC3: License information is displayed', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // License label is visible
      const licenseLabel = page.locator('.footer__license-label');
      await expect(licenseLabel).toBeVisible();
      await expect(licenseLabel).toHaveText('License:');

      // License link exists and points to repository
      const licenseLink = page.locator('.footer__license-link');
      await expect(licenseLink).toBeVisible();
      await expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    test('TC4: Author yetone is credited in footer', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Attribution section exists
      const attribution = page.locator('.footer__attribution');
      await expect(attribution).toBeVisible();

      // Author link exists with correct href
      const authorLink = page.locator('.footer__author-link');
      await expect(authorLink).toBeVisible();
      await expect(authorLink).toHaveText('yetone');
      await expect(authorLink).toHaveAttribute('href', 'https://github.com/yetone');
      await expect(authorLink).toHaveAttribute('data-author', 'yetone');
    });
  });

  test.describe('Step 3: Footer Link Functionality', () => {
    test('TC2: Click GitHub repository link opens https://github.com/yetone/mirdb in new tab', async ({ page, context }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Listen for new page (tab) to open
      const pagePromise = context.waitForEvent('page');

      // Click GitHub link
      const githubLink = page.locator('.footer__github-link');
      await expect(githubLink).toBeVisible();
      await githubLink.click();

      // Wait for new tab to open
      const newPage = await pagePromise;
      await newPage.waitForLoadState();

      // Verify new tab URL is GitHub repository
      const newPageUrl = newPage.url();
      expect(newPageUrl).toContain('github.com/yetone/mirdb');
    });

    test('GitHub link has correct attributes for new tab', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      const githubLink = page.locator('.footer__github-link');
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('Author link opens in new tab', async ({ page, context }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Listen for new page (tab) to open
      const pagePromise = context.waitForEvent('page');

      // Click author link
      const authorLink = page.locator('.footer__author-link');
      await authorLink.click();

      // Wait for new tab to open
      const newPage = await pagePromise;
      await newPage.waitForLoadState();

      // Verify new tab URL is author's GitHub profile
      const newPageUrl = newPage.url();
      expect(newPageUrl).toContain('github.com/yetone');
    });
  });

  test.describe('Footer Navigation', () => {
    test('Footer navigation links scroll to correct sections', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Test Features link
      const featuresLink = page.locator('.footer__link[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();

      // Wait for smooth scroll
      await page.waitForTimeout(1000);

      // Verify Features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('Footer has all navigation links', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Check all navigation links exist
      await expect(page.locator('.footer__link[href="#features"]')).toBeVisible();
      await expect(page.locator('.footer__link[href="#usage"]')).toBeVisible();
      await expect(page.locator('.footer__link[href="#architecture"]')).toBeVisible();
      await expect(page.locator('.footer__link[href="#getting-started"]')).toBeVisible();
    });
  });

  test.describe('Footer Branding', () => {
    test('Footer displays logo and tagline', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Logo exists
      const logo = page.locator('.footer__logo img');
      await expect(logo).toBeVisible();
      await expect(logo).toHaveAttribute('src', 'assets/images/logo.gif');

      // Tagline exists
      const tagline = page.locator('.footer__tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');
    });

    test('Footer logo links to homepage', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      const logoLink = page.locator('.footer__logo');
      await expect(logoLink).toHaveAttribute('href', '#');
    });
  });

  test.describe('Footer Accessibility', () => {
    test('Footer has proper ARIA role', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await expect(footer).toHaveAttribute('role', 'contentinfo');
    });

    test('Footer navigation has aria-label', async ({ page }) => {
      const footerNav = page.locator('.footer__nav');
      await expect(footerNav).toHaveAttribute('aria-label', 'Footer navigation');
    });

    test('GitHub link has accessible label', async ({ page }) => {
      const githubLink = page.locator('.footer__github-link');
      await expect(githubLink).toHaveAttribute('aria-label', 'View MirDB on GitHub');
    });
  });

  test.describe('Footer Styling', () => {
    test('Footer has correct background color', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      // Footer should have the secondary background color
      await expect(footer).toHaveCSS('background-color', 'rgb(22, 33, 62)');
    });

    test('Footer links have hover effects', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await footer.scrollIntoViewIfNeeded();

      const githubLink = page.locator('.footer__github-link');

      // Get initial color
      const initialColor = await githubLink.evaluate(el =>
        getComputedStyle(el).color
      );

      // Hover over the link
      await githubLink.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Color should change on hover
      const hoverColor = await githubLink.evaluate(el =>
        getComputedStyle(el).color
      );

      // Verify color changed (hover effect working)
      expect(hoverColor).not.toBe(initialColor);
    });
  });
});
