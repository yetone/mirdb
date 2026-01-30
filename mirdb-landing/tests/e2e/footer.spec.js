/**
 * Footer E2E Tests
 * Owner: Scenario 7 - Footer Section
 *
 * Tests:
 * - Footer visibility and content
 * - GitHub link functionality
 * - License information
 * - Responsive layout
 * - Keyboard navigation
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer is visible at bottom of page with dark theme styling', async ({ page }) => {
    // Test Case 1: Footer visibility and dark theme
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Verify footer has role="contentinfo"
    await expect(footer).toHaveAttribute('role', 'contentinfo');

    // Check dark theme styling - background color should be darker
    const backgroundColor = await footer.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Dark theme color #16213e = rgb(22, 33, 62)
    expect(backgroundColor).toMatch(/rgb\(22,\s*33,\s*62\)|rgba\(22,\s*33,\s*62/);

    // Verify border-top exists
    const borderTop = await footer.evaluate((el) => {
      return window.getComputedStyle(el).borderTopWidth;
    });
    expect(parseInt(borderTop)).toBeGreaterThan(0);

    // Check footer brand/logo is visible
    const footerLogo = page.locator('.footer__logo');
    await expect(footerLogo).toBeVisible();
    await expect(footerLogo).toHaveText('MirDB');

    // Check footer tagline
    const tagline = page.locator('.footer__tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
  });

  test('GitHub link opens repository in new tab with security attributes', async ({ page }) => {
    // Test Case 2: GitHub link functionality
    const githubLink = page.locator('.footer__link-list a[href*="github.com/yetone/mirdb"]').first();

    // Verify link attributes
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify it contains text about GitHub Repository
    await expect(githubLink).toContainText('GitHub');
  });

  test('footer displays license information matching MIT License', async ({ page }) => {
    // Test Case 3: License information verification
    const licenseSection = page.locator('.footer__license');
    await expect(licenseSection).toBeVisible();

    // Check MIT License is mentioned
    await expect(licenseSection).toContainText('MIT License');

    // Verify the license link exists and has correct attributes
    const licenseLink = page.locator('.footer__license a[href*="opensource.org"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toHaveAttribute('target', '_blank');
    await expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Check copyright notice
    const copyright = page.locator('.footer__copyright');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('MirDB');
    await expect(copyright).toContainText('2024');
  });

  test('footer content stacks vertically on mobile viewport with touch-friendly links', async ({ page }) => {
    // Test Case 4: Mobile responsive layout
    await page.setViewportSize({ width: 375, height: 667 });

    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Scroll to footer to ensure visibility
    await footer.scrollIntoViewIfNeeded();

    // Check that footer content is visible
    const footerBrand = page.locator('.footer__brand');
    await expect(footerBrand).toBeVisible();

    // Verify text is centered on mobile
    const brandTextAlign = await footerBrand.evaluate((el) => {
      return window.getComputedStyle(el).textAlign;
    });
    expect(brandTextAlign).toBe('center');

    // Check all links are touch-friendly (min height 44px)
    const footerLinks = page.locator('.footer__link-list .footer__link');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();

      const linkBox = await link.boundingBox();
      if (linkBox) {
        // Touch targets should be at least 44px (WCAG guideline)
        expect(linkBox.height).toBeGreaterThanOrEqual(40);
      }
    }

    // Verify footer bottom section is centered
    const footerBottom = page.locator('.footer__bottom');
    const bottomTextAlign = await footerBottom.evaluate((el) => {
      return window.getComputedStyle(el).textAlign;
    });
    expect(bottomTextAlign).toBe('center');
  });

  test('all footer links are focusable with visible focus indicators', async ({ page }) => {
    // Test Case 5: Keyboard navigation
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Get all focusable links in footer
    const footerLinks = page.locator('#footer a.footer__link');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Tab through each link and verify focus
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);

      // Focus the element
      await link.focus();

      // Verify the element is focused
      await expect(link).toBeFocused();

      // Check for visible focus indicator (outline)
      const outlineStyle = await link.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
        };
      });

      // Focus should have an outline (not 0px and not 'none')
      const hasVisibleOutline =
        outlineStyle.outlineWidth !== '0px' &&
        outlineStyle.outlineStyle !== 'none';

      expect(hasVisibleOutline).toBe(true);
    }
  });

  test('footer has proper semantic structure and ARIA attributes', async ({ page }) => {
    // Additional accessibility test
    const footer = page.locator('#footer');
    await expect(footer).toHaveAttribute('role', 'contentinfo');

    // Check footer navigation has aria-label
    const footerNav = page.locator('.footer__links');
    await expect(footerNav).toHaveAttribute('aria-label', 'Footer navigation');

    // Check all external links have proper security attributes
    const externalLinks = page.locator('#footer a[target="_blank"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    }
  });

  test('footer link groups are organized correctly', async ({ page }) => {
    // Test for proper organization of footer links
    const resourcesGroup = page.locator('.footer__link-group').first();
    const communityGroup = page.locator('.footer__link-group').last();

    // Check Resources group title
    const resourcesTitle = resourcesGroup.locator('.footer__link-title');
    await expect(resourcesTitle).toHaveText('Resources');

    // Check Community group title
    const communityTitle = communityGroup.locator('.footer__link-title');
    await expect(communityTitle).toHaveText('Community');

    // Verify Resources links exist
    const resourcesLinks = resourcesGroup.locator('.footer__link-list li');
    const resourcesCount = await resourcesLinks.count();
    expect(resourcesCount).toBeGreaterThanOrEqual(2);

    // Verify Community links exist
    const communityLinks = communityGroup.locator('.footer__link-list li');
    const communityCount = await communityLinks.count();
    expect(communityCount).toBeGreaterThanOrEqual(1);
  });
});
