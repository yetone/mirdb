/**
 * Footer Section E2E Tests
 * Owner: Scenario 9 - Footer Section
 *
 * End-to-end tests for footer functionality:
 * - Test Case 2: Privacy Policy link navigation
 * - Test Case 3: Terms of Service link navigation
 * - Test Case 5: Footer mobile viewport accessibility
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section - Link Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC2: Click Privacy Policy link navigates to privacy page', async ({ page }) => {
    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find and click the Privacy Policy link
    const privacyLink = page.getByTestId('footer-link-privacy-policy');
    await expect(privacyLink).toBeVisible();
    await expect(privacyLink).toHaveAttribute('href', '/privacy');

    // Click the link
    await privacyLink.click();

    // Verify navigation to privacy page
    await page.waitForURL('**/privacy');
    expect(page.url()).toContain('/privacy');
  });

  test('TC3: Click Terms of Service link navigates to terms page', async ({ page }) => {
    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find and click the Terms of Service link
    const termsLink = page.getByTestId('footer-link-terms-of-service');
    await expect(termsLink).toBeVisible();
    await expect(termsLink).toHaveAttribute('href', '/terms');

    // Click the link
    await termsLink.click();

    // Verify navigation to terms page
    await page.waitForURL('**/terms');
    expect(page.url()).toContain('/terms');
  });

  test('Click Contact link navigates to contact page', async ({ page }) => {
    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find and click the Contact link
    const contactLink = page.getByTestId('footer-link-contact');
    await expect(contactLink).toBeVisible();
    await expect(contactLink).toHaveAttribute('href', '/contact');

    // Click the link
    await contactLink.click();

    // Verify navigation to contact page
    await page.waitForURL('**/contact');
    expect(page.url()).toContain('/contact');
  });

  test('All footer links are accessible and have proper href attributes', async ({ page }) => {
    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();

    // Verify all links are present and accessible
    const links = [
      { testId: 'footer-link-privacy-policy', href: '/privacy', text: 'Privacy Policy' },
      { testId: 'footer-link-terms-of-service', href: '/terms', text: 'Terms of Service' },
      { testId: 'footer-link-contact', href: '/contact', text: 'Contact' },
    ];

    for (const link of links) {
      const linkElement = page.getByTestId(link.testId);
      await expect(linkElement).toBeVisible();
      await expect(linkElement).toHaveAttribute('href', link.href);
      await expect(linkElement).toHaveText(link.text);
    }
  });
});

test.describe('Footer Section - Mobile Responsiveness', () => {
  test('TC5: Footer links stack vertically on mobile viewport (320px)', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify all links are visible and accessible
    const privacyLink = page.getByTestId('footer-link-privacy-policy');
    const termsLink = page.getByTestId('footer-link-terms-of-service');
    const contactLink = page.getByTestId('footer-link-contact');

    await expect(privacyLink).toBeVisible();
    await expect(termsLink).toBeVisible();
    await expect(contactLink).toBeVisible();

    // Verify links are stacked vertically (using flex-col on mobile)
    const footerNav = page.getByTestId('footer-nav');
    const navList = footerNav.locator('ul');
    const navClasses = await navList.getAttribute('class');

    // Should have flex-col for mobile stacking
    expect(navClasses).toContain('flex-col');

    // Verify no horizontal scroll on mobile
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify copyright is visible
    const copyright = page.getByTestId('footer-copyright');
    await expect(copyright).toBeVisible();
  });

  test('TC5: Footer links remain accessible on iPhone viewport (428px)', async ({ page }) => {
    // Set iPhone viewport
    await page.setViewportSize({ width: 428, height: 926 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // All links should be visible and clickable
    const links = [
      page.getByTestId('footer-link-privacy-policy'),
      page.getByTestId('footer-link-terms-of-service'),
      page.getByTestId('footer-link-contact'),
    ];

    for (const link of links) {
      await expect(link).toBeVisible();
      await expect(link).toBeEnabled();
    }

    // Verify copyright is visible
    const copyright = page.getByTestId('footer-copyright');
    await expect(copyright).toBeVisible();
  });

  test('Footer displays horizontal layout on tablet viewport (768px)', async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer content has responsive classes for horizontal layout
    const footerContent = footer.locator('.flex');
    await expect(footerContent.first()).toBeVisible();

    const contentClasses = await footerContent.first().getAttribute('class');
    // Should have md:flex-row for tablet/desktop horizontal layout
    expect(contentClasses).toContain('md:flex-row');

    // All links should be visible
    const links = [
      page.getByTestId('footer-link-privacy-policy'),
      page.getByTestId('footer-link-terms-of-service'),
      page.getByTestId('footer-link-contact'),
    ];

    for (const link of links) {
      await expect(link).toBeVisible();
    }
  });

  test('Footer maintains proper spacing on desktop viewport (1024px)', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer has proper structure
    const footerNav = page.getByTestId('footer-nav');
    await expect(footerNav).toBeVisible();

    const copyright = page.getByTestId('footer-copyright');
    await expect(copyright).toBeVisible();

    // Verify links have proper spacing (gap classes)
    const navList = footerNav.locator('ul');
    const navClasses = await navList.getAttribute('class');
    expect(navClasses).toContain('gap-');
    // Should have sm:gap-6 for spacing between links
    expect(navClasses).toContain('sm:gap-6');
  });
});

test.describe('Footer Section - Copyright', () => {
  test('Copyright displays current year', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();

    // Verify copyright contains current year
    const copyright = page.getByTestId('footer-copyright');
    await expect(copyright).toBeVisible();

    const currentYear = new Date().getFullYear().toString();
    await expect(copyright).toContainText(currentYear);
  });

  test('Copyright displays company name', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();

    // Verify copyright contains company name
    const copyright = page.getByTestId('footer-copyright');
    await expect(copyright).toContainText('URL Shortener');
  });
});

test.describe('Footer Section - Accessibility', () => {
  test('Footer has proper semantic structure', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();

    // Verify footer has contentinfo role
    await expect(footer).toHaveAttribute('role', 'contentinfo');

    // Verify navigation has proper aria-label
    const footerNav = page.getByTestId('footer-nav');
    await expect(footerNav).toHaveAttribute('aria-label', 'Footer navigation');
  });

  test('Footer links are keyboard accessible', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();

    // Tab to the first footer link
    const privacyLink = page.getByTestId('footer-link-privacy-policy');
    await privacyLink.focus();
    await expect(privacyLink).toBeFocused();

    // Verify focus is visible (link has focus classes)
    const linkClasses = await privacyLink.getAttribute('class');
    expect(linkClasses).toContain('focus:');
  });
});
