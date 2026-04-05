import { test, expect } from '@playwright/test';

test.describe('Footer Navigation and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to footer to ensure it's visible
    await page.locator('#footer').scrollIntoViewIfNeeded();
  });

  test('TC1: Footer contains GitHub link with valid href', async ({ page }) => {
    // Query footer for GitHub link
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Find the GitHub link using data-testid
    const githubLink = footer.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify it has a valid GitHub href
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify link opens in new tab for security
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
  });

  test('TC2: Footer contains contact/community channel link', async ({ page }) => {
    // Verify contact information or community channel link is present
    const footer = page.locator('#footer');

    // Find the community/contact link
    const communityLink = footer.locator('[data-testid="community-link"]');
    await expect(communityLink).toBeVisible();

    // Verify it has a valid href
    const href = await communityLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify the link text indicates community/contact
    const linkText = await communityLink.textContent();
    expect(linkText?.toLowerCase()).toMatch(/community|contact|discuss|support/);
  });

  test('TC3: Footer displays license information', async ({ page }) => {
    // Verify license information or link to license is displayed
    const footer = page.locator('#footer');

    // Find the license link
    const licenseLink = footer.locator('[data-testid="license-link"]');
    await expect(licenseLink).toBeVisible();

    // Verify it has a valid href pointing to license
    const href = await licenseLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href?.toLowerCase()).toMatch(/license/i);

    // Also check for license text in the copyright section
    const copyrightText = await footer.locator('.footer__copyright').textContent();
    expect(copyrightText?.toLowerCase()).toMatch(/license|mit|apache|bsd/);
  });

  test('TC4: GitHub link navigates to correct repository URL', async ({ page }) => {
    // Query footer for GitHub link
    const footer = page.locator('#footer');
    const githubLink = footer.locator('[data-testid="github-link"]');

    // Verify the link is present and clickable
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toBeEnabled();

    // Get the href attribute
    const href = await githubLink.getAttribute('href');

    // Verify it points to the correct GitHub repository
    expect(href).toBe('https://github.com/mirdb/mirdb');

    // Verify the link is accessible (has proper ARIA attributes or text)
    const hasAccessibleName = await githubLink.evaluate(el => {
      const text = el.textContent?.trim();
      const ariaLabel = el.getAttribute('aria-label');
      return (text && text.length > 0) || (ariaLabel && ariaLabel.length > 0);
    });
    expect(hasAccessibleName).toBeTruthy();
  });

  test('Footer section is visible and has proper structure', async ({ page }) => {
    // Additional structural test for footer
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Verify footer has proper semantic role
    await expect(footer).toHaveAttribute('role', 'contentinfo');

    // Verify footer contains navigation links section
    const footerNav = footer.locator('.footer__links');
    await expect(footerNav).toBeVisible();

    // Verify footer has brand/logo section
    const brand = footer.locator('.footer__brand');
    await expect(brand).toBeVisible();

    // Verify footer has copyright section
    const copyright = footer.locator('.footer__copyright');
    await expect(copyright).toBeVisible();
  });

  test('Footer links have proper accessibility attributes', async ({ page }) => {
    const footer = page.locator('#footer');

    // Get all external links in footer
    const externalLinks = footer.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    // Verify each external link has rel="noopener" for security
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }

    // Verify footer navigation has proper ARIA label
    const footerNav = footer.locator('nav.footer__links');
    await expect(footerNav).toHaveAttribute('aria-label');
  });
});
