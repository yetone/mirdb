const { test, expect } = require('@playwright/test');

test.describe('Footer Content (REQ-10)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer element exists at the bottom of the page', async ({ page }) => {
    // Test Case 1: Check footer exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer is a semantic HTML element
    await expect(footer).toHaveCount(1);

    // Verify footer contains expected content structure
    const footerContent = footer.locator('.footer-content');
    await expect(footerContent).toBeVisible();
  });

  test('footer contains GitHub link', async ({ page }) => {
    // Test Case 2: Check footer contains GitHub link
    const footer = page.locator('footer');
    const githubLink = footer.locator('a[href*="github.com"]').first();

    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveText('GitHub');

    // Verify link opens in new tab for external links
    await expect(githubLink).toHaveAttribute('target', '_blank');
  });

  test('footer displays copyright notice with year', async ({ page }) => {
    // Test Case 3: Check footer contains copyright
    const footer = page.locator('footer');
    const copyright = footer.locator('.copyright');

    await expect(copyright).toBeVisible();

    // Verify copyright text contains year (4-digit number for year)
    const copyrightText = await copyright.textContent();
    expect(copyrightText).toBeTruthy();
    expect(copyrightText.length).toBeGreaterThan(0);

    // Verify copyright symbol and year pattern
    expect(copyrightText).toMatch(/©\s*\d{4}/); // © followed by 4-digit year
    expect(copyrightText.toLowerCase()).toContain('mirdb');
  });

  test('footer includes license information', async ({ page }) => {
    // Test Case 4: Check footer contains license information
    const footer = page.locator('footer');
    const copyright = footer.locator('.copyright');

    await expect(copyright).toBeVisible();

    const copyrightText = await copyright.textContent();

    // Check for MIT license mention
    expect(copyrightText.toLowerCase()).toContain('mit');
  });

  test('footer contains documentation link', async ({ page }) => {
    // Additional validation: Documentation link
    const footer = page.locator('footer');
    const docsLink = footer.locator('a:has-text("Documentation")');

    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
    await expect(docsLink).toHaveAttribute('target', '_blank');
  });

  test('footer contains Issues link', async ({ page }) => {
    // Additional validation: Issues link
    const footer = page.locator('footer');
    const issuesLink = footer.locator('a:has-text("Issues")');

    await expect(issuesLink).toBeVisible();
    await expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');
    await expect(issuesLink).toHaveAttribute('target', '_blank');
  });

  test('footer links are properly styled and accessible', async ({ page }) => {
    // Accessibility: Footer links should be in a list for screen readers
    const footer = page.locator('footer');
    const footerLinks = footer.locator('.footer-links');

    await expect(footerLinks).toBeVisible();

    // Check that footer links container exists and has list items
    const listItems = footerLinks.locator('li');
    await expect(listItems).toHaveCount(3); // GitHub, Documentation, Issues
  });
});
