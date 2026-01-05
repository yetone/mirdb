const { test, expect } = require('@playwright/test');

test.describe('GitHub Repository Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: GitHub link is present in header navigation', async ({ page }) => {
    // Locate the header navigation
    const navLinks = page.locator('header .nav-links');
    await expect(navLinks).toBeVisible();

    // Find the GitHub link in navigation (specific to exact repo URL without #readme)
    const githubLink = navLinks.locator('a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify the link text contains "GitHub"
    const linkText = await githubLink.textContent();
    expect(linkText).toContain('GitHub');
  });

  test('TC2: GitHub link is present in footer', async ({ page }) => {
    // Locate the footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find the GitHub link in footer
    const footerLinks = footer.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    const githubLink = footerLinks.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Verify the link text contains "GitHub"
    const linkText = await githubLink.textContent();
    expect(linkText).toContain('GitHub');
  });

  test('TC3: GitHub links navigate to valid repository URL', async ({ page }) => {
    // Get header GitHub link (specific to exact repo URL)
    const headerGithubLink = page.locator('header .nav-links a[href="https://github.com/yetone/mirdb"]');
    const headerHref = await headerGithubLink.getAttribute('href');

    // Verify it's a valid GitHub URL
    expect(headerHref).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab
    const headerTarget = await headerGithubLink.getAttribute('target');
    expect(headerTarget).toBe('_blank');

    // Get footer GitHub link
    const footerGithubLink = page.locator('footer .footer-links a[href="https://github.com/yetone/mirdb"]');
    const footerHref = await footerGithubLink.getAttribute('href');

    // Verify footer link is valid
    expect(footerHref).toBe('https://github.com/yetone/mirdb');

    // Verify footer link opens in new tab
    const footerTarget = await footerGithubLink.getAttribute('target');
    expect(footerTarget).toBe('_blank');
  });

  test('TC4: GitHub links have correct href attribute pointing to github.com', async ({ page }) => {
    // Get all GitHub links on the page
    const allGithubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');

    // Verify there are at least 2 GitHub links (header and footer)
    const linkCount = await allGithubLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(2);

    // Verify all links point to the correct repository
    for (let i = 0; i < linkCount; i++) {
      const link = allGithubLinks.nth(i);
      const href = await link.getAttribute('href');

      // Verify href starts with the correct GitHub repository URL
      expect(href).toMatch(/^https:\/\/github\.com\/yetone\/mirdb/);
    }
  });

  test('Header GitHub link is accessible via keyboard navigation', async ({ page }) => {
    // Navigate to the header nav area
    const navLinks = page.locator('header .nav-links a');

    // Verify the GitHub link can be focused (specific to exact repo URL)
    const githubLink = page.locator('header .nav-links a[href="https://github.com/yetone/mirdb"]');
    await githubLink.focus();

    // Verify the link is focused
    const isFocused = await githubLink.evaluate((el) => document.activeElement === el);
    expect(isFocused).toBe(true);
  });

  test('Footer GitHub link is styled consistently with other footer links', async ({ page }) => {
    // Get footer links
    const footerLinks = page.locator('footer .footer-links a');
    const githubLink = page.locator('footer .footer-links a[href="https://github.com/yetone/mirdb"]');

    // Verify GitHub link exists
    await expect(githubLink).toBeVisible();

    // Get computed style of GitHub link
    const githubColor = await githubLink.evaluate((el) =>
      window.getComputedStyle(el).color
    );

    // Get computed style of another footer link for comparison
    const firstLink = footerLinks.first();
    const firstLinkColor = await firstLink.evaluate((el) =>
      window.getComputedStyle(el).color
    );

    // Verify consistent styling
    expect(githubColor).toBe(firstLinkColor);
  });
});
