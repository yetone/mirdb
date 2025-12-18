import { test, expect } from '@playwright/test';

test.describe('Footer Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer element exists at bottom of page', async ({ page }) => {
    // Check that the footer element exists
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer is a semantic footer element
    const footerElement = page.locator('footer[data-testid="footer"]');
    await expect(footerElement).toBeVisible();

    // Scroll to bottom and verify footer is visible
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeInViewport();
  });

  test('TC2: Link to documentation is present in footer', async ({ page }) => {
    // Navigate to the footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Check for documentation link
    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify it has proper text content
    await expect(docsLink).toContainText('Documentation');

    // Verify it's an anchor element with href
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
  });

  test('TC3: Link to GitHub repository is present in footer', async ({ page }) => {
    // Navigate to the footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Check for GitHub link
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify it has proper text content
    await expect(githubLink).toContainText('GitHub');

    // Verify it links to the GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');
  });

  test('TC4: License information or link is present in footer', async ({ page }) => {
    // Navigate to the footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Check for license information
    const licenseInfo = page.locator('[data-testid="license-info"]');
    await expect(licenseInfo).toBeVisible();

    // Verify it contains license text (MIT, Apache, or similar)
    const licenseText = await licenseInfo.textContent();
    expect(licenseText).toContain('License');
    expect(licenseText).toContain('MIT');
  });

  test('Footer links section exists with proper structure', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Check footer links section exists
    const footerLinks = page.locator('[data-testid="footer-links"]');
    await expect(footerLinks).toBeVisible();

    // Verify it has a heading
    const linksHeading = footerLinks.locator('h4');
    await expect(linksHeading).toContainText('Links');

    // Verify it contains a list of links
    const linksList = footerLinks.locator('ul');
    await expect(linksList).toBeVisible();

    const linkItems = footerLinks.locator('li a');
    const count = await linkItems.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test('Project status is displayed in footer', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Check footer status section exists
    const footerStatus = page.locator('[data-testid="footer-status"]');
    await expect(footerStatus).toBeVisible();

    // Verify project status indicator
    const projectStatus = page.locator('[data-testid="project-status"]');
    await expect(projectStatus).toBeVisible();
    const statusText = await projectStatus.textContent();
    expect(statusText).toBeTruthy();
  });
});
