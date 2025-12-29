// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.join(__dirname, '..', 'index.html');

test.describe('Footer and Navigation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test('TC1: Footer section exists at the bottom of the page', async ({ page }) => {
    // Locate the footer section
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer has role="contentinfo" for accessibility
    const role = await footer.getAttribute('role');
    expect(role).toBe('contentinfo');

    // Verify footer is positioned at the bottom by checking it comes after main content
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Footer should be a sibling that appears after main
    const footerBox = await footer.boundingBox();
    const mainBox = await main.boundingBox();
    expect(footerBox).toBeTruthy();
    expect(mainBox).toBeTruthy();
    expect(footerBox.y).toBeGreaterThan(mainBox.y);
  });

  test('TC2: GitHub link is present and points to MirDB repository', async ({ page }) => {
    // Locate the GitHub link in the footer
    const footer = page.locator('footer.footer');
    const githubLink = footer.locator('a.footer-link-github');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toContainText('GitHub');

    // Verify the href points to the MirDB repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC3: Documentation link is present and functional', async ({ page }) => {
    // Locate the Documentation link in the footer
    const footer = page.locator('footer.footer');
    const docsLink = footer.locator('a.footer-link-docs');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toContainText('Documentation');

    // Verify the href points to documentation (README)
    const href = await docsLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb#readme');

    // Verify it opens in a new tab
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC4: License link is present and points to license information', async ({ page }) => {
    // Locate the License link in the footer
    const footer = page.locator('footer.footer');
    const licenseLink = footer.locator('a.footer-link-license');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toContainText('License');

    // Verify the href points to the LICENSE file
    const href = await licenseLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb/blob/master/LICENSE');

    // Verify it opens in a new tab
    const target = await licenseLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Also verify the inline license link in the footer text
    const inlineLicenseLink = footer.locator('a.license-link');
    await expect(inlineLicenseLink).toBeVisible();
    await expect(inlineLicenseLink).toContainText('MIT License');
    const inlineHref = await inlineLicenseLink.getAttribute('href');
    expect(inlineHref).toBe('https://github.com/yetone/mirdb/blob/master/LICENSE');
  });

  test('TC5: Version number is displayed in the footer', async ({ page }) => {
    // Locate the version information in the footer
    const footer = page.locator('footer.footer');
    const versionText = footer.locator('.version');
    await expect(versionText).toBeVisible();

    // Verify version text contains a version number pattern
    const text = await versionText.textContent();
    expect(text).toMatch(/Version \d+\.\d+\.\d+/);
    expect(text).toContain('0.1.0');
  });
});
