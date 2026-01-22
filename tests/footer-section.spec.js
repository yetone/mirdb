// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to footer to make it visible
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  });

  test('TC1: Footer displays author attribution - yetone', async ({ page }) => {
    // Find the footer section
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Find the author attribution element
    const footerAuthor = page.locator('.footer-author');
    await expect(footerAuthor).toBeVisible();

    // Verify the author name is displayed
    const authorText = await footerAuthor.textContent();
    expect(authorText).toContain('yetone');

    // Verify the "Created by" text with author name
    await expect(footerAuthor).toContainText('Created by');
    await expect(footerAuthor).toContainText('yetone');
  });

  test('TC2: Footer displays or links to author email - yetoneful@gmail.com', async ({ page }) => {
    // Find the footer section
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Find the email link within footer author section
    const emailLink = page.locator('.footer-author a[href^="mailto:"]');
    await expect(emailLink).toBeVisible();

    // Verify the mailto link contains the correct email
    await expect(emailLink).toHaveAttribute('href', 'mailto:yetoneful@gmail.com');

    // Verify the link text is the author name (yetone)
    await expect(emailLink).toHaveText('yetone');
  });

  test('TC3: Footer contains link to GitHub repository', async ({ page }) => {
    // Find the footer links section
    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Find the GitHub link in footer
    const githubLink = page.locator('.footer-links a[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Verify the link text
    await expect(githubLink).toHaveText('GitHub');

    // Verify the correct URL
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify security attributes for external link
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('TC4: Footer contains CircleCI build status badge', async ({ page }) => {
    // Find the footer links section
    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Find the CircleCI badge link in footer
    const badgeLink = page.locator('.footer-links a[href*="circleci.com"]');
    await expect(badgeLink).toBeVisible();

    // Verify the link points to the correct CircleCI project page
    await expect(badgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');

    // Verify security attributes for external link
    await expect(badgeLink).toHaveAttribute('target', '_blank');
    await expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Find the badge image within the link
    const badgeImage = page.locator('.footer-links a[href*="circleci.com"] img');
    await expect(badgeImage).toBeVisible();

    // Verify the badge image source
    await expect(badgeImage).toHaveAttribute('src', /circleci\.com\/gh\/yetone\/mirdb\.svg/);

    // Verify the badge has accessible alt text
    const altText = await badgeImage.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toContain('circleci');
    expect(altText.toLowerCase()).toContain('build');
    expect(altText.toLowerCase()).toContain('status');
  });

  test('Footer section is accessible and has proper structure', async ({ page }) => {
    // Find the footer element
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer has proper container structure
    const footerContainer = page.locator('footer.footer .container');
    await expect(footerContainer).toBeVisible();

    // Verify footer content wrapper
    const footerContent = page.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // Verify footer brand section exists
    const footerBrand = page.locator('.footer-brand');
    await expect(footerBrand).toBeVisible();

    // Verify footer links section exists
    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();
  });
});
