// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Navigation Header Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Navigation bar contains Logo, Features, Commands, Quick Start, and GitHub links', async ({ page }) => {
    // Verify navigation bar is present
    const navbar = page.locator('header.navbar').first();
    await expect(navbar).toBeVisible();

    // Check Logo is present
    const logo = page.locator('.logo, [data-testid="logo"], nav a:has-text("MirDB")').first();
    await expect(logo).toBeVisible();
    await expect(logo).toContainText('MirDB');

    // Check Features link is present
    const featuresLink = page.locator('nav a[href="#features"], nav a:has-text("Features")').first();
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toContainText('Features');

    // Check Commands link is present
    const commandsLink = page.locator('nav a[href="#commands"], nav a:has-text("Commands")').first();
    await expect(commandsLink).toBeVisible();
    await expect(commandsLink).toContainText('Commands');

    // Check Quick Start link is present
    const quickStartLink = page.locator('nav a[href="#quick-start"], nav a:has-text("Quick Start")').first();
    await expect(quickStartLink).toBeVisible();
    await expect(quickStartLink).toContainText('Quick Start');

    // Check GitHub link is present
    const githubLink = page.locator('nav a:has-text("GitHub")').first();
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toContainText('GitHub');
  });

  test('TC2: Features navigation link scrolls to Key Features section', async ({ page }) => {
    // Click Features navigation link
    const featuresLink = page.locator('nav a[href="#features"], nav a:has-text("Features")').first();
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Verify URL contains #features hash
    await expect(page).toHaveURL(/#features/);

    // Verify Key Features section is visible
    const featuresSection = page.locator('#features, [data-testid="features"]');
    await expect(featuresSection).toBeVisible();

    // Verify section heading
    const featuresHeading = featuresSection.locator('h2');
    await expect(featuresHeading).toContainText('Features');
  });

  test('TC3: Commands navigation link scrolls to Supported Commands section', async ({ page }) => {
    // Click Commands navigation link
    const commandsLink = page.locator('nav a[href="#commands"], nav a:has-text("Commands")').first();
    await expect(commandsLink).toBeVisible();
    await commandsLink.click();

    // Verify URL contains #commands hash
    await expect(page).toHaveURL(/#commands/);

    // Verify Supported Commands section is visible
    const commandsSection = page.locator('#commands, [data-testid="commands"]');
    await expect(commandsSection).toBeVisible();

    // Verify section heading
    const commandsHeading = commandsSection.locator('h2');
    await expect(commandsHeading).toContainText('Commands');
  });

  test('TC4: Quick Start navigation link scrolls to Quick Start section', async ({ page }) => {
    // Click Quick Start navigation link
    const quickStartLink = page.locator('nav a[href="#quick-start"], nav a:has-text("Quick Start")').first();
    await expect(quickStartLink).toBeVisible();
    await quickStartLink.click();

    // Verify URL contains #quick-start hash
    await expect(page).toHaveURL(/#quick-start/);

    // Verify Quick Start section is visible
    const quickStartSection = page.locator('#quick-start, [data-testid="quick-start"]');
    await expect(quickStartSection).toBeVisible();

    // Verify section heading
    const quickStartHeading = quickStartSection.locator('h2');
    await expect(quickStartHeading).toContainText('Quick Start');
  });

  test('TC5: GitHub navigation link opens GitHub repository URL', async ({ page }) => {
    // Get GitHub navigation link
    const githubLink = page.locator('nav a:has-text("GitHub")').first();
    await expect(githubLink).toBeVisible();

    // Verify link href points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');

    // Verify link opens in new tab (target="_blank")
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes for external link
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
