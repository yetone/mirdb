const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display and Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Project logo is visible and properly sized', async ({ page }) => {
    // Test Case 1: Load homepage and inspect hero section - Project logo is visible and properly sized
    const logo = page.locator('#logo');

    // Verify logo is visible
    await expect(logo).toBeVisible();

    // Verify logo has proper src attribute
    await expect(logo).toHaveAttribute('src', '../assets/logo.gif');

    // Verify logo has alt text for accessibility
    await expect(logo).toHaveAttribute('alt', 'MirDB Logo');

    // Verify logo is properly sized (max-width: 200px as per CSS)
    const boundingBox = await logo.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.width).toBeGreaterThan(0);
    expect(boundingBox.width).toBeLessThanOrEqual(200);
  });

  test('TC2: Headline clearly communicates MirDB core value proposition', async ({ page }) => {
    // Test Case 2: Check hero section headline - communicates MirDB's core value proposition
    const projectName = page.locator('#project-name');
    const description = page.locator('#description');

    // Verify project name is displayed
    await expect(projectName).toBeVisible();
    await expect(projectName).toHaveText('MirDB');

    // Verify description mentions key value propositions
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();

    // Check for persistent storage mention
    expect(descriptionText.toLowerCase()).toContain('persistent');

    // Check for memcached compatibility mention
    expect(descriptionText.toLowerCase()).toContain('memcached');

    // Check for Rust performance mention
    expect(descriptionText.toLowerCase()).toContain('rust');
  });

  test('TC3: Tagline reads Persistent Key-Value Store with Memcached Protocol', async ({ page }) => {
    // Test Case 3: Check tagline content - reads expected text or similar
    const tagline = page.locator('#tagline');

    // Verify tagline is visible
    await expect(tagline).toBeVisible();

    // Verify tagline contains the expected text
    const taglineText = await tagline.textContent();
    expect(taglineText).toBe('Persistent Key-Value Store with Memcached Protocol');
  });

  test('TC4: Get Started button is visible and clickable', async ({ page }) => {
    // Test Case 4: Verify primary CTA button - Get Started button is visible and clickable
    const getStartedBtn = page.locator('#get-started-btn');

    // Verify button is visible
    await expect(getStartedBtn).toBeVisible();

    // Verify button text
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify button has correct href (links to getting-started section)
    await expect(getStartedBtn).toHaveAttribute('href', '#getting-started');

    // Verify button is clickable (has proper role/element type)
    await expect(getStartedBtn).toBeEnabled();

    // Verify button has primary styling class
    await expect(getStartedBtn).toHaveClass(/btn-primary/);
  });

  test('TC5: View on GitHub button is visible and links to repository', async ({ page }) => {
    // Test Case 5: Verify secondary CTA button - View on GitHub button links to repository
    const githubBtn = page.locator('#github-btn');

    // Verify button is visible
    await expect(githubBtn).toBeVisible();

    // Verify button text
    await expect(githubBtn).toHaveText('View on GitHub');

    // Verify button links to GitHub repository
    await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify link opens in new tab
    await expect(githubBtn).toHaveAttribute('target', '_blank');

    // Verify security attribute
    await expect(githubBtn).toHaveAttribute('rel', 'noopener noreferrer');

    // Verify button has secondary styling class
    await expect(githubBtn).toHaveClass(/btn-secondary/);
  });

  test('Hero section is rendered above the fold', async ({ page }) => {
    // Additional test: Verify hero section is the first visible area
    const hero = page.locator('#hero');

    // Verify hero section exists
    await expect(hero).toBeVisible();

    // Verify hero section is positioned at the top
    const heroBox = await hero.boundingBox();
    expect(heroBox.y).toBe(0);
  });

  test('Hero section contains all required elements', async ({ page }) => {
    // Comprehensive test for all hero section elements
    const hero = page.locator('#hero');
    const logo = page.locator('#logo');
    const projectName = page.locator('#project-name');
    const tagline = page.locator('#tagline');
    const description = page.locator('#description');
    const getStartedBtn = page.locator('#get-started-btn');
    const githubBtn = page.locator('#github-btn');

    // Verify all elements are within hero section and visible
    await expect(hero).toBeVisible();
    await expect(logo).toBeVisible();
    await expect(projectName).toBeVisible();
    await expect(tagline).toBeVisible();
    await expect(description).toBeVisible();
    await expect(getStartedBtn).toBeVisible();
    await expect(githubBtn).toBeVisible();
  });
});
