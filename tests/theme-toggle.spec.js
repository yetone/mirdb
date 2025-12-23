// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Dark Mode / Light Mode Theme', () => {
  // Note: We don't clear localStorage in beforeEach for the persistence test
  // Instead, we handle state management per-test as needed

  test('Test Case 1: Theme toggle control is visible in the header', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    // Find the theme toggle button in the header
    const themeToggle = page.locator('.theme-toggle');

    // Verify the theme toggle is visible
    await expect(themeToggle).toBeVisible();

    // Verify it's within the header/nav
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Verify the toggle is inside the nav
    const navToggle = page.locator('.nav .theme-toggle');
    await expect(navToggle).toBeVisible();

    // Verify the toggle has accessible label
    await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle theme');

    // Verify the theme icon is present
    const themeIcon = page.locator('.theme-icon');
    await expect(themeIcon).toBeVisible();
  });

  test('Test Case 2: Click theme toggle to enable dark mode', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    const themeToggle = page.locator('.theme-toggle');
    const html = page.locator('html');

    // Get initial theme state - default should be light (or system preference)
    // First click should toggle to dark mode
    await themeToggle.click();

    // Verify the data-theme attribute changes to dark
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Verify the page has dark background
    const body = page.locator('body');
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Dark mode background should be dark (rgb(15, 15, 35) = #0f0f23)
    expect(bgColor).toBe('rgb(15, 15, 35)');

    // Verify the theme icon changes to sun emoji for dark mode
    const themeIcon = page.locator('.theme-icon');
    await expect(themeIcon).toHaveText('☀️');
  });

  test('Test Case 3: Verify text contrast in dark mode', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    const themeToggle = page.locator('.theme-toggle');

    // Switch to dark mode
    await themeToggle.click();

    // Wait for theme to apply
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

    // Check text color on body
    const body = page.locator('body');
    const textColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Text should be white (rgb(255, 255, 255)) in dark mode
    expect(textColor).toBe('rgb(255, 255, 255)');

    // Check hero title is readable
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    // Check feature cards text is readable
    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();
    await expect(firstCard).toBeVisible();

    // Verify section headings are visible and readable
    const featuresHeading = page.locator('.features h2');
    await expect(featuresHeading).toBeVisible();

    // Check navigation links are readable
    const navLinks = page.locator('.nav-links a');
    await expect(navLinks.first()).toBeVisible();
  });

  test('Test Case 4: Verify code blocks in dark mode', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    const themeToggle = page.locator('.theme-toggle');

    // Switch to dark mode
    await themeToggle.click();
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

    // Navigate to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Check that code blocks are visible
    const codeBlocks = page.locator('.code-block, pre');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify code block has appropriate dark styling
    const codeBlock = page.locator('.code-block').first();
    const codeBgColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Code block should have dark background (rgb(30, 30, 30) = #1e1e1e)
    expect(codeBgColor).toBe('rgb(30, 30, 30)');

    // Verify syntax highlighting elements are present
    const tokenComment = page.locator('.token.comment').first();
    if (await tokenComment.count() > 0) {
      await expect(tokenComment).toBeVisible();
    }

    // Verify code text is readable (light colored)
    const codeText = page.locator('.code-block code').first();
    const codeTextColor = await codeText.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Code text should be light colored for readability
    // Expected: rgb(212, 212, 212) = #d4d4d4
    expect(codeTextColor).toBe('rgb(212, 212, 212)');
  });

  test('Test Case 5: Toggle back to light mode', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    const themeToggle = page.locator('.theme-toggle');
    const html = page.locator('html');

    // First, switch to dark mode
    await themeToggle.click();
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Now toggle back to light mode
    await themeToggle.click();
    await expect(html).toHaveAttribute('data-theme', 'light');

    // Verify the page has light background
    const body = page.locator('body');
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Light mode background should be white (rgb(255, 255, 255) = #ffffff)
    expect(bgColor).toBe('rgb(255, 255, 255)');

    // Verify text is dark
    const textColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Text should be dark (rgb(26, 26, 46) = #1a1a2e) in light mode
    expect(textColor).toBe('rgb(26, 26, 46)');

    // Verify the theme icon changes back to moon emoji
    const themeIcon = page.locator('.theme-icon');
    await expect(themeIcon).toHaveText('🌙');
  });

  test('Test Case 6: Check theme persistence across page reload', async ({ page }) => {
    // First, clear any existing theme preference and navigate
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.reload();

    const themeToggle = page.locator('.theme-toggle');
    const html = page.locator('html');

    // Switch to dark mode
    await themeToggle.click();
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Verify localStorage was set
    const theme = await page.evaluate(() => {
      return localStorage.getItem('theme');
    });
    expect(theme).toBe('dark');

    // Reload the page (without clearing localStorage)
    await page.reload();

    // Verify dark mode is still applied after reload
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Verify icon is still sun (dark mode indicator)
    const themeIcon = page.locator('.theme-icon');
    await expect(themeIcon).toHaveText('☀️');

    // Switch to light mode
    await page.locator('.theme-toggle').click();
    await expect(html).toHaveAttribute('data-theme', 'light');

    // Verify localStorage was updated
    const newTheme = await page.evaluate(() => {
      return localStorage.getItem('theme');
    });
    expect(newTheme).toBe('light');

    // Reload again
    await page.reload();

    // Verify light mode persists after reload
    await expect(html).toHaveAttribute('data-theme', 'light');
    await expect(page.locator('.theme-icon')).toHaveText('🌙');
  });

  test('Theme toggle has proper touch target size', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    const themeToggle = page.locator('.theme-toggle');
    const boundingBox = await themeToggle.boundingBox();

    // Verify touch target is at least 44x44 pixels (accessibility standard)
    expect(boundingBox?.width).toBeGreaterThanOrEqual(44);
    expect(boundingBox?.height).toBeGreaterThanOrEqual(44);
  });

  test('Header elements adapt to dark mode', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    const themeToggle = page.locator('.theme-toggle');

    // Switch to dark mode
    await themeToggle.click();
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

    // Check header background adapts
    const header = page.locator('.header');
    const headerBgColor = await header.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Header should have dark background in dark mode
    expect(headerBgColor).toBe('rgb(15, 15, 35)');

    // Check logo color (should be primary color which adapts)
    const logo = page.locator('.logo');
    const logoColor = await logo.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Logo should be visible (primary color in dark mode: rgb(96, 165, 250) = #60a5fa)
    expect(logoColor).toBe('rgb(96, 165, 250)');
  });

  test('Feature cards adapt to dark mode', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    const themeToggle = page.locator('.theme-toggle');

    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Switch to dark mode
    await themeToggle.click();
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

    // Check feature card background
    const featureCard = page.locator('.feature-card').first();
    const cardBgColor = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Feature card background in dark mode should be --color-bg (rgb(15, 15, 35))
    expect(cardBgColor).toBe('rgb(15, 15, 35)');
  });

  test('Footer adapts to dark mode', async ({ page }) => {
    // Clear localStorage and go to page
    await page.addInitScript(() => localStorage.clear());
    await page.goto('/');

    const themeToggle = page.locator('.theme-toggle');

    // Switch to dark mode
    await themeToggle.click();
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

    // Navigate to footer
    await page.locator('.footer').scrollIntoViewIfNeeded();

    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Check footer background adapts to dark mode
    const footerBgColor = await footer.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Footer should have secondary dark background (rgb(26, 26, 46) = #1a1a2e)
    expect(footerBgColor).toBe('rgb(26, 26, 46)');
  });
});
