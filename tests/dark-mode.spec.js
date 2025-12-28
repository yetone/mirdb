const { test, expect } = require('@playwright/test');

test.describe('Dark Mode Support', () => {
  test('should render with dark color scheme when system prefers dark mode', async ({ page }) => {
    // Emulate dark mode system preference
    await page.emulateMedia({ colorScheme: 'dark' });

    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Get computed background color of body
    const bodyBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    // In dark mode, background should be dark (not white/light)
    // Dark colors have low RGB values (typically < 50 for each component)
    // Expecting something like rgb(15, 23, 42) for slate-900 or similar dark color
    const rgbMatch = bodyBgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    const [, r, g, b] = rgbMatch.map(Number);
    // Check that the background is dark (average RGB < 80)
    const avgColor = (r + g + b) / 3;
    expect(avgColor).toBeLessThan(80);
  });

  test('should render with light color scheme when system prefers light mode', async ({ page }) => {
    // Emulate light mode system preference
    await page.emulateMedia({ colorScheme: 'light' });

    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Get computed background color of body
    const bodyBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    // In light mode, background should be light/white
    // Light colors have high RGB values (typically > 200 for each component)
    const rgbMatch = bodyBgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(rgbMatch).toBeTruthy();

    const [, r, g, b] = rgbMatch.map(Number);
    // Check that the background is light (average RGB > 200)
    const avgColor = (r + g + b) / 3;
    expect(avgColor).toBeGreaterThan(200);
  });

  test('should have appropriate dark mode styling for code blocks', async ({ page }) => {
    // Emulate dark mode system preference
    await page.emulateMedia({ colorScheme: 'dark' });

    await page.goto('/');

    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Find a code block
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Get computed styles for the code block
    const codeBlockBgColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Get computed styles for the code element inside
    const codeElement = codeBlock.locator('code').first();
    const codeTextColor = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Verify code block has dark background
    const bgRgbMatch = codeBlockBgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(bgRgbMatch).toBeTruthy();
    const [, bgR, bgG, bgB] = bgRgbMatch.map(Number);
    const bgAvgColor = (bgR + bgG + bgB) / 3;
    // Code blocks should have dark background (< 60)
    expect(bgAvgColor).toBeLessThan(60);

    // Verify code text has light/readable color
    const textRgbMatch = codeTextColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    expect(textRgbMatch).toBeTruthy();
    const [, textR, textG, textB] = textRgbMatch.map(Number);
    const textAvgColor = (textR + textG + textB) / 3;
    // Code text should be light for readability (> 150)
    expect(textAvgColor).toBeGreaterThan(150);

    // Calculate contrast ratio for readability
    // This is a simplified contrast check - darker bg with lighter text
    const contrastDiff = textAvgColor - bgAvgColor;
    expect(contrastDiff).toBeGreaterThan(100);
  });
});
