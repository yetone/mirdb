// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Usage/Quick-Start Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: A dedicated usage/quick-start section is visible', async ({ page }) => {
    // Check that a dedicated usage section exists
    const usageSection = page.locator('#usage, section.usage, [data-testid="usage"], #quick-start, section.quick-start, [data-testid="quick-start"]').first();
    await expect(usageSection).toBeVisible();

    // Verify section has a heading
    const heading = usageSection.locator('h2, h3').first();
    await expect(heading).toBeVisible();

    // Verify heading contains usage-related text
    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toMatch(/usage|quick.*start|getting.*started|how.*to.*use/i);
  });

  test('TC2: Code snippet or terminal example (usage.gif) is present', async ({ page }) => {
    // Check for usage section
    const usageSection = page.locator('#usage, section.usage, [data-testid="usage"], #quick-start, section.quick-start, [data-testid="quick-start"]').first();
    await expect(usageSection).toBeVisible();

    // Check for code block OR terminal animation (usage.gif)
    const codeBlock = usageSection.locator('pre, code, .code-block, [data-testid="code-block"]');
    const terminalAnimation = usageSection.locator('img[src*="usage"], img[alt*="usage" i], img[alt*="terminal" i], img[alt*="demo" i], .terminal-demo, [data-testid="usage-demo"]');

    // Either code block or terminal animation should be present
    const codeBlockCount = await codeBlock.count();
    const terminalAnimationCount = await terminalAnimation.count();

    expect(codeBlockCount + terminalAnimationCount).toBeGreaterThanOrEqual(1);
  });

  test('TC3: Text emphasizing simplicity or memcached compatibility is visible', async ({ page }) => {
    // Check for usage section
    const usageSection = page.locator('#usage, section.usage, [data-testid="usage"], #quick-start, section.quick-start, [data-testid="quick-start"]').first();
    await expect(usageSection).toBeVisible();

    // Check for ease-of-use messaging that emphasizes simplicity or memcached compatibility
    // PRD mentions: "painless as using memcached" messaging
    const sectionText = await usageSection.textContent();
    const hasSimplicityMessaging =
      /simple|easy|painless|straightforward|familiar|drop.?in|just.*like.*memcached|memcached.*compatible|memcached.*client/i.test(sectionText);

    expect(hasSimplicityMessaging).toBe(true);
  });
});
