import { test, expect } from '@playwright/test';

test.describe('Supported Memcached Commands Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for supported commands list', async ({ page }) => {
    // Navigate to the code-examples section which contains the commands
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Find the Supported Commands section
    const commandsHeading = page.locator('h3:has-text("Supported Commands")');
    await expect(commandsHeading).toBeVisible();

    // Verify the commands list container exists
    const commandsList = page.locator('.commands-list');
    await expect(commandsList).toBeVisible();

    // Verify multiple commands are listed
    const commands = page.locator('.commands-list .command');
    const commandCount = await commands.count();
    expect(commandCount).toBeGreaterThanOrEqual(3);
  });

  test('TC2: Verify GET command listed', async ({ page }) => {
    // Navigate to code-examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Find the commands list
    const commandsList = page.locator('.commands-list');
    await expect(commandsList).toBeVisible();

    // Verify GET command is present
    const getCommand = page.locator('.commands-list .command:has-text("GET")');
    await expect(getCommand).toBeVisible();
    await expect(getCommand).toHaveText('GET');
  });

  test('TC3: Verify SET command listed', async ({ page }) => {
    // Navigate to code-examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Find the commands list
    const commandsList = page.locator('.commands-list');
    await expect(commandsList).toBeVisible();

    // Verify SET command is present
    const setCommand = page.locator('.commands-list .command:has-text("SET")');
    await expect(setCommand).toBeVisible();
    await expect(setCommand).toHaveText('SET');
  });

  test('TC4: Verify DELETE command listed', async ({ page }) => {
    // Navigate to code-examples section
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Find the commands list
    const commandsList = page.locator('.commands-list');
    await expect(commandsList).toBeVisible();

    // Verify DELETE command is present
    const deleteCommand = page.locator('.commands-list .command:has-text("DELETE")');
    await expect(deleteCommand).toBeVisible();
    await expect(deleteCommand).toHaveText('DELETE');
  });
});
