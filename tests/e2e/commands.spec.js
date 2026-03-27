/**
 * Commands Section E2E Tests
 * Owner: Scenario 7 - Supported Commands List
 *
 * Tests:
 * - Commands section exists and is visible
 * - GET, SET, DELETE commands are displayed
 * - Command cards have proper structure
 */
import { test, expect } from '@playwright/test';

test.describe('Supported Commands List', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('index.html');
  });

  test('TC1: Check for supported commands section', async ({ page }) => {
    // Section exists listing memcached commands supported by MirDB
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify section heading
    const heading = commandsSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Supported Commands');

    // Verify description mentions memcached protocol
    const description = commandsSection.locator('p').first();
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('memcached');
  });

  test('TC2: Verify GET command is listed', async ({ page }) => {
    // GET command appears in supported commands list
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find GET command card
    const getCommandCard = commandsSection.locator('[data-command="get"]');
    await expect(getCommandCard).toBeVisible();

    // Verify GET command code badge
    const getCommandCode = getCommandCard.locator('.commands-code');
    await expect(getCommandCode).toBeVisible();
    await expect(getCommandCode).toHaveText('GET');

    // Verify description exists
    const description = getCommandCard.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('retrieve');
  });

  test('TC3: Verify SET command is listed', async ({ page }) => {
    // SET command appears in supported commands list
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find SET command card
    const setCommandCard = commandsSection.locator('[data-command="set"]');
    await expect(setCommandCard).toBeVisible();

    // Verify SET command code badge
    const setCommandCode = setCommandCard.locator('.commands-code');
    await expect(setCommandCode).toBeVisible();
    await expect(setCommandCode).toHaveText('SET');

    // Verify description exists
    const description = setCommandCard.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('store');
  });

  test('TC4: Verify DELETE command is listed', async ({ page }) => {
    // DELETE command appears in supported commands list
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Find DELETE command card
    const deleteCommandCard = commandsSection.locator('[data-command="delete"]');
    await expect(deleteCommandCard).toBeVisible();

    // Verify DELETE command code badge
    const deleteCommandCode = deleteCommandCard.locator('.commands-code');
    await expect(deleteCommandCode).toBeVisible();
    await expect(deleteCommandCode).toHaveText('DELETE');

    // Verify description exists
    const description = deleteCommandCard.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('remove');
  });

  test('All memcached commands have syntax examples', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Each command card should have a syntax example in pre/code
    const commandCards = commandsSection.locator('.commands-card');
    const count = await commandCards.count();
    expect(count).toBeGreaterThan(0);

    // Verify at least GET, SET, DELETE have syntax examples
    for (const command of ['get', 'set', 'delete']) {
      const card = commandsSection.locator(`[data-command="${command}"]`);
      const syntaxBlock = card.locator('pre code');
      await expect(syntaxBlock).toBeVisible();
    }
  });

  test('Commands section is accessible', async ({ page }) => {
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Section should have aria-labelledby
    await expect(commandsSection).toHaveAttribute('aria-labelledby', 'commands-heading');

    // Heading should have proper id
    const heading = commandsSection.locator('#commands-heading');
    await expect(heading).toBeVisible();
  });
});
