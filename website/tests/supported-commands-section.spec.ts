import { test, expect } from '@playwright/test';

test.describe('Supported Commands Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Supported Commands section exists', async ({ page }) => {
    // Query for supported commands section
    // Expected: Supported Commands or Commands section exists
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const sectionTitle = commandsSection.locator('.step-title');
    await expect(sectionTitle).toContainText('Supported Commands');
  });

  test('TC2: get command is documented with description about retrieving values', async ({ page }) => {
    // Check for 'get' command documentation
    // Expected: get command is listed with description about retrieving values
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const getterCategory = commandsSection.locator('.command-category:has(h4:has-text("Getter Commands"))');
    await expect(getterCategory).toBeVisible();

    // Use a more specific locator to match only 'get' command, not 'gets'
    const getCommand = getterCategory.locator('.command-list li').filter({ hasText: /^get\s/ });
    await expect(getCommand).toBeVisible();

    // Verify description mentions retrieving values
    const getCommandText = await getCommand.textContent();
    expect(getCommandText).toContain('get');
    expect(getCommandText?.toLowerCase()).toMatch(/retriev|value/i);
  });

  test('TC3: gets command is documented with description about CAS token', async ({ page }) => {
    // Check for 'gets' command documentation
    // Expected: gets command is listed with description about CAS token
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const getterCategory = commandsSection.locator('.command-category:has(h4:has-text("Getter Commands"))');
    await expect(getterCategory).toBeVisible();

    const getsCommand = getterCategory.locator('.command-list li', { hasText: 'gets' });
    await expect(getsCommand).toBeVisible();

    // Verify description mentions CAS token
    const getsCommandText = await getsCommand.textContent();
    expect(getsCommandText).toContain('gets');
    expect(getsCommandText?.toLowerCase()).toContain('cas');
  });

  test('TC4: set command is documented with description about storing values', async ({ page }) => {
    // Check for 'set' command documentation
    // Expected: set command is listed with description about storing values
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const setterCategory = commandsSection.locator('.command-category:has(h4:has-text("Setter Commands"))');
    await expect(setterCategory).toBeVisible();

    const setCommand = setterCategory.locator('.command-list li', { hasText: /^set\b/ });
    await expect(setCommand).toBeVisible();

    // Verify description mentions storing values
    const setCommandText = await setCommand.textContent();
    expect(setCommandText).toContain('set');
    expect(setCommandText?.toLowerCase()).toMatch(/stor|value/i);
  });

  test('TC5: add command is documented with description about storing only if key does not exist', async ({ page }) => {
    // Check for 'add' command documentation
    // Expected: add command is listed with description about storing only if key doesn't exist
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const setterCategory = commandsSection.locator('.command-category:has(h4:has-text("Setter Commands"))');
    await expect(setterCategory).toBeVisible();

    const addCommand = setterCategory.locator('.command-list li', { hasText: 'add' });
    await expect(addCommand).toBeVisible();

    // Verify description mentions storing only if key doesn't exist
    const addCommandText = await addCommand.textContent();
    expect(addCommandText).toContain('add');
    expect(addCommandText?.toLowerCase()).toMatch(/doesn.*exist|not.*exist/i);
  });

  test('TC6: replace command is documented with description about storing only if key exists', async ({ page }) => {
    // Check for 'replace' command documentation
    // Expected: replace command is listed with description about storing only if key exists
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const setterCategory = commandsSection.locator('.command-category:has(h4:has-text("Setter Commands"))');
    await expect(setterCategory).toBeVisible();

    const replaceCommand = setterCategory.locator('.command-list li', { hasText: 'replace' });
    await expect(replaceCommand).toBeVisible();

    // Verify description mentions storing only if key exists
    const replaceCommandText = await replaceCommand.textContent();
    expect(replaceCommandText).toContain('replace');
    expect(replaceCommandText?.toLowerCase()).toMatch(/if.*key.*exist/i);
  });

  test('TC7: append and prepend commands are documented with descriptions', async ({ page }) => {
    // Check for 'append' and 'prepend' command documentation
    // Expected: append and prepend commands are listed with descriptions
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const setterCategory = commandsSection.locator('.command-category:has(h4:has-text("Setter Commands"))');
    await expect(setterCategory).toBeVisible();

    // Check append command
    const appendCommand = setterCategory.locator('.command-list li', { hasText: 'append' });
    await expect(appendCommand).toBeVisible();
    const appendCommandText = await appendCommand.textContent();
    expect(appendCommandText).toContain('append');
    expect(appendCommandText?.toLowerCase()).toContain('append');

    // Check prepend command
    const prependCommand = setterCategory.locator('.command-list li', { hasText: 'prepend' });
    await expect(prependCommand).toBeVisible();
    const prependCommandText = await prependCommand.textContent();
    expect(prependCommandText).toContain('prepend');
    expect(prependCommandText?.toLowerCase()).toContain('prepend');
  });

  test('TC8: delete command is documented with description about removing keys', async ({ page }) => {
    // Check for 'delete' command documentation
    // Expected: delete command is listed with description about removing keys
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const otherCategory = commandsSection.locator('.command-category:has(h4:has-text("Other Commands"))');
    await expect(otherCategory).toBeVisible();

    const deleteCommand = otherCategory.locator('.command-list li', { hasText: 'delete' });
    await expect(deleteCommand).toBeVisible();

    // Verify description mentions removing keys
    const deleteCommandText = await deleteCommand.textContent();
    expect(deleteCommandText).toContain('delete');
    expect(deleteCommandText?.toLowerCase()).toMatch(/remov|delet/i);
  });

  test('TC9: info and major_compaction commands are documented', async ({ page }) => {
    // Check for 'info' and 'major_compaction' commands
    // Expected: info and major_compaction commands are documented
    const commandsSection = page.locator('.quickstart-step:has(.step-title:has-text("Supported Commands"))');
    await expect(commandsSection).toBeVisible();

    const otherCategory = commandsSection.locator('.command-category:has(h4:has-text("Other Commands"))');
    await expect(otherCategory).toBeVisible();

    // Check info command
    const infoCommand = otherCategory.locator('.command-list li', { hasText: 'info' });
    await expect(infoCommand).toBeVisible();
    const infoCommandText = await infoCommand.textContent();
    expect(infoCommandText).toContain('info');

    // Check major_compaction command
    const majorCompactionCommand = otherCategory.locator('.command-list li', { hasText: 'major_compaction' });
    await expect(majorCompactionCommand).toBeVisible();
    const majorCompactionCommandText = await majorCompactionCommand.textContent();
    expect(majorCompactionCommandText).toContain('major_compaction');
    expect(majorCompactionCommandText?.toLowerCase()).toContain('compaction');
  });
});
