const { test, expect } = require('@playwright/test');

test.describe('Supported Commands Overview', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: SET command is listed with description in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Query for SET command
    const setCommand = commandsSection.locator('[data-testid="command-set"]');
    await expect(setCommand).toBeVisible();

    // Verify SET command name is displayed
    const commandName = setCommand.locator('.command-name');
    await expect(commandName).toContainText('SET');

    // Verify SET command has a description
    const commandDesc = setCommand.locator('.command-description');
    await expect(commandDesc).toBeVisible();
    const descText = await commandDesc.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  test('TC2: GET command is listed with description in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Query for GET command
    const getCommand = commandsSection.locator('[data-testid="command-get"]');
    await expect(getCommand).toBeVisible();

    // Verify GET command name is displayed
    const commandName = getCommand.locator('.command-name');
    await expect(commandName).toContainText('GET');

    // Verify GET command has a description
    const commandDesc = getCommand.locator('.command-description');
    await expect(commandDesc).toBeVisible();
    const descText = await commandDesc.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  test('TC3: DELETE command is listed with description in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Query for DELETE command
    const deleteCommand = commandsSection.locator('[data-testid="command-delete"]');
    await expect(deleteCommand).toBeVisible();

    // Verify DELETE command name is displayed
    const commandName = deleteCommand.locator('.command-name');
    await expect(commandName).toContainText('DELETE');

    // Verify DELETE command has a description
    const commandDesc = deleteCommand.locator('.command-description');
    await expect(commandDesc).toBeVisible();
    const descText = await commandDesc.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  test('TC4: ADD command is listed with description in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Query for ADD command
    const addCommand = commandsSection.locator('[data-testid="command-add"]');
    await expect(addCommand).toBeVisible();

    // Verify ADD command name is displayed
    const commandName = addCommand.locator('.command-name');
    await expect(commandName).toContainText('ADD');

    // Verify ADD command has a description
    const commandDesc = addCommand.locator('.command-description');
    await expect(commandDesc).toBeVisible();
    const descText = await commandDesc.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  test('TC5: REPLACE command is listed with description in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Query for REPLACE command
    const replaceCommand = commandsSection.locator('[data-testid="command-replace"]');
    await expect(replaceCommand).toBeVisible();

    // Verify REPLACE command name is displayed
    const commandName = replaceCommand.locator('.command-name');
    await expect(commandName).toContainText('REPLACE');

    // Verify REPLACE command has a description
    const commandDesc = replaceCommand.locator('.command-description');
    await expect(commandDesc).toBeVisible();
    const descText = await commandDesc.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  test('TC6: APPEND and PREPEND commands are listed in commands section', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Query for APPEND command
    const appendCommand = commandsSection.locator('[data-testid="command-append"]');
    await expect(appendCommand).toBeVisible();

    // Verify APPEND command name is displayed
    const appendName = appendCommand.locator('.command-name');
    await expect(appendName).toContainText('APPEND');

    // Query for PREPEND command
    const prependCommand = commandsSection.locator('[data-testid="command-prepend"]');
    await expect(prependCommand).toBeVisible();

    // Verify PREPEND command name is displayed
    const prependName = prependCommand.locator('.command-name');
    await expect(prependName).toContainText('PREPEND');
  });

  test('TC7: Link to full command documentation is present and functional', async ({ page }) => {
    // Navigate to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();
    await expect(commandsSection).toBeVisible();

    // Query for documentation link
    const docsLink = commandsSection.locator('[data-testid="commands-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify link has href attribute pointing to documentation
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify link contains text indicating it's a documentation link
    const linkText = await docsLink.textContent();
    expect(linkText.toLowerCase()).toMatch(/doc|memcached|protocol|full|reference/);
  });
});
