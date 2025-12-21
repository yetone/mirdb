const { test, expect } = require('@playwright/test');

test.describe('Technical Highlights Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section listing supported memcached commands is present', async ({ page }) => {
    // Navigate to technical highlights section
    const technicalSection = page.locator('[data-testid="technical-highlights"]');
    await expect(technicalSection).toBeVisible();

    // Verify commands section exists
    const commandsSection = page.locator('[data-testid="supported-commands"]');
    await expect(commandsSection).toBeVisible();

    // Verify section heading
    const heading = commandsSection.locator('h2, h3').first();
    await expect(heading).toContainText(/command/i);
  });

  test('TC2: Storage commands SET, ADD, REPLACE are listed', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="supported-commands"]');
    await expect(commandsSection).toBeVisible();

    // Check for storage commands
    const storageCommands = commandsSection.locator('[data-testid="storage-commands"]');
    await expect(storageCommands).toBeVisible();

    // Verify SET command is listed
    await expect(commandsSection).toContainText('SET');

    // Verify ADD command is listed
    await expect(commandsSection).toContainText('ADD');

    // Verify REPLACE command is listed
    await expect(commandsSection).toContainText('REPLACE');
  });

  test('TC3: Retrieval commands GET, GETS are listed', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="supported-commands"]');
    await expect(commandsSection).toBeVisible();

    // Check for retrieval commands
    const retrievalCommands = commandsSection.locator('[data-testid="retrieval-commands"]');
    await expect(retrievalCommands).toBeVisible();

    // Verify GET command is listed
    await expect(commandsSection).toContainText('GET');

    // Verify GETS command is listed
    await expect(commandsSection).toContainText('GETS');
  });

  test('TC4: MirDB-specific commands INFO and MAJOR_COMPACTION are documented', async ({ page }) => {
    const commandsSection = page.locator('[data-testid="supported-commands"]');
    await expect(commandsSection).toBeVisible();

    // Check for MirDB extensions section
    const mirdbExtensions = commandsSection.locator('[data-testid="mirdb-extensions"]');
    await expect(mirdbExtensions).toBeVisible();

    // Verify INFO command is listed
    await expect(commandsSection).toContainText('INFO');

    // Verify MAJOR_COMPACTION command is listed
    await expect(commandsSection).toContainText('MAJOR_COMPACTION');
  });

  test('TC5: Visual or textual representation of LSM-tree architecture is present', async ({ page }) => {
    const technicalSection = page.locator('[data-testid="technical-highlights"]');
    await expect(technicalSection).toBeVisible();

    // Check for architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify LSM-tree terminology is present
    await expect(architectureSection).toContainText(/LSM|memtable|SSTable/i);

    // Verify architecture diagram or description exists
    const architectureDiagram = architectureSection.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toBeVisible();

    // Verify key components are mentioned
    await expect(architectureSection).toContainText(/memtable/i);
    await expect(architectureSection).toContainText(/SSTable/i);
    await expect(architectureSection).toContainText(/level/i);
  });

  test('TC6: Configuration reference shows listen address (0.0.0.0:12333), memtable size (4MB), etc.', async ({ page }) => {
    const technicalSection = page.locator('[data-testid="technical-highlights"]');
    await expect(technicalSection).toBeVisible();

    // Check for configuration section
    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    // Verify listen address default value
    await expect(configSection).toContainText('0.0.0.0:12333');

    // Verify memtable size default value
    await expect(configSection).toContainText('4MB');

    // Verify other configuration values are present
    await expect(configSection).toContainText(/max.*level|7/i);
    await expect(configSection).toContainText(/SSTable.*100MB|100MB/i);
    await expect(configSection).toContainText(/block.*4KB|4KB/i);
  });
});
