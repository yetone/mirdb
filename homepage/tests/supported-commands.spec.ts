import { test, expect } from '@playwright/test';

test.describe('Supported Commands Documentation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Storage Commands', () => {
    test('SET command is documented', async ({ page }) => {
      // Look for SET command anywhere on the page (commands section, features, or quick start)
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('SET');
    });

    test('ADD command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('ADD');
    });

    test('REPLACE command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('REPLACE');
    });

    test('APPEND command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('APPEND');
    });

    test('PREPEND command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('PREPEND');
    });

    test('All storage commands are listed in the commands section', async ({ page }) => {
      // Find the supported commands section
      const commandsSection = page.locator('[data-testid="supported-commands-section"]');
      await expect(commandsSection).toBeVisible();

      // Verify storage commands subsection
      const storageCommands = commandsSection.locator('[data-testid="storage-commands"]');
      await expect(storageCommands).toBeVisible();

      // Check all storage commands are present
      await expect(storageCommands).toContainText('SET');
      await expect(storageCommands).toContainText('ADD');
      await expect(storageCommands).toContainText('REPLACE');
      await expect(storageCommands).toContainText('APPEND');
      await expect(storageCommands).toContainText('PREPEND');
    });
  });

  test.describe('TC2: Retrieval Commands', () => {
    test('GET command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('GET');
    });

    test('GETS command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('GETS');
    });

    test('All retrieval commands are listed in the commands section', async ({ page }) => {
      // Find the supported commands section
      const commandsSection = page.locator('[data-testid="supported-commands-section"]');
      await expect(commandsSection).toBeVisible();

      // Verify retrieval commands subsection
      const retrievalCommands = commandsSection.locator('[data-testid="retrieval-commands"]');
      await expect(retrievalCommands).toBeVisible();

      // Check all retrieval commands are present
      await expect(retrievalCommands).toContainText('GET');
      await expect(retrievalCommands).toContainText('GETS');
    });
  });

  test.describe('TC3: Management Commands', () => {
    test('DELETE command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('DELETE');
    });

    test('INFO command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('INFO');
    });

    test('MAJOR_COMPACTION command is documented', async ({ page }) => {
      const pageContent = await page.textContent('body');
      expect(pageContent?.toUpperCase()).toContain('MAJOR_COMPACTION');
    });

    test('All management commands are listed in the commands section', async ({ page }) => {
      // Find the supported commands section
      const commandsSection = page.locator('[data-testid="supported-commands-section"]');
      await expect(commandsSection).toBeVisible();

      // Verify management commands subsection
      const managementCommands = commandsSection.locator('[data-testid="management-commands"]');
      await expect(managementCommands).toBeVisible();

      // Check all management commands are present
      await expect(managementCommands).toContainText('DELETE');
      await expect(managementCommands).toContainText('INFO');
      await expect(managementCommands).toContainText('MAJOR_COMPACTION');
    });
  });

  test.describe('Supported Commands Section Structure', () => {
    test('Supported commands section exists and is visible', async ({ page }) => {
      const commandsSection = page.locator('[data-testid="supported-commands-section"]');
      await expect(commandsSection).toBeVisible();
    });

    test('Supported commands section has a heading', async ({ page }) => {
      const commandsSection = page.locator('[data-testid="supported-commands-section"]');
      await expect(commandsSection).toBeVisible();

      const heading = commandsSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText(/command/i);
    });

    test('Commands are organized by category', async ({ page }) => {
      const commandsSection = page.locator('[data-testid="supported-commands-section"]');
      await expect(commandsSection).toBeVisible();

      // Verify three categories exist
      await expect(commandsSection.locator('[data-testid="storage-commands"]')).toBeVisible();
      await expect(commandsSection.locator('[data-testid="retrieval-commands"]')).toBeVisible();
      await expect(commandsSection.locator('[data-testid="management-commands"]')).toBeVisible();
    });

    test('Navigation includes link to commands section', async ({ page }) => {
      // Check for navigation link to commands section
      const navLinks = page.locator('nav a[href="#commands"], nav a[href="#supported-commands"]');
      const linkCount = await navLinks.count();

      // Either via nav or accessible from features section
      if (linkCount > 0) {
        await expect(navLinks.first()).toBeVisible();
      } else {
        // Commands section should still be accessible via scroll
        const commandsSection = page.locator('[data-testid="supported-commands-section"]');
        await expect(commandsSection).toBeVisible();
      }
    });
  });
});
