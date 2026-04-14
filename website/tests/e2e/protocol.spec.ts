/**
 * Protocol Reference Section E2E Tests
 * Owner: Scenario 5 - Protocol Reference Section
 *
 * Tests for:
 * - Protocol reference section visibility
 * - Standard memcached commands documentation
 * - MirDB-specific commands with visual differentiation
 * - Syntax examples for commands
 */
import { test, expect } from '@playwright/test';
import { SELECTORS } from '../fixtures/test-data';

test.describe('Protocol Reference Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Command table is visible', async ({ page }) => {
    // Navigate to protocol section
    const protocolSection = page.locator(SELECTORS.protocol);
    await protocolSection.scrollIntoViewIfNeeded();

    // Verify protocol section is visible
    await expect(protocolSection).toBeVisible();

    // Verify command table is visible
    const commandTable = page.locator(SELECTORS.commandTable);
    await expect(commandTable).toBeVisible();

    // Verify MirDB-specific command table is visible
    const mirdbTable = page.locator(SELECTORS.mirdbCommandTable);
    await expect(mirdbTable).toBeVisible();
  });

  test('TC2: SET command is documented with correct syntax', async ({ page }) => {
    const protocolSection = page.locator(SELECTORS.protocol);
    await protocolSection.scrollIntoViewIfNeeded();

    // Find the SET command row
    const setRow = page.locator('tr[data-command="SET"]');
    await expect(setRow).toBeVisible();

    // Verify SET command text
    await expect(setRow.locator('code').first()).toContainText('SET');

    // Verify syntax contains the expected format
    await expect(setRow).toContainText('SET <key> <flags> <exptime> <bytes>');
  });

  test('TC3: GET command is documented with correct syntax', async ({ page }) => {
    const protocolSection = page.locator(SELECTORS.protocol);
    await protocolSection.scrollIntoViewIfNeeded();

    // Find the GET command row
    const getRow = page.locator('tr[data-command="GET"]');
    await expect(getRow).toBeVisible();

    // Verify GET command text
    await expect(getRow.locator('code').first()).toContainText('GET');

    // Verify syntax contains the expected format
    await expect(getRow).toContainText('GET <key>');
  });

  test('TC4: DELETE command is documented with correct syntax', async ({ page }) => {
    const protocolSection = page.locator(SELECTORS.protocol);
    await protocolSection.scrollIntoViewIfNeeded();

    // Find the DELETE command row
    const deleteRow = page.locator('tr[data-command="DELETE"]');
    await expect(deleteRow).toBeVisible();

    // Verify DELETE command text
    await expect(deleteRow.locator('code').first()).toContainText('DELETE');

    // Verify syntax contains the expected format
    await expect(deleteRow).toContainText('DELETE <key>');
  });

  test('TC5: INFO command is documented and marked as MirDB-specific', async ({ page }) => {
    const protocolSection = page.locator(SELECTORS.protocol);
    await protocolSection.scrollIntoViewIfNeeded();

    // Find the INFO command row in MirDB-specific table
    const infoRow = page.locator('tr[data-command="INFO"]');
    await expect(infoRow).toBeVisible();

    // Verify INFO command text
    await expect(infoRow.locator('code').first()).toContainText('INFO');

    // Verify it has the MirDB badge
    const mirdbBadge = infoRow.locator('.mirdb-badge');
    await expect(mirdbBadge).toBeVisible();
    await expect(mirdbBadge).toContainText('MirDB');

    // Verify the row has mirdb-specific class
    await expect(infoRow).toHaveClass(/mirdb-specific/);
  });

  test('TC6: MAJOR_COMPACTION command is documented and marked as MirDB-specific', async ({ page }) => {
    const protocolSection = page.locator(SELECTORS.protocol);
    await protocolSection.scrollIntoViewIfNeeded();

    // Find the MAJOR_COMPACTION command row in MirDB-specific table
    const compactionRow = page.locator('tr[data-command="MAJOR_COMPACTION"]');
    await expect(compactionRow).toBeVisible();

    // Verify MAJOR_COMPACTION command text
    await expect(compactionRow.locator('code').first()).toContainText('MAJOR_COMPACTION');

    // Verify it has the MirDB badge
    const mirdbBadge = compactionRow.locator('.mirdb-badge');
    await expect(mirdbBadge).toBeVisible();
    await expect(mirdbBadge).toContainText('MirDB');

    // Verify the row has mirdb-specific class
    await expect(compactionRow).toHaveClass(/mirdb-specific/);
  });

  test('TC7: Syntax examples are provided', async ({ page }) => {
    const protocolSection = page.locator(SELECTORS.protocol);
    await protocolSection.scrollIntoViewIfNeeded();

    // Verify syntax examples section exists
    const syntaxExamples = page.locator(SELECTORS.syntaxExamples);
    await expect(syntaxExamples).toBeVisible();

    // Verify at least 3 syntax examples are provided (SET, GET, DELETE)
    const examples = page.locator(SELECTORS.syntaxExample);
    await expect(examples).toHaveCount(3);

    // Verify SET example exists
    const setExample = syntaxExamples.locator('h4:has-text("SET Command Example")');
    await expect(setExample).toBeVisible();

    // Verify GET example exists
    const getExample = syntaxExamples.locator('h4:has-text("GET Command Example")');
    await expect(getExample).toBeVisible();

    // Verify DELETE example exists
    const deleteExample = syntaxExamples.locator('h4:has-text("DELETE Command Example")');
    await expect(deleteExample).toBeVisible();
  });

  test('Protocol section has proper structure and accessibility', async ({ page }) => {
    const protocolSection = page.locator(SELECTORS.protocol);
    await protocolSection.scrollIntoViewIfNeeded();

    // Verify section has a title
    const title = page.locator('.protocol-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Protocol Reference');

    // Verify tables have proper headers
    const commandTable = page.locator(SELECTORS.commandTable);
    const headers = commandTable.locator('th');
    await expect(headers).toHaveCount(3);
    await expect(headers.nth(0)).toContainText('Command');
    await expect(headers.nth(1)).toContainText('Syntax');
    await expect(headers.nth(2)).toContainText('Description');
  });
});
