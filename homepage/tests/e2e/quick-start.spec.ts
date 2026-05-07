import { test, expect } from '@playwright/test';

test.describe('Quick-Start Guide Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/#quick-start');
  });

  test('displays quick-start section with heading', async ({ page }) => {
    const section = page.locator('[data-testid="quick-start-section"]');
    await expect(section).toBeVisible();

    const heading = section.locator('h2');
    await expect(heading).toHaveText('Getting Started');
  });

  test('contains step-by-step clone, build, run, and connect instructions', async ({ page }) => {
    const section = page.locator('[data-testid="quick-start-section"]');

    await expect(section.getByText('Clone the repository')).toBeVisible();
    await expect(section.getByText('Build the project')).toBeVisible();
    await expect(section.getByText('Run the server')).toBeVisible();
    await expect(section.getByText('Connect and test')).toBeVisible();
  });

  test('clone step contains git clone and cd commands', async ({ page }) => {
    const step0 = page.locator('[data-testid="quick-start-step-0"]');
    const codeBlock = step0.locator('[data-testid="code-block"]');

    await expect(codeBlock).toContainText('git clone');
    await expect(codeBlock).toContainText('cd mirdb');
  });

  test('build step contains cargo build --release command', async ({ page }) => {
    const step1 = page.locator('[data-testid="quick-start-step-1"]');
    const codeBlock = step1.locator('[data-testid="code-block"]');

    await expect(codeBlock).toContainText('cargo build --release');
  });

  test('run step contains cargo run command', async ({ page }) => {
    const step2 = page.locator('[data-testid="quick-start-step-2"]');
    const codeBlock = step2.locator('[data-testid="code-block"]');

    await expect(codeBlock).toContainText('cargo run --release --bin mirdb');
  });

  test('connect step contains telnet and memcached SET/GET commands', async ({ page }) => {
    const step3 = page.locator('[data-testid="quick-start-step-3"]');
    const codeBlock = step3.locator('[data-testid="code-block"]');

    await expect(codeBlock).toContainText('telnet');
    await expect(codeBlock).toContainText('set mykey');
    await expect(codeBlock).toContainText('get mykey');
    await expect(codeBlock).toContainText('STORED');
    await expect(codeBlock).toContainText('END');
  });

  test('code blocks have bash language identifier', async ({ page }) => {
    const codeBlocks = page.locator('[data-testid="code-block"]');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < count; i++) {
      const lang = await codeBlocks.nth(i).getAttribute('data-language');
      expect(lang).toBe('bash');
    }
  });

  test('copy button copies code block content to clipboard', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const firstCodeBlock = page.locator('[data-testid="code-block"]').first();
    const copyButton = firstCodeBlock.locator('[data-testid="copy-button"]');

    await expect(copyButton).toBeVisible();

    // Click copy button
    await copyButton.click();

    // Verify button shows "Copied!" state
    await expect(copyButton).toContainText('Copied!');

    // Verify clipboard content matches the code
    const clipboardText = await page.evaluate(async () => {
      return await navigator.clipboard.readText();
    });

    await expect(clipboardText).toContain('git clone');
  });

  test('default configuration shows correct listen address and work directory', async ({ page }) => {
    const section = page.locator('[data-testid="quick-start-section"]');

    await expect(section).toContainText('0.0.0.0:12333');
    await expect(section).toContainText('/tmp/mirdb');
    await expect(section).toContainText('etc/mirdb.toml');
  });

  test('section has correct id for anchor navigation', async ({ page }) => {
    const section = page.locator('section#quick-start');
    await expect(section).toBeVisible();
  });
});
