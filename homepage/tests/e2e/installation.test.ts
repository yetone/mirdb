/**
 * E2E tests for Installation section.
 * Owner: Scenario 5 - Installation Section
 *
 * Tests:
 * - Installation section structure
 * - Tab switching for different platforms (Cargo, Docker, Source)
 * - Copy button functionality with visual feedback
 * - Quick start guide display
 */

import { test, expect } from '@playwright/test';

test.describe('Installation Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/mirdb/');
    // Wait for the installation section to be visible
    await page.waitForSelector('#installation');
  });

  test.describe('Section Structure', () => {
    test('should display the Installation heading', async ({ page }) => {
      const heading = page.locator('#installation-heading');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('Installation');
    });

    test('should have navigation to installation section', async ({ page }) => {
      const section = page.locator('#installation');
      await expect(section).toBeVisible();
    });

    test('should display installation commands for Cargo, Docker, and source build', async ({ page }) => {
      // Check that all three platform tabs exist
      const cargoTab = page.locator('[data-install-tab="cargo"]');
      const dockerTab = page.locator('[data-install-tab="docker"]');
      const sourceTab = page.locator('[data-install-tab="source"]');

      await expect(cargoTab).toBeVisible();
      await expect(dockerTab).toBeVisible();
      await expect(sourceTab).toBeVisible();
    });
  });

  test.describe('Tab Interface', () => {
    test('should display three platform tabs: Cargo, Docker, From Source', async ({ page }) => {
      const cargoTab = page.locator('[data-install-tab="cargo"]');
      const dockerTab = page.locator('[data-install-tab="docker"]');
      const sourceTab = page.locator('[data-install-tab="source"]');

      await expect(cargoTab).toContainText('Cargo');
      await expect(dockerTab).toContainText('Docker');
      await expect(sourceTab).toContainText('From Source');
    });

    test('should have Cargo tab selected by default', async ({ page }) => {
      const cargoTab = page.locator('[data-install-tab="cargo"]');
      await expect(cargoTab).toHaveAttribute('aria-selected', 'true');

      const cargoPanel = page.locator('[data-install-panel="cargo"]');
      await expect(cargoPanel).toBeVisible();
    });

    test('should switch to Docker tab on click', async ({ page }) => {
      const dockerTab = page.locator('[data-install-tab="docker"]');
      await dockerTab.click();

      await expect(dockerTab).toHaveAttribute('aria-selected', 'true');

      const dockerPanel = page.locator('[data-install-panel="docker"]');
      await expect(dockerPanel).toBeVisible();

      const cargoPanel = page.locator('[data-install-panel="cargo"]');
      await expect(cargoPanel).not.toBeVisible();
    });

    test('should switch to Source tab on click', async ({ page }) => {
      const sourceTab = page.locator('[data-install-tab="source"]');
      await sourceTab.click();

      await expect(sourceTab).toHaveAttribute('aria-selected', 'true');

      const sourcePanel = page.locator('[data-install-panel="source"]');
      await expect(sourcePanel).toBeVisible();
    });
  });

  test.describe('Cargo Install Command', () => {
    test('should show cargo install mirdb command', async ({ page }) => {
      const cargoPanel = page.locator('[data-install-panel="cargo"]');
      await expect(cargoPanel).toBeVisible();

      const codeBlock = page.locator('#install-code-cargo');
      const content = await codeBlock.textContent();
      expect(content).toContain('cargo install mirdb');
    });

    test('should have copy functionality for Cargo command', async ({ page }) => {
      const copyButton = cargoCodeBlockCopyButton(page);
      await expect(copyButton).toBeVisible();
    });
  });

  test.describe('Docker Command', () => {
    test('should show docker run command with port mapping', async ({ page }) => {
      const dockerTab = page.locator('[data-install-tab="docker"]');
      await dockerTab.click();

      const codeBlock = page.locator('#install-code-docker');
      const content = await codeBlock.textContent();
      expect(content).toContain('docker run -p 9000:9000 yetone/mirdb');
    });

    test('should have copy functionality for Docker command', async ({ page }) => {
      const dockerTab = page.locator('[data-install-tab="docker"]');
      await dockerTab.click();

      const copyButton = page.locator('[data-install-panel="docker"] .copy-button').first();
      await expect(copyButton).toBeVisible();
    });
  });

  test.describe('Source Build Commands', () => {
    test('should show git clone and cargo build steps', async ({ page }) => {
      const sourceTab = page.locator('[data-install-tab="source"]');
      await sourceTab.click();

      const sourcePanel = page.locator('[data-install-panel="source"]');
      const content = await sourcePanel.textContent();

      expect(content).toContain('git clone');
      expect(content).toContain('https://github.com/yetone/mirdb');
      expect(content).toContain('cargo build --release');
    });

    test('should display numbered steps for source build', async ({ page }) => {
      const sourceTab = page.locator('[data-install-tab="source"]');
      await sourceTab.click();

      const steps = page.locator('[data-install-panel="source"] .install-step');
      await expect(steps).toHaveCount(4); // git clone, cd, cargo build, run
    });
  });

  test.describe('Quick Start Guide', () => {
    test('should display Quick Start section', async ({ page }) => {
      const quickStartHeading = page.getByRole('heading', { name: 'Quick Start' });
      await expect(quickStartHeading).toBeVisible();
    });

    test('should show telnet connection command', async ({ page }) => {
      const quickStartCode = page.locator('#install-code-quickstart');
      const content = await quickStartCode.textContent();
      expect(content).toContain('telnet localhost 9000');
    });
  });

  test.describe('Copy Button Functionality', () => {
    test('should have copy button on install command code blocks', async ({ page }) => {
      const copyButton = cargoCodeBlockCopyButton(page);
      await expect(copyButton).toBeVisible();
    });

    test('should have accessible copy button label', async ({ page }) => {
      const copyButton = cargoCodeBlockCopyButton(page);
      await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');
    });

    test('should show check icon after clicking copy with visual feedback', async ({ page }) => {
      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = cargoCodeBlockCopyButton(page);
      const copyIcon = copyButton.locator('.copy-icon');
      const checkIcon = copyButton.locator('.check-icon');

      // Initially copy icon visible, check icon hidden
      await expect(copyIcon).toBeVisible();
      await expect(checkIcon).not.toBeVisible();

      // Click copy button
      await copyButton.click();

      // Check icon should become visible (visual feedback)
      await expect(checkIcon).toBeVisible();
      await expect(copyIcon).not.toBeVisible();

      // After 2 seconds, should reset back
      await page.waitForTimeout(2500);
      await expect(copyIcon).toBeVisible();
      await expect(checkIcon).not.toBeVisible();
    });

    test('should copy install command to clipboard', async ({ page }) => {
      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = cargoCodeBlockCopyButton(page);
      await copyButton.click();

      // Read clipboard and verify content was copied
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toBeTruthy();
      expect(clipboardContent).toContain('cargo install mirdb');
    });

    test('should update aria-label after copy', async ({ page }) => {
      await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);

      const copyButton = cargoCodeBlockCopyButton(page);
      await copyButton.click();

      // Aria-label should update to "Copied!"
      await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');
    });
  });

  test.describe('Keyboard Navigation', () => {
    test('should navigate tabs with arrow keys', async ({ page }) => {
      const cargoTab = page.locator('[data-install-tab="cargo"]');
      await cargoTab.focus();

      // Press ArrowRight to move to Docker
      await page.keyboard.press('ArrowRight');

      const dockerTab = page.locator('[data-install-tab="docker"]');
      await expect(dockerTab).toBeFocused();
      await expect(dockerTab).toHaveAttribute('aria-selected', 'true');
    });

    test('should wrap around from last to first tab', async ({ page }) => {
      const sourceTab = page.locator('[data-install-tab="source"]');
      await sourceTab.click();
      await sourceTab.focus();

      // Press ArrowRight to wrap to Cargo
      await page.keyboard.press('ArrowRight');

      const cargoTab = page.locator('[data-install-tab="cargo"]');
      await expect(cargoTab).toBeFocused();
    });

    test('should navigate with Home and End keys', async ({ page }) => {
      const dockerTab = page.locator('[data-install-tab="docker"]');
      await dockerTab.click();
      await dockerTab.focus();

      // Press Home to go to first tab
      await page.keyboard.press('Home');

      const cargoTab = page.locator('[data-install-tab="cargo"]');
      await expect(cargoTab).toBeFocused();

      // Press End to go to last tab
      await page.keyboard.press('End');

      const sourceTab = page.locator('[data-install-tab="source"]');
      await expect(sourceTab).toBeFocused();
    });
  });

  test.describe('Accessibility', () => {
    test('should have proper ARIA roles', async ({ page }) => {
      const tablist = page.locator('#installation [role="tablist"]');
      await expect(tablist).toBeVisible();

      const tabs = page.locator('#installation [role="tab"]');
      await expect(tabs).toHaveCount(3);

      const tabpanels = page.locator('#installation [role="tabpanel"]');
      await expect(tabpanels).toHaveCount(3);
    });

    test('should have aria-controls linking tabs to panels', async ({ page }) => {
      const cargoTab = page.locator('[data-install-tab="cargo"]');
      const controlsId = await cargoTab.getAttribute('aria-controls');
      expect(controlsId).toBe('install-panel-cargo');

      const panel = page.locator(`#${controlsId}`);
      await expect(panel).toBeVisible();
    });

    test('code blocks should be focusable for screen readers', async ({ page }) => {
      const codeBlock = page.locator('#installation .terminal-body pre').first();
      await expect(codeBlock).toHaveAttribute('tabindex', '0');
    });

    test('should have section heading with aria-labelledby', async ({ page }) => {
      const section = page.locator('#installation');
      await expect(section).toHaveAttribute('aria-labelledby', 'installation-heading');
    });
  });

  test.describe('Documentation Link', () => {
    test('should have link to full documentation', async ({ page }) => {
      const docLink = page.getByRole('link', { name: /View Full Documentation/i });
      await expect(docLink).toBeVisible();
      await expect(docLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');
      await expect(docLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });
});

// Helper function to get the copy button for the Cargo code block
function cargoCodeBlockCopyButton(page: any) {
  return page.locator('[data-install-panel="cargo"] .copy-button').first();
}
