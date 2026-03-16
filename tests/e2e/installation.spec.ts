/**
 * Installation Instructions Tests
 * Owner: Scenario 6 - Installation Instructions
 *
 * Test cases:
 * - Installation section exists
 * - Cargo install command present
 * - Docker installation option (if applicable)
 * - Installation tabs/toggles work
 */

import { test, expect } from '@playwright/test';

test.describe('Installation Instructions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Installation section heading exists', async ({ page }) => {
    // Navigate to installation section
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeVisible();

    // Check for heading containing 'Install' or 'Installation'
    const heading = installationSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText).toMatch(/install(ation)?/i);
  });

  test('TC2: Cargo install command is present', async ({ page }) => {
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeVisible();

    // Ensure cargo tab is active (it should be by default)
    const cargoTab = page.locator('.tab-button[data-tab="cargo"]');
    await expect(cargoTab).toHaveClass(/active/);

    // Check for cargo install or cargo build commands in code blocks
    const cargoPanel = page.locator('#cargo-panel');
    await expect(cargoPanel).toBeVisible();

    // Check for cargo install command
    const cargoInstallCode = cargoPanel.locator('code:has-text("cargo install")');
    await expect(cargoInstallCode).toBeVisible();

    // Also verify cargo build command exists
    const cargoBuildCode = cargoPanel.locator('code:has-text("cargo build")');
    await expect(cargoBuildCode).toBeVisible();
  });

  test('TC3: Docker installation option is present', async ({ page }) => {
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeVisible();

    // Click on Docker tab
    const dockerTab = page.locator('.tab-button[data-tab="docker"]');
    await expect(dockerTab).toBeVisible();
    await dockerTab.click();

    // Check Docker tab is now active
    await expect(dockerTab).toHaveClass(/active/);

    // Check for Docker panel visibility
    const dockerPanel = page.locator('#docker-panel');
    await expect(dockerPanel).toBeVisible();

    // Check for docker pull command
    const dockerPullCode = dockerPanel.locator('code:has-text("docker pull")');
    await expect(dockerPullCode).toBeVisible();

    // Check for docker run command
    const dockerRunCode = dockerPanel.locator('code:has-text("docker run")');
    await expect(dockerRunCode).toBeVisible();
  });

  test('TC4: Installation tabs or toggles work correctly', async ({ page }) => {
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeVisible();

    // Get tab buttons
    const cargoTab = page.locator('.tab-button[data-tab="cargo"]');
    const dockerTab = page.locator('.tab-button[data-tab="docker"]');
    const cargoPanel = page.locator('#cargo-panel');
    const dockerPanel = page.locator('#docker-panel');

    // Initially, cargo tab should be active and visible
    await expect(cargoTab).toHaveClass(/active/);
    await expect(cargoTab).toHaveAttribute('aria-selected', 'true');
    await expect(cargoPanel).toBeVisible();
    await expect(dockerPanel).toBeHidden();

    // Click Docker tab
    await dockerTab.click();

    // Docker should now be active
    await expect(dockerTab).toHaveClass(/active/);
    await expect(dockerTab).toHaveAttribute('aria-selected', 'true');
    await expect(cargoTab).not.toHaveClass(/active/);
    await expect(cargoTab).toHaveAttribute('aria-selected', 'false');

    // Docker panel should be visible, Cargo panel hidden
    await expect(dockerPanel).toBeVisible();
    await expect(cargoPanel).toBeHidden();

    // Click Cargo tab again
    await cargoTab.click();

    // Cargo should be active again
    await expect(cargoTab).toHaveClass(/active/);
    await expect(cargoPanel).toBeVisible();
    await expect(dockerPanel).toBeHidden();
  });

  test('Installation section has proper ARIA attributes for tabs', async ({ page }) => {
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeVisible();

    // Check tablist role
    const tablist = page.locator('.installation-tabs');
    await expect(tablist).toHaveAttribute('role', 'tablist');

    // Check tab roles
    const cargoTab = page.locator('#cargo-tab');
    await expect(cargoTab).toHaveAttribute('role', 'tab');
    await expect(cargoTab).toHaveAttribute('aria-controls', 'cargo-panel');

    const dockerTab = page.locator('#docker-tab');
    await expect(dockerTab).toHaveAttribute('role', 'tab');
    await expect(dockerTab).toHaveAttribute('aria-controls', 'docker-panel');

    // Check tabpanel roles
    const cargoPanel = page.locator('#cargo-panel');
    await expect(cargoPanel).toHaveAttribute('role', 'tabpanel');
    await expect(cargoPanel).toHaveAttribute('aria-labelledby', 'cargo-tab');

    const dockerPanel = page.locator('#docker-panel');
    await expect(dockerPanel).toHaveAttribute('role', 'tabpanel');
    await expect(dockerPanel).toHaveAttribute('aria-labelledby', 'docker-tab');
  });

  test('Installation code blocks have copy buttons', async ({ page }) => {
    const installationSection = page.locator('#installation');
    await expect(installationSection).toBeVisible();

    // Check cargo panel has copy buttons
    const cargoPanel = page.locator('#cargo-panel');
    const cargoCopyButtons = cargoPanel.locator('.copy-button');
    await expect(cargoCopyButtons).toHaveCount(3); // install, build, run

    // Check docker panel has copy buttons
    const dockerTab = page.locator('.tab-button[data-tab="docker"]');
    await dockerTab.click();

    const dockerPanel = page.locator('#docker-panel');
    const dockerCopyButtons = dockerPanel.locator('.copy-button');
    await expect(dockerCopyButtons).toHaveCount(3); // pull, run, compose
  });

  test('Installation section can be navigated to via anchor link', async ({ page }) => {
    // Navigate to top of page
    await page.goto('/');

    // Click on Get Started which should link to getting-started
    // Then check installation section can be reached by scrolling
    const installationSection = page.locator('#installation');

    // Scroll to installation section
    await installationSection.scrollIntoViewIfNeeded();
    await expect(installationSection).toBeVisible();
    await expect(installationSection).toBeInViewport();
  });
});
