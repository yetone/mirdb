/**
 * E2E Tests for Installation and Setup Instructions
 * Scenario: Installation and Setup Instructions
 * Test Cases: 1, 2, 4
 * Validates that comprehensive installation and setup instructions are provided
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Installation and Setup Instructions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test.describe('Test Case 1: Installation Commands', () => {
    test('should have quick-start section with installation commands', async ({ page }) => {
      const quickStart = page.locator('#quick-start');
      await expect(quickStart).toBeVisible();
    });

    test('should contain cargo install or git clone command', async ({ page }) => {
      const quickStart = page.locator('#quick-start');
      const text = await quickStart.textContent();

      // Check for cargo or git clone installation commands
      const hasCargoInstall = text.includes('cargo install') || text.includes('cargo build');
      const hasGitClone = text.includes('git clone');

      expect(hasCargoInstall || hasGitClone).toBe(true);
    });

    test('should display installation commands in code blocks', async ({ page }) => {
      const quickStart = page.locator('#quick-start');
      const codeBlocks = quickStart.locator('pre, code');

      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('Test Case 2: Server Startup Command', () => {
    test('should show how to start MirDB server', async ({ page }) => {
      const quickStart = page.locator('#quick-start');
      const text = await quickStart.textContent();

      // Check for server startup commands
      const hasCargoRun = text.includes('cargo run');
      const hasMirdbCommand = text.includes('mirdb') || text.includes('./mirdb');
      const hasStartInstruction = text.toLowerCase().includes('start') ||
                                   text.toLowerCase().includes('run');

      expect(hasCargoRun || hasMirdbCommand || hasStartInstruction).toBe(true);
    });

    test('should include startup command in a code block', async ({ page }) => {
      const quickStart = page.locator('#quick-start');
      const codeBlocks = quickStart.locator('pre');

      let foundStartupCommand = false;
      const count = await codeBlocks.count();

      for (let i = 0; i < count; i++) {
        const text = await codeBlocks.nth(i).textContent();
        if (text.includes('cargo run') || text.includes('mirdb')) {
          foundStartupCommand = true;
          break;
        }
      }

      expect(foundStartupCommand).toBe(true);
    });
  });

  test.describe('Test Case 4: Rust Prerequisite Mention', () => {
    test('should mention Rust or Cargo as prerequisite', async ({ page }) => {
      const quickStart = page.locator('#quick-start');
      const text = await quickStart.textContent();
      const lowerText = text.toLowerCase();

      // Check for Rust/Cargo mentions
      const mentionsRust = lowerText.includes('rust');
      const mentionsCargo = lowerText.includes('cargo');
      const mentionsPrerequisite = lowerText.includes('prerequisite') ||
                                    lowerText.includes('requirement') ||
                                    lowerText.includes('install rust') ||
                                    lowerText.includes('need rust');

      expect(mentionsRust || mentionsCargo).toBe(true);
    });

    test('should link to Rust installation or mention rustup', async ({ page }) => {
      const quickStart = page.locator('#quick-start');

      // Check for rust-lang.org link or rustup mention
      const rustLinks = quickStart.locator('a[href*="rust-lang.org"], a[href*="rustup"]');
      const linkCount = await rustLinks.count();

      const text = await quickStart.textContent();
      const mentionsRustup = text.toLowerCase().includes('rustup') ||
                              text.toLowerCase().includes('rust-lang.org');

      expect(linkCount > 0 || mentionsRustup).toBe(true);
    });
  });

  test.describe('Quick Start Section Accessibility', () => {
    test('should be accessible via Get Started button', async ({ page }) => {
      const getStartedBtn = page.locator('a[href="#quick-start"]');
      await expect(getStartedBtn).toBeVisible();

      await getStartedBtn.click();

      const quickStart = page.locator('#quick-start');
      await expect(quickStart).toBeInViewport();
    });

    test('should have proper heading for the section', async ({ page }) => {
      const quickStart = page.locator('#quick-start');
      const heading = quickStart.locator('h2');

      await expect(heading).toBeVisible();
      const text = await heading.textContent();
      expect(text.toLowerCase()).toMatch(/quick\s*start|getting\s*started|installation/);
    });
  });
});
