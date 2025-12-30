// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests for MirDB Homepage
 * Verifies homepage renders correctly across Chrome, Firefox, Safari, and Edge (NFR-5)
 *
 * Test cases:
 * TC1: All sections render correctly (hero, features, commands, quick-start, status, footer)
 * TC2: Navigation works correctly (all links function)
 * TC3: Code blocks display properly (syntax highlighting, formatting)
 * TC4: Responsive layout elements render correctly
 *
 * These tests run across all configured browsers in playwright.config.js:
 * - chromium (Chrome)
 * - firefox (Firefox)
 * - webkit (Safari)
 * - msedge (Microsoft Edge)
 */

test.describe('Cross-Browser Compatibility (NFR-5)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Section Rendering', () => {
    test('TC1.1: Hero section renders correctly', async ({ page }) => {
      // Verify hero section is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify product name displays
      const productName = heroSection.locator('h1');
      await expect(productName).toBeVisible();
      await expect(productName).toContainText('MirDB');

      // Verify tagline displays
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify hero buttons are visible
      const getStartedBtn = heroSection.locator('a:has-text("Get Started")');
      const githubBtn = heroSection.locator('a:has-text("View on GitHub")');
      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();
    });

    test('TC1.2: Features section renders correctly', async ({ page }) => {
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify section heading
      const heading = featuresSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('Key Features');

      // Verify all 5 feature cards are rendered
      const featureCards = page.locator('[data-testid="feature-card"]');
      await expect(featureCards).toHaveCount(5);

      // Verify each card has heading and description
      const expectedFeatures = [
        'Memcached Protocol',
        'Persistent Storage',
        'LSM Tree Engine',
        'Async I/O',
        'Configurable'
      ];

      for (let i = 0; i < expectedFeatures.length; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();
        const cardHeading = card.locator('h3');
        await expect(cardHeading).toContainText(expectedFeatures[i]);
      }
    });

    test('TC1.3: Commands section renders correctly', async ({ page }) => {
      const commandsSection = page.locator('[data-testid="commands-section"]');
      await expect(commandsSection).toBeVisible();

      // Verify section heading
      const heading = commandsSection.locator('h2');
      await expect(heading).toContainText('Supported Commands');

      // Verify all expected commands are displayed
      const expectedCommands = [
        'SET', 'GET', 'DELETE', 'ADD', 'REPLACE',
        'APPEND', 'PREPEND', 'INFO', 'MAJOR_COMPACTION'
      ];

      for (const command of expectedCommands) {
        const commandItem = page.locator(`[data-testid="command-${command}"]`);
        await expect(commandItem).toBeVisible();
        const codeElement = commandItem.locator('code');
        await expect(codeElement).toContainText(command);
      }
    });

    test('TC1.4: Quick Start section renders correctly', async ({ page }) => {
      const quickStartSection = page.locator('[data-testid="quick-start"]');
      await expect(quickStartSection).toBeVisible();

      // Verify section heading
      const heading = quickStartSection.locator('h2');
      await expect(heading).toContainText('Quick Start');

      // Verify code block is present
      const codeBlock = quickStartSection.locator('.code-block pre code');
      await expect(codeBlock).toBeVisible();

      // Verify code content includes key commands
      const codeText = await codeBlock.textContent();
      expect(codeText).toContain('cargo install mirdb');
      expect(codeText).toContain('telnet localhost 12333');
      expect(codeText).toContain('STORED');
    });

    test('TC1.5: Project Status section renders correctly', async ({ page }) => {
      const statusSection = page.locator('[data-testid="status-section"]');
      await expect(statusSection).toBeVisible();

      // Verify section heading
      const heading = statusSection.locator('h2');
      await expect(heading).toContainText('Project Status');

      // Verify status grid is present
      const statusGrid = page.locator('[data-testid="status-grid"]');
      await expect(statusGrid).toBeVisible();

      // Verify implemented features are marked correctly
      const implementedItems = [
        'async-networking',
        'memtable',
        'minor-compaction',
        'major-compaction'
      ];

      for (const item of implementedItems) {
        const statusItem = page.locator(`[data-testid="status-item-${item}"]`);
        await expect(statusItem).toBeVisible();
        await expect(statusItem).toHaveAttribute('data-status', 'implemented');
      }

      // Verify planned feature
      const plannedItem = page.locator('[data-testid="status-item-raft-consensus"]');
      await expect(plannedItem).toBeVisible();
      await expect(plannedItem).toHaveAttribute('data-status', 'planned');
    });

    test('TC1.6: Footer section renders correctly', async ({ page }) => {
      const footerSection = page.locator('[data-testid="footer-section"]');
      await expect(footerSection).toBeVisible();

      // Verify footer links
      const githubLink = page.locator('[data-testid="footer-github-link"]');
      const docsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(githubLink).toBeVisible();
      await expect(docsLink).toBeVisible();

      // Verify license info
      const licenseInfo = page.locator('[data-testid="license-info"]');
      await expect(licenseInfo).toBeVisible();
      await expect(licenseInfo).toContainText('MIT/Apache-2.0');
    });
  });

  test.describe('Navigation Functionality', () => {
    test('TC2.1: Navigation header links work correctly', async ({ page }) => {
      const navLinks = page.locator('[data-testid="nav-links"]');
      await expect(navLinks).toBeVisible();

      // Test Features link
      const featuresLink = navLinks.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();
      await expect(page).toHaveURL(/#features$/);

      // Test Commands link
      const commandsLink = navLinks.locator('a[href="#commands"]');
      await expect(commandsLink).toBeVisible();
      await commandsLink.click();
      await expect(page).toHaveURL(/#commands$/);

      // Test Quick Start link
      const quickStartLink = navLinks.locator('a[href="#quick-start"]');
      await expect(quickStartLink).toBeVisible();
      await quickStartLink.click();
      await expect(page).toHaveURL(/#quick-start$/);
    });

    test('TC2.2: GitHub link has correct attributes', async ({ page }) => {
      const navLinks = page.locator('[data-testid="nav-links"]');
      const githubLink = navLinks.locator('a:has-text("GitHub")');

      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', /github\.com/);
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', /noopener/);
    });

    test('TC2.3: Logo link navigates to home', async ({ page }) => {
      const logoLink = page.locator('.logo');
      await expect(logoLink).toBeVisible();
      await expect(logoLink).toHaveAttribute('href', '/');
    });

    test('TC2.4: Get Started button scrolls to Quick Start', async ({ page }) => {
      const getStartedBtn = page.locator('a.btn-primary:has-text("Get Started")');
      await expect(getStartedBtn).toBeVisible();

      await getStartedBtn.click();
      await expect(page).toHaveURL(/#quick-start$/);

      // Verify quick start section is in view
      const quickStartSection = page.locator('[data-testid="quick-start"]');
      await expect(quickStartSection).toBeInViewport();
    });

    test('TC2.5: Footer navigation links work', async ({ page }) => {
      const footerLinks = page.locator('[data-testid="footer-links"]');

      // Test GitHub link in footer
      const githubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(githubLink).toHaveAttribute('href', /github\.com/);
      await expect(githubLink).toHaveAttribute('target', '_blank');

      // Test docs link
      const docsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(docsLink).toHaveAttribute('href', '#quick-start');
    });
  });

  test.describe('Code Blocks Display', () => {
    test('TC3.1: Code block is properly formatted', async ({ page }) => {
      const codeBlock = page.locator('[data-testid="quick-start"] .code-block');
      await expect(codeBlock).toBeVisible();

      // Verify pre and code elements exist
      const preElement = codeBlock.locator('pre');
      const codeElement = preElement.locator('code');
      await expect(preElement).toBeVisible();
      await expect(codeElement).toBeVisible();

      // Verify code block has proper styling (monospace font)
      const fontFamily = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      // Check that font-family includes monospace font
      expect(fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/i);
    });

    test('TC3.2: Code block preserves formatting', async ({ page }) => {
      const codeElement = page.locator('[data-testid="quick-start"] .code-block pre code');
      const codeText = await codeElement.textContent();

      // Verify multi-line content is preserved
      expect(codeText).toContain('\n');

      // Verify indentation/structure is maintained
      expect(codeText).toContain('# Install MirDB');
      expect(codeText).toContain('$ cargo install mirdb');
    });

    test('TC3.3: Command codes in commands section display correctly', async ({ page }) => {
      const commandItems = page.locator('[data-testid^="command-"] code');
      const count = await commandItems.count();

      // Verify all command codes are visible
      expect(count).toBe(9);

      for (let i = 0; i < count; i++) {
        const code = commandItems.nth(i);
        await expect(code).toBeVisible();

        // Verify code elements have monospace styling
        const fontFamily = await code.evaluate((el) => {
          return window.getComputedStyle(el).fontFamily;
        });
        expect(fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/i);
      }
    });
  });

  test.describe('Visual Layout Rendering', () => {
    test('TC4.1: Page has proper document structure', async ({ page }) => {
      // Verify HTML lang attribute
      const html = page.locator('html');
      await expect(html).toHaveAttribute('lang', 'en');

      // Verify main content area
      const main = page.locator('main#main-content');
      await expect(main).toBeVisible();

      // Verify skip link for accessibility
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toBeAttached();
    });

    test('TC4.2: Feature cards grid displays correctly', async ({ page }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Verify grid container has proper display
      const display = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(['grid', 'flex']).toContain(display);
    });

    test('TC4.3: Commands grid displays correctly', async ({ page }) => {
      const commandsGrid = page.locator('.commands-grid');
      await expect(commandsGrid).toBeVisible();

      // Verify grid container has proper display
      const display = await commandsGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(['grid', 'flex']).toContain(display);
    });

    test('TC4.4: Status grid displays correctly', async ({ page }) => {
      const statusGrid = page.locator('[data-testid="status-grid"]');
      await expect(statusGrid).toBeVisible();

      // Verify grid container has proper display
      const display = await statusGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(['grid', 'flex']).toContain(display);
    });

    test('TC4.5: Hero buttons have proper styling', async ({ page }) => {
      const heroSection = page.locator('[data-testid="hero-section"]');
      const primaryBtn = heroSection.locator('.btn-primary');
      const secondaryBtn = heroSection.locator('.btn-secondary');

      await expect(primaryBtn).toBeVisible();
      await expect(secondaryBtn).toBeVisible();

      // Verify buttons have distinct background colors
      const primaryBg = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      const secondaryBg = await secondaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Primary and secondary buttons should have different styles
      expect(primaryBg).not.toBe(secondaryBg);
    });
  });
});
