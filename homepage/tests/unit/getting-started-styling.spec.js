// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Unit Tests for Getting Started Section Styling and Syntax Highlighting
 * Test Case 4: Verify code blocks are syntax highlighted
 * Expected: Installation commands and config examples have proper syntax highlighting
 */

test.describe('Getting Started Section Styling - Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');
    // Scroll to getting started section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 4: Verify code blocks are syntax highlighted
   * Input: Verify code blocks are syntax highlighted
   * Expected: Installation commands and config examples have proper syntax highlighting
   */
  test('TC4: Code blocks have syntax highlighting classes applied', async ({ page }) => {
    // Verify installation code block has language-bash class
    const installationCode = page.locator('[data-testid="installation-code"] code');
    await expect(installationCode).toBeVisible();

    const bashClass = await installationCode.getAttribute('class');
    expect(bashClass).toContain('language-bash');

    // Verify configuration code block has language-toml class
    const configCode = page.locator('[data-testid="configuration-code"] code');
    await expect(configCode).toBeVisible();

    const tomlClass = await configCode.getAttribute('class');
    expect(tomlClass).toContain('language-toml');
  });

  /**
   * Test: Verify syntax highlighting styles are applied to code blocks
   */
  test('TC4-B: Syntax highlighting colors are distinct for different languages', async ({ page }) => {
    // Get bash code color
    const installationCode = page.locator('[data-testid="installation-code"] code');
    const bashColor = await installationCode.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Get TOML code color
    const configCode = page.locator('[data-testid="configuration-code"] code');
    const tomlColor = await configCode.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Both should have syntax highlighting colors applied (not default white/gray)
    // Bash: #98c379 = rgb(152, 195, 121)
    // TOML: #e5c07b = rgb(229, 192, 123)
    expect(bashColor).toMatch(/rgb\(152,\s*195,\s*121\)/);
    expect(tomlColor).toMatch(/rgb\(229,\s*192,\s*123\)/);

    // Verify colors are different (different syntax highlighting)
    expect(bashColor).not.toBe(tomlColor);
  });

  /**
   * Test: Verify code blocks have monospace font
   */
  test('TC4-C: Code blocks use monospace font family', async ({ page }) => {
    const installationCode = page.locator('[data-testid="installation-code"] code');
    const fontFamily = await installationCode.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Should contain monospace font
    expect(fontFamily.toLowerCase()).toMatch(/sfmono|consolas|liberation mono|menlo|monospace/);

    // Verify same font for TOML code
    const configCode = page.locator('[data-testid="configuration-code"] code');
    const tomlFontFamily = await configCode.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    expect(tomlFontFamily.toLowerCase()).toMatch(/sfmono|consolas|liberation mono|menlo|monospace/);
  });

  /**
   * Test: Verify code blocks have proper line height for readability
   */
  test('TC4-D: Code blocks have proper line height for readability', async ({ page }) => {
    const installationCode = page.locator('[data-testid="installation-code"] code');
    const lineHeight = await installationCode.evaluate((el) => {
      return window.getComputedStyle(el).lineHeight;
    });

    // Line height should be greater than 1 (typically 1.5 or 1.6)
    const lineHeightValue = parseFloat(lineHeight);
    expect(lineHeightValue).toBeGreaterThan(20); // At least 20px for good readability
  });

  /**
   * Test: Verify code example containers have border radius
   */
  test('TC4-E: Code example containers have proper border radius', async ({ page }) => {
    const installationContainer = page.locator('[data-testid="installation-code"]');
    const borderRadius = await installationContainer.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });

    // Should have border radius applied (8px from CSS variables)
    expect(borderRadius).not.toBe('0px');
    expect(parseInt(borderRadius)).toBeGreaterThan(0);
  });

  /**
   * Test: Verify code containers have proper padding
   */
  test('TC4-F: Code containers have consistent padding', async ({ page }) => {
    const codeBlocks = page.locator('.code-example');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThanOrEqual(2);

    // Collect padding values
    const paddings = [];
    for (let i = 0; i < count; i++) {
      const block = codeBlocks.nth(i);
      const padding = await block.evaluate((el) => {
        return window.getComputedStyle(el).padding;
      });
      paddings.push(padding);
    }

    // All code blocks should have consistent padding
    const firstPadding = paddings[0];
    for (const padding of paddings) {
      expect(padding).toBe(firstPadding);
    }

    // Padding should be 2rem (32px)
    expect(firstPadding).toMatch(/32px/);
  });

  /**
   * Test: Verify subsection titles have consistent styling
   */
  test('TC4-G: Subsection titles have consistent styling', async ({ page }) => {
    const subsectionTitles = page.locator('.subsection-title');
    const count = await subsectionTitles.count();

    expect(count).toBeGreaterThanOrEqual(2);

    // Get styles for all subsection titles
    const styles = [];
    for (let i = 0; i < count; i++) {
      const title = subsectionTitles.nth(i);
      const style = await title.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          fontSize: computed.fontSize,
          color: computed.color,
          textAlign: computed.textAlign
        };
      });
      styles.push(style);
    }

    // All titles should have consistent styling
    const firstStyle = styles[0];
    for (const style of styles) {
      expect(style.fontSize).toBe(firstStyle.fontSize);
      expect(style.color).toBe(firstStyle.color);
      expect(style.textAlign).toBe(firstStyle.textAlign);
    }

    // Should be center-aligned
    expect(firstStyle.textAlign).toBe('center');
  });

  /**
   * Test: Verify documentation links have button styling
   */
  test('TC4-H: Documentation links have proper button styling', async ({ page }) => {
    const docsLink = page.locator('[data-testid="docs-link"]');
    await expect(docsLink).toBeVisible();

    const buttonStyles = await docsLink.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        padding: computed.padding,
        backgroundColor: computed.backgroundColor,
        borderRadius: computed.borderRadius,
        fontWeight: computed.fontWeight
      };
    });

    // Should be block or inline-block for button display (block in flex container)
    expect(['block', 'inline-block']).toContain(buttonStyles.display);

    // Should have padding for button appearance
    expect(buttonStyles.padding).not.toBe('0px');

    // Should have accessible primary color background (WCAG AA: #4a5bb8 = rgb(74, 91, 184))
    expect(buttonStyles.backgroundColor).toMatch(/rgb\(74,\s*91,\s*184\)/);

    // Should have border radius
    expect(parseInt(buttonStyles.borderRadius)).toBeGreaterThan(0);

    // Should have bold font weight
    expect(parseInt(buttonStyles.fontWeight)).toBeGreaterThanOrEqual(600);
  });

  /**
   * Test: Verify section renders without JavaScript errors
   */
  test('TC4-I: Getting started section renders without JavaScript errors', async ({ page }) => {
    const consoleErrors = [];
    const pageErrors = [];

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    page.on('pageerror', (error) => {
      pageErrors.push(error.message);
    });

    // Navigate and wait for load
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Scroll to getting started section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Verify section is visible
    const section = page.locator('[data-testid="getting-started-section"]');
    await expect(section).toBeVisible();

    // Filter for getting-started related errors
    const sectionErrors = consoleErrors.filter(error =>
      error.toLowerCase().includes('getting-started') ||
      error.toLowerCase().includes('installation') ||
      error.toLowerCase().includes('configuration')
    );

    const sectionPageErrors = pageErrors.filter(error =>
      error.toLowerCase().includes('getting-started') ||
      error.toLowerCase().includes('installation') ||
      error.toLowerCase().includes('configuration')
    );

    // Verify no section-related errors
    expect(sectionErrors).toHaveLength(0);
    expect(sectionPageErrors).toHaveLength(0);
  });
});
