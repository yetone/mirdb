// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Unit Tests for Code Examples Section Styling and Syntax Highlighting
 * Test Case 6: Verify syntax highlighting is applied
 * Expected: Code blocks have appropriate syntax highlighting for command examples
 */

test.describe('Code Examples Section Styling - Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');
    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 6: Verify syntax highlighting is applied
   * Input: Verify syntax highlighting is applied
   * Expected: Code blocks have appropriate syntax highlighting for command examples
   */
  test('TC6: Code blocks have syntax highlighting classes applied', async ({ page }) => {
    // Verify all code blocks have the language-memcached class
    const codeElements = page.locator('.code-block code');
    const count = await codeElements.count();

    expect(count).toBeGreaterThanOrEqual(8);

    for (let i = 0; i < count; i++) {
      const code = codeElements.nth(i);
      await expect(code).toBeVisible();

      const codeClass = await code.getAttribute('class');
      expect(codeClass).toContain('language-memcached');
    }
  });

  /**
   * Test: Verify syntax highlighting color is applied
   */
  test('TC6-B: Syntax highlighting color is applied to code blocks', async ({ page }) => {
    const codeElement = page.locator('[data-testid="code-get"]');
    await expect(codeElement).toBeVisible();

    const color = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Should have the syntax highlight color (#61afef = rgb(97, 175, 239))
    expect(color).toMatch(/rgb\(97,\s*175,\s*239\)/);
  });

  /**
   * Test: Verify code blocks use monospace font
   */
  test('TC6-C: Code blocks use monospace font family', async ({ page }) => {
    const codeElement = page.locator('[data-testid="code-get"]');
    const fontFamily = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Should contain monospace font
    expect(fontFamily.toLowerCase()).toMatch(/sfmono|consolas|liberation mono|menlo|monospace/);
  });

  /**
   * Test: Verify code blocks have dark background
   */
  test('TC6-D: Code blocks have dark background', async ({ page }) => {
    const codeBlock = page.locator('[data-testid="code-block-get"]');
    const bgColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Should be dark background (#1e1e1e = rgb(30, 30, 30))
    expect(bgColor).toMatch(/rgb\(30,\s*30,\s*30\)/);
  });

  /**
   * Test: Verify code blocks have proper line height
   */
  test('TC6-E: Code blocks have proper line height for readability', async ({ page }) => {
    const codeElement = page.locator('[data-testid="code-get"]');
    const lineHeight = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).lineHeight;
    });

    // Line height should be greater than 1 (at least 1.5)
    const lineHeightValue = parseFloat(lineHeight);
    expect(lineHeightValue).toBeGreaterThan(15); // At least 15px for good readability
  });

  /**
   * Test: Verify code blocks have proper padding
   */
  test('TC6-F: Code blocks have consistent padding', async ({ page }) => {
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThanOrEqual(8);

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

    // Padding should be 1rem (16px)
    expect(firstPadding).toMatch(/16px/);
  });

  /**
   * Test: Verify code blocks have border radius
   */
  test('TC6-G: Code blocks have proper border radius', async ({ page }) => {
    const codeBlock = page.locator('[data-testid="code-block-get"]');
    const borderRadius = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });

    // Should have border radius applied (6px)
    expect(borderRadius).not.toBe('0px');
    expect(parseInt(borderRadius)).toBeGreaterThan(0);
  });

  /**
   * Test: Verify command cards have hover effect styling
   */
  test('TC6-H: Command cards have proper card styling', async ({ page }) => {
    const commandCard = page.locator('[data-testid="command-card-get"]');
    await expect(commandCard).toBeVisible();

    const styles = await commandCard.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        borderRadius: computed.borderRadius,
        padding: computed.padding
      };
    });

    // Should have background color
    expect(styles.backgroundColor).toBeTruthy();
    expect(styles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

    // Should have border radius
    expect(parseInt(styles.borderRadius)).toBeGreaterThan(0);

    // Should have padding
    expect(styles.padding).not.toBe('0px');
  });

  /**
   * Test: Verify command titles have proper styling
   */
  test('TC6-I: Command titles have proper styling', async ({ page }) => {
    const commandTitle = page.locator('[data-testid="command-title-get"]');
    await expect(commandTitle).toBeVisible();

    const styles = await commandTitle.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        fontFamily: computed.fontFamily,
        color: computed.color,
        fontSize: computed.fontSize
      };
    });

    // Should use monospace font for command titles
    expect(styles.fontFamily.toLowerCase()).toMatch(/sfmono|consolas|liberation mono|menlo|monospace/);

    // Should have accessible dark primary color (WCAG AA compliant #2d3a7a = rgb(45, 58, 122))
    expect(styles.color).toMatch(/rgb\(45,\s*58,\s*122\)/);
  });

  /**
   * Test: Verify copy button positioning
   */
  test('TC6-J: Copy buttons are properly positioned', async ({ page }) => {
    const copyBtn = page.locator('[data-testid="copy-btn-get"]');
    await expect(copyBtn).toBeVisible();

    const position = await copyBtn.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        position: computed.position,
        top: computed.top,
        right: computed.right
      };
    });

    // Should be positioned absolutely
    expect(position.position).toBe('absolute');

    // Should be at top-right corner
    expect(parseInt(position.top)).toBeLessThan(20);
    expect(parseInt(position.right)).toBeLessThan(20);
  });

  /**
   * Test: Verify copy button styling
   */
  test('TC6-K: Copy buttons have proper styling', async ({ page }) => {
    const copyBtn = page.locator('[data-testid="copy-btn-get"]');
    await expect(copyBtn).toBeVisible();

    const styles = await copyBtn.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        cursor: computed.cursor,
        borderRadius: computed.borderRadius,
        padding: computed.padding
      };
    });

    // Should have pointer cursor
    expect(styles.cursor).toBe('pointer');

    // Should have border radius
    expect(parseInt(styles.borderRadius)).toBeGreaterThan(0);

    // Should have padding
    expect(styles.padding).not.toBe('0px');
  });

  /**
   * Test: Verify section renders without JavaScript errors
   */
  test('TC6-L: Code examples section renders without JavaScript errors', async ({ page }) => {
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

    // Scroll to code examples section
    await page.locator('#code-examples').scrollIntoViewIfNeeded();

    // Verify section is visible
    const section = page.locator('[data-testid="code-examples-section"]');
    await expect(section).toBeVisible();

    // Filter for code-examples related errors
    const sectionErrors = consoleErrors.filter(error =>
      error.toLowerCase().includes('code-example') ||
      error.toLowerCase().includes('command') ||
      error.toLowerCase().includes('copy')
    );

    const sectionPageErrors = pageErrors.filter(error =>
      error.toLowerCase().includes('code-example') ||
      error.toLowerCase().includes('command') ||
      error.toLowerCase().includes('copy')
    );

    // Verify no section-related errors
    expect(sectionErrors).toHaveLength(0);
    expect(sectionPageErrors).toHaveLength(0);
  });

  /**
   * Test: Verify all command descriptions are visible
   */
  test('TC6-M: All command descriptions have consistent styling', async ({ page }) => {
    const descriptions = page.locator('.command-description');
    const count = await descriptions.count();

    expect(count).toBeGreaterThanOrEqual(8);

    const styles = [];
    for (let i = 0; i < count; i++) {
      const desc = descriptions.nth(i);
      const style = await desc.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          fontSize: computed.fontSize,
          color: computed.color,
          lineHeight: computed.lineHeight
        };
      });
      styles.push(style);
    }

    // All descriptions should have consistent styling
    const firstStyle = styles[0];
    for (const style of styles) {
      expect(style.fontSize).toBe(firstStyle.fontSize);
      expect(style.lineHeight).toBe(firstStyle.lineHeight);
    }
  });

  /**
   * Test: Verify commands grid has proper gap
   */
  test('TC6-N: Commands grid has proper gap between cards', async ({ page }) => {
    const commandsGrid = page.locator('[data-testid="commands-grid"]');
    const gap = await commandsGrid.evaluate((el) => {
      return window.getComputedStyle(el).gap;
    });

    // Should have gap for spacing (1.5rem = 24px)
    expect(gap).toBeTruthy();
    expect(gap).not.toBe('0px');
    expect(gap).not.toBe('normal');
  });
});
