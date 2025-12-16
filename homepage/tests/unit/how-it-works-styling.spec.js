// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Unit Tests for How It Works / Architecture Section Styling
 * Tests visual appearance and CSS styling of the architecture overview section
 */

test.describe('How It Works Section Styling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.locator('#how-it-works').scrollIntoViewIfNeeded();
  });

  /**
   * Test: Section has correct background color
   */
  test('Section has correct background styling', async ({ page }) => {
    const section = page.locator('[data-testid="how-it-works-section"]');
    await expect(section).toBeVisible();

    // Check background color matches --bg-color (#f8f9fa)
    const bgColor = await section.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // RGB value of #f8f9fa
    expect(bgColor).toBe('rgb(248, 249, 250)');
  });

  /**
   * Test: Section title has correct styling
   */
  test('Section title has correct styling', async ({ page }) => {
    const title = page.locator('[data-testid="how-it-works-title"]');
    await expect(title).toBeVisible();

    const styles = await title.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        textAlign: computed.textAlign,
        fontSize: computed.fontSize,
        marginBottom: computed.marginBottom
      };
    });

    expect(styles.textAlign).toBe('center');
    // Font size should be 2.5rem (40px at default 16px root)
    expect(parseFloat(styles.fontSize)).toBeGreaterThanOrEqual(32);
  });

  /**
   * Test: Diagram container is centered
   */
  test('Architecture diagram container is centered', async ({ page }) => {
    const container = page.locator('[data-testid="architecture-diagram-container"]');
    await expect(container).toBeVisible();

    const styles = await container.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        justifyContent: computed.justifyContent
      };
    });

    expect(styles.display).toBe('flex');
    expect(styles.justifyContent).toBe('center');
  });

  /**
   * Test: Mermaid diagram has correct container styling
   */
  test('Mermaid diagram has correct container styling', async ({ page }) => {
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    const styles = await diagram.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        borderRadius: computed.borderRadius,
        padding: computed.padding,
        minHeight: computed.minHeight
      };
    });

    // Background should be white (#ffffff)
    expect(styles.backgroundColor).toBe('rgb(255, 255, 255)');
    // Border radius should be 8px (--border-radius)
    expect(styles.borderRadius).toBe('8px');
    // Padding should be 2rem (32px)
    expect(parseFloat(styles.padding)).toBeGreaterThanOrEqual(16);
    // Min height should be set
    expect(parseFloat(styles.minHeight)).toBeGreaterThanOrEqual(300);
  });

  /**
   * Test: Data path cards have correct styling
   */
  test('Data path cards have correct styling', async ({ page }) => {
    const writeCard = page.locator('[data-testid="write-path-card"]');
    const readCard = page.locator('[data-testid="read-path-card"]');

    await expect(writeCard).toBeVisible();
    await expect(readCard).toBeVisible();

    // Check write card styling
    const writeStyles = await writeCard.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        borderRadius: computed.borderRadius,
        padding: computed.padding,
        boxShadow: computed.boxShadow
      };
    });

    // Background should be white
    expect(writeStyles.backgroundColor).toBe('rgb(255, 255, 255)');
    // Border radius should be 8px
    expect(writeStyles.borderRadius).toBe('8px');
    // Should have box shadow
    expect(writeStyles.boxShadow).not.toBe('none');
  });

  /**
   * Test: Data path titles have correct color
   */
  test('Data path titles use primary color', async ({ page }) => {
    const writeTitle = page.locator('[data-testid="write-path-title"]');
    const readTitle = page.locator('[data-testid="read-path-title"]');

    await expect(writeTitle).toBeVisible();
    await expect(readTitle).toBeVisible();

    const writeTitleColor = await writeTitle.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Primary color is #667eea (rgb(102, 126, 234))
    expect(writeTitleColor).toBe('rgb(102, 126, 234)');
  });

  /**
   * Test: Data paths grid uses CSS grid layout
   */
  test('Data paths grid uses correct layout', async ({ page }) => {
    const grid = page.locator('[data-testid="data-paths-grid"]');
    await expect(grid).toBeVisible();

    const styles = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gap: computed.gap
      };
    });

    expect(styles.display).toBe('grid');
    // Gap should be 2rem (32px)
    expect(parseFloat(styles.gap)).toBeGreaterThanOrEqual(16);
  });

  /**
   * Test: Data path cards have hover transform effect
   */
  test('Data path cards have transition for hover effects', async ({ page }) => {
    const writeCard = page.locator('[data-testid="write-path-card"]');
    await expect(writeCard).toBeVisible();

    const transition = await writeCard.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });

    // Should have transition property set
    expect(transition).not.toBe('none');
    expect(transition).toContain('0.3s');
  });

  /**
   * Test: Visually hidden class hides element visually but keeps it accessible
   */
  test('Visually hidden class properly hides alt text', async ({ page }) => {
    const altText = page.locator('[data-testid="architecture-diagram-alt"]');
    await expect(altText).toBeAttached();

    const styles = await altText.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        position: computed.position,
        width: computed.width,
        height: computed.height,
        overflow: computed.overflow,
        clip: computed.clip
      };
    });

    // Should be positioned absolutely and clipped
    expect(styles.position).toBe('absolute');
    expect(styles.width).toBe('1px');
    expect(styles.height).toBe('1px');
    expect(styles.overflow).toBe('hidden');
  });

  /**
   * Test: Steps list has correct styling
   */
  test('Data path steps have correct list styling', async ({ page }) => {
    const writeSteps = page.locator('[data-testid="write-path-steps"]');
    await expect(writeSteps).toBeVisible();

    const styles = await writeSteps.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        paddingLeft: computed.paddingLeft,
        lineHeight: computed.lineHeight
      };
    });

    // Should have left padding for list markers
    expect(parseFloat(styles.paddingLeft)).toBeGreaterThanOrEqual(16);
    // Line height should be comfortable for reading
    expect(parseFloat(styles.lineHeight)).toBeGreaterThanOrEqual(20);
  });

  /**
   * Test: Step strong elements use primary color
   */
  test('Step labels use primary color styling', async ({ page }) => {
    const stepStrong = page.locator('[data-testid="write-step-1"] strong');
    await expect(stepStrong).toBeVisible();

    const color = await stepStrong.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Primary color is #667eea (rgb(102, 126, 234))
    expect(color).toBe('rgb(102, 126, 234)');
  });
});
