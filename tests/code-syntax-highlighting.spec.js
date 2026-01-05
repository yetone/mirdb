const { test, expect } = require('@playwright/test');

test.describe('Code Syntax Highlighting (NFR-6)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check code blocks use pre/code elements (Unit)
  test('TC1: Code examples are wrapped in <pre><code> elements', async ({ page }) => {
    // Navigate to quickstart section which contains code examples
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify code blocks exist with pre element
    const preElements = quickstartSection.locator('pre');
    await expect(preElements.first()).toBeVisible();

    // Verify code elements exist within the code block
    const codeElements = quickstartSection.locator('code');
    const codeCount = await codeElements.count();
    expect(codeCount).toBeGreaterThan(0);

    // Verify code elements are inside pre elements
    const preContent = await preElements.first().innerHTML();
    expect(preContent).toContain('<code');
  });

  // Test Case 2: Check code blocks have monospace font (E2E)
  test('TC2: Code examples display in monospace font family', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get the pre element containing code
    const preElement = quickstartSection.locator('.code-block pre').first();
    await expect(preElement).toBeVisible();

    // Check that the computed font-family is monospace
    const fontFamily = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Verify it uses a monospace font (common monospace fonts or generic 'monospace')
    const isMonospace = fontFamily.toLowerCase().includes('monaco') ||
                        fontFamily.toLowerCase().includes('menlo') ||
                        fontFamily.toLowerCase().includes('monospace') ||
                        fontFamily.toLowerCase().includes('consolas') ||
                        fontFamily.toLowerCase().includes('courier') ||
                        fontFamily.toLowerCase().includes('ubuntu mono');
    expect(isMonospace).toBe(true);

    // Also check code elements
    const codeElement = quickstartSection.locator('.code-block code').first();
    if (await codeElement.count() > 0) {
      const codeFontFamily = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      const codeIsMonospace = codeFontFamily.toLowerCase().includes('monaco') ||
                              codeFontFamily.toLowerCase().includes('menlo') ||
                              codeFontFamily.toLowerCase().includes('monospace') ||
                              codeFontFamily.toLowerCase().includes('consolas') ||
                              codeFontFamily.toLowerCase().includes('courier') ||
                              codeFontFamily.toLowerCase().includes('ubuntu mono');
      expect(codeIsMonospace).toBe(true);
    }
  });

  // Test Case 3: Check code blocks have syntax highlighting classes (Unit)
  test('TC3: Code blocks have syntax highlighting applied (via classes or inline styles)', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for syntax highlighting classes or inline styles
    // The page uses .code-comment for comments and <code> for commands
    const codeComments = quickstartSection.locator('.code-comment');
    const commentCount = await codeComments.count();
    expect(commentCount).toBeGreaterThan(0);

    // Verify comments have different color than regular code
    const commentColor = await codeComments.first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Check that code elements exist and have styling
    const codeElements = quickstartSection.locator('.code-block code');
    const codeCount = await codeElements.count();
    expect(codeCount).toBeGreaterThan(0);

    // Verify code has different color (syntax highlighting)
    const codeColor = await codeElements.first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // The comment and code should have different colors for syntax highlighting
    // or at least one of them should have a distinct color
    const hasDistinctColors = commentColor !== codeColor ||
                              commentColor !== 'rgb(248, 250, 252)' || // not just default text color
                              codeColor !== 'rgb(248, 250, 252)';
    expect(hasDistinctColors).toBe(true);

    // Verify the colors are not the same as the background
    const preElement = quickstartSection.locator('.code-block pre').first();
    const preColor = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Either comments or code should be visually differentiated
    const hasSyntaxHighlighting = commentColor !== preColor || codeColor !== preColor;
    expect(hasSyntaxHighlighting).toBe(true);
  });

  // Test Case 4: Check code blocks are readable (E2E)
  test('TC4: Code blocks have sufficient contrast and are easily readable', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get the code block
    const codeBlock = quickstartSection.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Get background color and text color
    const backgroundColor = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    const preElement = quickstartSection.locator('.code-block pre').first();
    const textColor = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Parse RGB values
    const parsedBg = parseRgb(backgroundColor);
    const parsedText = parseRgb(textColor);

    // Calculate contrast ratio (simplified check)
    // WCAG AA requires at least 4.5:1 for normal text
    const bgLuminance = calculateLuminance(parsedBg);
    const textLuminance = calculateLuminance(parsedText);
    const contrastRatio = calculateContrastRatio(bgLuminance, textLuminance);

    // Verify contrast ratio meets WCAG AA standard (4.5:1)
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);

    // Also check code element contrast
    const codeElement = quickstartSection.locator('.code-block code').first();
    if (await codeElement.count() > 0) {
      const codeColor = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      const parsedCode = parseRgb(codeColor);
      const codeLuminance = calculateLuminance(parsedCode);
      const codeContrastRatio = calculateContrastRatio(bgLuminance, codeLuminance);
      expect(codeContrastRatio).toBeGreaterThanOrEqual(4.5);
    }

    // Check comment contrast
    const commentElement = quickstartSection.locator('.code-comment').first();
    if (await commentElement.count() > 0) {
      const commentColor = await commentElement.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      const parsedComment = parseRgb(commentColor);
      const commentLuminance = calculateLuminance(parsedComment);
      const commentContrastRatio = calculateContrastRatio(bgLuminance, commentLuminance);
      // Comments should have at least 3:1 contrast (WCAG AA for large text)
      expect(commentContrastRatio).toBeGreaterThanOrEqual(3);
    }
  });

  // Additional test: Verify syntax highlighting elements are visually distinct
  test('TC5: Different syntax elements have distinct visual styling', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get colors for different syntax elements
    const codeElements = quickstartSection.locator('.code-block code');
    const commentElements = quickstartSection.locator('.code-comment');

    // Ensure both elements exist
    await expect(codeElements.first()).toBeVisible();
    await expect(commentElements.first()).toBeVisible();

    // Get their colors
    const codeColor = await codeElements.first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    const commentColor = await commentElements.first().evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // They should have different colors for visual distinction
    expect(codeColor).not.toBe(commentColor);
  });
});

// Helper function to parse RGB color string
function parseRgb(rgbString) {
  const match = rgbString.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
  if (match) {
    return {
      r: parseInt(match[1]),
      g: parseInt(match[2]),
      b: parseInt(match[3])
    };
  }
  // Handle rgba
  const rgbaMatch = rgbString.match(/rgba\((\d+),\s*(\d+),\s*(\d+),\s*[\d.]+\)/);
  if (rgbaMatch) {
    return {
      r: parseInt(rgbaMatch[1]),
      g: parseInt(rgbaMatch[2]),
      b: parseInt(rgbaMatch[3])
    };
  }
  return { r: 0, g: 0, b: 0 };
}

// Helper function to calculate relative luminance
function calculateLuminance({ r, g, b }) {
  const sR = r / 255;
  const sG = g / 255;
  const sB = b / 255;

  const R = sR <= 0.03928 ? sR / 12.92 : Math.pow((sR + 0.055) / 1.055, 2.4);
  const G = sG <= 0.03928 ? sG / 12.92 : Math.pow((sG + 0.055) / 1.055, 2.4);
  const B = sB <= 0.03928 ? sB / 12.92 : Math.pow((sB + 0.055) / 1.055, 2.4);

  return 0.2126 * R + 0.7152 * G + 0.0722 * B;
}

// Helper function to calculate contrast ratio
function calculateContrastRatio(lum1, lum2) {
  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);
  return (lighter + 0.05) / (darker + 0.05);
}
