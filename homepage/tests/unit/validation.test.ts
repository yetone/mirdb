/**
 * HTML and CSS Validation Tests
 * Owner: Scenario 13 - HTML and CSS Validation
 *
 * Tests for W3C validation compliance, semantic markup,
 * and web standards for the MirDB homepage.
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { readFileSync, existsSync } from 'fs';
import { resolve, join } from 'path';
import { execSync } from 'child_process';

// Base paths
const rootDir = resolve(__dirname, '../..');
const indexPath = join(rootDir, 'index.html');
const cssDir = join(rootDir, 'css');

// Read HTML content once
let htmlContent: string;

beforeAll(() => {
  htmlContent = readFileSync(indexPath, 'utf-8');
});

describe('HTML Validation', () => {
  /**
   * Test Case 1: Run W3C HTML validator on homepage
   * Expected: No HTML validation errors
   */
  test('should pass W3C HTML validation with no errors', () => {
    // Run html-validate (W3C-style validator)
    try {
      const result = execSync(`npx html-validate "${indexPath}" --formatter json`, {
        cwd: rootDir,
        encoding: 'utf-8',
        stdio: ['pipe', 'pipe', 'pipe']
      });

      const validationResult = JSON.parse(result);

      // Check for errors (warnings are acceptable per requirements)
      const errors = validationResult.flatMap((file: any) =>
        file.messages.filter((msg: any) => msg.severity === 2)
      );

      expect(errors).toHaveLength(0);
    } catch (error: any) {
      // If html-validate exits with non-zero, parse the output
      if (error.stdout) {
        const validationResult = JSON.parse(error.stdout);
        const errors = validationResult.flatMap((file: any) =>
          file.messages.filter((msg: any) => msg.severity === 2)
        );

        // Filter out non-critical errors (severity 2 = error)
        expect(errors).toHaveLength(0);
      } else {
        throw error;
      }
    }
  });

  /**
   * Test Case 3: Check for DOCTYPE declaration
   * Expected: Page starts with <!DOCTYPE html>
   */
  test('should have DOCTYPE declaration at the start', () => {
    const trimmedContent = htmlContent.trimStart();
    const hasDoctype = trimmedContent.toLowerCase().startsWith('<!doctype html>');

    expect(hasDoctype).toBe(true);
  });

  /**
   * Test Case 4: Check for lang attribute on html element
   * Expected: html element has lang='en' attribute
   */
  test('should have lang="en" attribute on html element', () => {
    // Match <html with lang attribute
    const htmlTagMatch = htmlContent.match(/<html[^>]*>/i);
    expect(htmlTagMatch).not.toBeNull();

    const htmlTag = htmlTagMatch![0];
    const hasLangEn = /lang\s*=\s*["']en["']/i.test(htmlTag);

    expect(hasLangEn).toBe(true);
  });

  /**
   * Test Case 5: Check for viewport meta tag
   * Expected: Page includes responsive viewport meta tag
   */
  test('should have responsive viewport meta tag', () => {
    // Check for viewport meta tag with width=device-width
    const hasViewport = /<meta[^>]*name\s*=\s*["']viewport["'][^>]*>/i.test(htmlContent);
    expect(hasViewport).toBe(true);

    // Verify it contains width=device-width for responsive design
    const viewportMatch = htmlContent.match(/<meta[^>]*name\s*=\s*["']viewport["'][^>]*>/i);
    expect(viewportMatch).not.toBeNull();

    const viewportTag = viewportMatch![0];
    const hasDeviceWidth = /content\s*=\s*["'][^"']*width\s*=\s*device-width[^"']*["']/i.test(viewportTag);

    expect(hasDeviceWidth).toBe(true);
  });

  /**
   * Test Case 6: Verify use of semantic elements
   * Expected: Page uses header, main, section, article, footer appropriately
   */
  test('should use semantic HTML5 elements appropriately', () => {
    // Check for header element
    const hasHeader = /<header[\s>]/i.test(htmlContent);
    expect(hasHeader).toBe(true);

    // Check for main element
    const hasMain = /<main[\s>]/i.test(htmlContent);
    expect(hasMain).toBe(true);

    // Check for section elements
    const hasSections = /<section[\s>]/i.test(htmlContent);
    expect(hasSections).toBe(true);

    // Check for article elements (for feature cards)
    const hasArticles = /<article[\s>]/i.test(htmlContent);
    expect(hasArticles).toBe(true);

    // Check for footer element
    const hasFooter = /<footer[\s>]/i.test(htmlContent);
    expect(hasFooter).toBe(true);

    // Verify proper nesting: sections should be inside main
    const mainContent = htmlContent.match(/<main[^>]*>([\s\S]*?)<\/main>/i);
    expect(mainContent).not.toBeNull();

    const mainInnerContent = mainContent![1];
    const sectionsInMain = (mainInnerContent.match(/<section[\s>]/gi) || []).length;
    expect(sectionsInMain).toBeGreaterThanOrEqual(1);
  });

  /**
   * Test Case 7: Check for duplicate IDs
   * Expected: No duplicate ID attributes in HTML
   */
  test('should have no duplicate ID attributes', () => {
    // Extract all id attributes (but not data-testid, aria-labelledby, etc.)
    // Use word boundary to match only standalone 'id' attribute
    const idMatches = htmlContent.matchAll(/\sid\s*=\s*["']([^"']+)["']/gi);
    const ids: string[] = [];

    for (const match of idMatches) {
      ids.push(match[1].toLowerCase());
    }

    // Check for duplicates
    const uniqueIds = new Set(ids);
    const duplicates = ids.filter((id, index) => ids.indexOf(id) !== index);

    expect(duplicates).toHaveLength(0);
    expect(ids.length).toBe(uniqueIds.size);
  });
});

describe('CSS Validation', () => {
  /**
   * Test Case 2: Run W3C CSS validator on stylesheets
   * Expected: No CSS validation errors (warnings acceptable)
   */
  test('should pass CSS validation with no errors', () => {
    // Run stylelint for CSS validation
    try {
      execSync(`npx stylelint "css/**/*.css" --formatter json`, {
        cwd: rootDir,
        encoding: 'utf-8',
        stdio: ['pipe', 'pipe', 'pipe']
      });

      // If command succeeds with exit code 0, no errors
      expect(true).toBe(true);
    } catch (error: any) {
      if (error.stdout) {
        const validationResult = JSON.parse(error.stdout);

        // Count only errors, not warnings
        const errors = validationResult.reduce((total: number, file: any) => {
          return total + file.warnings.filter((w: any) => w.severity === 'error').length;
        }, 0);

        expect(errors).toBe(0);
      } else {
        throw error;
      }
    }
  });

  test('should have valid CSS file structure', () => {
    const cssFiles = [
      'main.css',
      'variables.css',
      'components.css',
      'themes.css',
      'responsive.css'
    ];

    for (const file of cssFiles) {
      const filePath = join(cssDir, file);
      expect(existsSync(filePath)).toBe(true);

      const content = readFileSync(filePath, 'utf-8');

      // Basic CSS syntax checks
      // Check for balanced braces
      const openBraces = (content.match(/{/g) || []).length;
      const closeBraces = (content.match(/}/g) || []).length;
      expect(openBraces).toBe(closeBraces);
    }
  });

  test('should use valid CSS custom properties', () => {
    const variablesContent = readFileSync(join(cssDir, 'variables.css'), 'utf-8');

    // Check that :root contains CSS custom properties
    const hasRoot = /:root\s*{/i.test(variablesContent);
    expect(hasRoot).toBe(true);

    // Check for color variables
    const hasColorVars = /--color-/i.test(variablesContent);
    expect(hasColorVars).toBe(true);

    // Check for typography variables
    const hasFontVars = /--font-/i.test(variablesContent);
    expect(hasFontVars).toBe(true);

    // Check for spacing variables
    const hasSpacingVars = /--spacing-/i.test(variablesContent);
    expect(hasSpacingVars).toBe(true);
  });

  test('should have valid color values', () => {
    const cssFiles = ['variables.css', 'themes.css'];

    for (const file of cssFiles) {
      const content = readFileSync(join(cssDir, file), 'utf-8');

      // Extract hex colors and validate format
      const hexColors = content.match(/#[0-9a-fA-F]{3,8}\b/g) || [];

      for (const color of hexColors) {
        // Valid hex: #RGB, #RRGGBB, or #RRGGBBAA
        const isValidHex = /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$/.test(color);
        expect(isValidHex).toBe(true);
      }
    }
  });
});

describe('Semantic Markup Quality', () => {
  test('should have proper heading hierarchy', () => {
    // Extract all headings in order
    const headings = htmlContent.matchAll(/<h([1-6])[^>]*>/gi);
    const headingLevels: number[] = [];

    for (const match of headings) {
      headingLevels.push(parseInt(match[1]));
    }

    // Should start with h1
    expect(headingLevels[0]).toBe(1);

    // Check that we don't skip heading levels (e.g., h1 -> h3)
    for (let i = 1; i < headingLevels.length; i++) {
      const diff = headingLevels[i] - headingLevels[i - 1];
      // Should not increase by more than 1 level at a time
      expect(diff).toBeLessThanOrEqual(1);
    }
  });

  test('should have accessible images with alt attributes', () => {
    // Find all img tags
    const imgTags = htmlContent.match(/<img[^>]*>/gi) || [];

    for (const img of imgTags) {
      // Every img should have an alt attribute
      const hasAlt = /alt\s*=\s*["'][^"']*["']/i.test(img);
      expect(hasAlt).toBe(true);
    }
  });

  test('should have meta charset declaration', () => {
    const hasCharset = /<meta[^>]*charset\s*=\s*["']utf-8["'][^>]*>/i.test(htmlContent) ||
                       /<meta[^>]*charset\s*=\s*["']UTF-8["'][^>]*>/i.test(htmlContent);
    expect(hasCharset).toBe(true);
  });

  test('should have a title element', () => {
    const hasTitle = /<title>[^<]+<\/title>/i.test(htmlContent);
    expect(hasTitle).toBe(true);

    // Title should not be empty
    const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
    expect(titleMatch).not.toBeNull();
    expect(titleMatch![1].trim().length).toBeGreaterThan(0);
  });

  test('should have nav element for navigation', () => {
    const hasNav = /<nav[\s>]/i.test(htmlContent);
    expect(hasNav).toBe(true);
  });

  test('should have figure and figcaption for images with captions', () => {
    // Check if figure is used (for demo section)
    const hasFigure = /<figure[\s>]/i.test(htmlContent);
    expect(hasFigure).toBe(true);

    // If figure exists, figcaption should be present
    if (hasFigure) {
      const hasFigcaption = /<figcaption[\s>]/i.test(htmlContent);
      expect(hasFigcaption).toBe(true);
    }
  });
});
