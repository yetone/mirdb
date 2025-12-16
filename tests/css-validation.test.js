/**
 * CSS Validation and Maintainability Tests
 *
 * These tests verify:
 * - CSS validity (basic validation checks)
 * - CSS custom properties usage for theming
 * - Viewport meta tag presence
 * - External stylesheet usage (no inline styles)
 */
import { describe, it, expect, beforeAll } from 'vitest';
import fs from 'fs';
import path from 'path';

let cssContent;
let htmlContent;
const rootDir = process.cwd();

beforeAll(() => {
  cssContent = fs.readFileSync(path.join(rootDir, 'styles.css'), 'utf-8');
  htmlContent = fs.readFileSync(path.join(rootDir, 'index.html'), 'utf-8');
});

describe('CSS Validation', () => {
  it('should have valid CSS syntax with no unclosed brackets', () => {
    // Count opening and closing braces
    const openBraces = (cssContent.match(/\{/g) || []).length;
    const closeBraces = (cssContent.match(/\}/g) || []).length;
    expect(openBraces).toBe(closeBraces);
  });

  it('should have valid CSS syntax with no unclosed parentheses', () => {
    // Count opening and closing parentheses
    const openParens = (cssContent.match(/\(/g) || []).length;
    const closeParens = (cssContent.match(/\)/g) || []).length;
    expect(openParens).toBe(closeParens);
  });

  it('should not have empty CSS rules', () => {
    // Look for patterns like: selector { }
    const emptyRules = cssContent.match(/\{[\s]*\}/g);
    expect(emptyRules).toBeNull();
  });

  it('should have valid property-value pairs with colons', () => {
    // Extract content within braces and check for proper property: value format
    const ruleBlocks = cssContent.match(/\{[^}]+\}/g) || [];
    ruleBlocks.forEach(block => {
      const content = block.slice(1, -1).trim();
      if (content) {
        // Each non-empty line in a block should contain a colon (for property: value)
        const lines = content.split(';').filter(l => l.trim());
        lines.forEach(line => {
          const trimmedLine = line.trim();
          // Skip comments and empty lines
          if (trimmedLine && !trimmedLine.startsWith('/*') && !trimmedLine.startsWith('//')) {
            expect(trimmedLine).toContain(':');
          }
        });
      }
    });
  });

  it('should have CSS variables properly formatted', () => {
    // Check that CSS variable definitions use -- prefix
    const varDefinitions = cssContent.match(/--[a-zA-Z0-9-]+\s*:/g);
    expect(varDefinitions).toBeTruthy();
    expect(varDefinitions.length).toBeGreaterThan(0);
  });

  it('should have CSS variable references using var() syntax', () => {
    // Check that var() is used to reference variables
    const varReferences = cssContent.match(/var\(--[a-zA-Z0-9-]+\)/g);
    expect(varReferences).toBeTruthy();
    expect(varReferences.length).toBeGreaterThan(0);
  });
});

describe('CSS Custom Properties for Theming', () => {
  it('should have :root selector with CSS custom properties', () => {
    expect(cssContent).toContain(':root');
    // Check for :root block with variables
    const rootBlockMatch = cssContent.match(/:root\s*\{[^}]+\}/);
    expect(rootBlockMatch).toBeTruthy();
  });

  it('should define color variables in :root', () => {
    const rootBlock = cssContent.match(/:root\s*\{([^}]+)\}/);
    expect(rootBlock).toBeTruthy();

    const rootContent = rootBlock[1];
    // Check for color-related variables
    expect(rootContent).toMatch(/--.*color.*:/i);
  });

  it('should define primary color variable', () => {
    expect(cssContent).toMatch(/--primary-color\s*:/);
  });

  it('should define background color variable', () => {
    expect(cssContent).toMatch(/--background-color\s*:/);
  });

  it('should define text color variables', () => {
    expect(cssContent).toMatch(/--text-(primary|secondary)\s*:/);
  });

  it('should use CSS variables for colors throughout the stylesheet', () => {
    // Count var() usages for colors
    const colorVarUsages = cssContent.match(/var\(--.*color\)/gi) || [];
    expect(colorVarUsages.length).toBeGreaterThan(5);
  });

  it('should define spacing or sizing variables if applicable', () => {
    const rootBlock = cssContent.match(/:root\s*\{([^}]+)\}/);
    expect(rootBlock).toBeTruthy();

    // Check for font-related variables (common maintainability pattern)
    const hasTypographyVars = /--font/.test(cssContent);
    // Check for surface/border colors (theming pattern)
    const hasThemedVars = /--surface|--border/.test(cssContent);

    // Should have at least some theming variables beyond basic colors
    expect(hasTypographyVars || hasThemedVars).toBe(true);
  });
});

describe('Viewport Meta Tag', () => {
  it('should have viewport meta tag', () => {
    expect(htmlContent).toMatch(/<meta[^>]*name=["']viewport["'][^>]*>/i);
  });

  it('should have width=device-width in viewport meta tag', () => {
    const viewportMeta = htmlContent.match(/<meta[^>]*name=["']viewport["'][^>]*>/i);
    expect(viewportMeta).toBeTruthy();
    expect(viewportMeta[0]).toContain('width=device-width');
  });

  it('should have initial-scale set in viewport meta tag', () => {
    const viewportMeta = htmlContent.match(/<meta[^>]*name=["']viewport["'][^>]*>/i);
    expect(viewportMeta).toBeTruthy();
    expect(viewportMeta[0]).toContain('initial-scale');
  });
});

describe('External Stylesheet Usage', () => {
  it('should link to external stylesheet', () => {
    expect(htmlContent).toMatch(/<link[^>]*rel=["']stylesheet["'][^>]*>/i);
  });

  it('should link to styles.css file', () => {
    expect(htmlContent).toMatch(/<link[^>]*href=["']styles\.css["'][^>]*>/i);
  });

  it('should not have style tags with substantial inline CSS', () => {
    const styleTags = htmlContent.match(/<style[^>]*>[\s\S]*?<\/style>/gi) || [];
    // Allow empty or minimal style tags, but not substantial inline CSS
    styleTags.forEach(tag => {
      const content = tag.replace(/<\/?style[^>]*>/gi, '').trim();
      // If there's inline CSS, it should be minimal (less than 500 chars)
      expect(content.length).toBeLessThan(500);
    });
  });

  it('should not have excessive inline style attributes', () => {
    // Count style="" attributes in HTML
    const inlineStyles = htmlContent.match(/\sstyle=["'][^"']+["']/gi) || [];
    // Allow some inline styles (e.g., for SVG), but not excessive
    expect(inlineStyles.length).toBeLessThan(20);
  });

  it('should have styles.css file exist and be non-empty', () => {
    expect(cssContent.length).toBeGreaterThan(100);
  });
});

describe('CSS Organization', () => {
  it('should have CSS reset or normalize section', () => {
    // Check for box-sizing reset or common reset patterns
    expect(cssContent).toMatch(/box-sizing\s*:\s*border-box/);
  });

  it('should have CSS organized with comments or sections', () => {
    // Check for CSS comments indicating organization
    const comments = cssContent.match(/\/\*[^*]*\*+(?:[^/*][^*]*\*+)*\//g) || [];
    expect(comments.length).toBeGreaterThan(0);
  });

  it('should have responsive media queries', () => {
    expect(cssContent).toMatch(/@media/);
  });

  it('should have media queries for mobile breakpoints', () => {
    // Check for common mobile breakpoint (768px or similar)
    expect(cssContent).toMatch(/@media\s*\([^)]*max-width\s*:\s*\d+px[^)]*\)/);
  });

  it('should define body styles using CSS variables', () => {
    const bodyRule = cssContent.match(/body\s*\{[^}]+\}/);
    expect(bodyRule).toBeTruthy();
    expect(bodyRule[0]).toMatch(/var\(--/);
  });
});
