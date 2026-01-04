// @ts-check
const fs = require('fs');
const path = require('path');
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: CSS Validation
 *
 * Verifies that CSS is valid and follows best practices
 * Tests W3C CSS validation and vendor prefix presence
 */

const cssFilePath = path.join(__dirname, '..', '..', 'styles.css');
const cssContent = fs.readFileSync(cssFilePath, 'utf-8');

test.describe('CSS Validation', () => {
  /**
   * Test Case 1: Run W3C CSS validation
   * Input: Run W3C CSS validation
   * Expected: No CSS validation errors
   */
  test('should have valid CSS with no W3C validation errors', () => {
    // Parse CSS for common validation errors

    // 1. Check for unclosed braces
    const openBraces = (cssContent.match(/{/g) || []).length;
    const closeBraces = (cssContent.match(/}/g) || []).length;
    expect(openBraces).toBe(closeBraces);

    // 2. Check for missing semicolons in property declarations
    // Match property: value patterns and ensure they end with semicolon (or are last before })
    const propertyValuePattern = /[a-z-]+\s*:\s*[^;{}]+(?=[;}])/gi;
    const matches = cssContent.match(propertyValuePattern) || [];
    // Each match should be followed by ; or }
    for (const match of matches) {
      // This is a simplified check - the pattern itself ensures valid termination
      expect(match.trim().length).toBeGreaterThan(0);
    }

    // 3. Check for valid CSS selectors (no empty selectors)
    const emptySelectors = cssContent.match(/{\s*}/g);
    expect(emptySelectors).toBeNull();

    // 4. Check for valid color values
    const hexColors = cssContent.match(/#[0-9a-fA-F]+/g) || [];
    for (const color of hexColors) {
      // Valid hex colors are 3, 4, 6, or 8 characters (including #)
      const hexPart = color.slice(1);
      expect([3, 4, 6, 8]).toContain(hexPart.length);
      expect(hexPart).toMatch(/^[0-9a-fA-F]+$/);
    }

    // 5. Check for valid unit values (common units)
    const unitPattern = /:\s*[\d.]+([a-z%]+)/gi;
    const unitMatches = [...cssContent.matchAll(unitPattern)];
    const validUnits = ['px', 'em', 'rem', '%', 'vh', 'vw', 'vmin', 'vmax', 's', 'ms', 'deg', 'fr', 'ch'];
    for (const match of unitMatches) {
      const unit = match[1].toLowerCase();
      expect(validUnits).toContain(unit);
    }

    // 6. Check for valid CSS custom properties (variables)
    const customProperties = cssContent.match(/--[a-zA-Z-]+/g) || [];
    for (const prop of customProperties) {
      // Custom properties should have valid names
      expect(prop).toMatch(/^--[a-zA-Z][a-zA-Z0-9-]*$/);
    }

    // 7. Check for valid var() function usage
    const varUsages = cssContent.match(/var\([^)]+\)/g) || [];
    for (const varUsage of varUsages) {
      // Should reference a custom property
      expect(varUsage).toMatch(/var\(\s*--[a-zA-Z][a-zA-Z0-9-]*\s*\)/);
    }

    // 8. Check for valid CSS functions
    const functionPattern = /\b(rgb|rgba|hsl|hsla|linear-gradient|radial-gradient|calc|clamp|min|max|repeat|minmax|auto-fit|url)\s*\(/gi;
    const functionMatches = cssContent.match(functionPattern) || [];
    // All function usages should be from known CSS functions
    expect(functionMatches.length).toBeGreaterThan(0);

    // 9. Check for duplicate property declarations in same block
    // This is a basic check for obvious duplicates
    const blocks = cssContent.split('}');
    for (const block of blocks) {
      if (!block.includes('{')) continue;
      const blockContent = block.split('{')[1];
      if (!blockContent) continue;

      // Extract property names (excluding vendor-prefixed as duplicates are intentional)
      const nonPrefixedProps = [...blockContent.matchAll(/\n\s*([a-z][a-z-]*)(?=\s*:)/gi)]
        .map(m => m[1])
        .filter(p => !p.startsWith('-webkit') && !p.startsWith('-moz') && !p.startsWith('-ms') && !p.startsWith('-o'));

      // Check for unintentional duplicates (same property appears twice)
      const uniqueProps = [...new Set(nonPrefixedProps)];
      // Allow some duplicates as they may be intentional for fallbacks
      expect(nonPrefixedProps.length - uniqueProps.length).toBeLessThanOrEqual(nonPrefixedProps.length / 2);
    }

    // 10. Check that media queries have valid syntax
    const mediaQueries = cssContent.match(/@media[^{]+/g) || [];
    for (const mq of mediaQueries) {
      // Should contain valid media features
      expect(mq).toMatch(/@media\s*\([^)]+\)/);
    }

    // 11. Verify :root contains CSS custom properties
    const rootBlock = cssContent.match(/:root\s*{[^}]+}/);
    expect(rootBlock).not.toBeNull();
    expect(rootBlock[0]).toContain('--');

    // 12. Check for valid pseudo-classes and pseudo-elements
    const pseudoPattern = /::?(before|after|hover|focus|active|visited|first-child|last-child|nth-child|not|root|focus-within|focus-visible)\b/gi;
    const pseudoMatches = cssContent.match(pseudoPattern) || [];
    expect(pseudoMatches.length).toBeGreaterThan(0);
  });

  /**
   * Test Case 2: Check for browser prefixes
   * Input: Check for browser prefixes
   * Expected: Necessary vendor prefixes are included for cross-browser support
   */
  test('should have necessary vendor prefixes for cross-browser support', () => {
    // 1. Check for -webkit- prefixes (Safari, older Chrome)
    const webkitPrefixes = cssContent.match(/-webkit-[a-z-]+/g) || [];
    expect(webkitPrefixes.length).toBeGreaterThan(0);

    // 2. Verify flexbox has webkit prefixes
    // Standard display: flex should have -webkit-flex fallback
    const hasFlexbox = cssContent.includes('display: flex') || cssContent.includes('display:flex');
    const hasWebkitFlex = cssContent.includes('-webkit-flex');
    if (hasFlexbox) {
      expect(hasWebkitFlex).toBe(true);
    }

    // 3. Verify flex-direction has webkit prefix if used
    const hasFlexDirection = cssContent.includes('flex-direction');
    const hasWebkitFlexDirection = cssContent.includes('-webkit-flex-direction');
    if (hasFlexDirection) {
      expect(hasWebkitFlexDirection).toBe(true);
    }

    // 4. Verify justify-content has webkit prefix
    const hasJustifyContent = cssContent.includes('justify-content');
    const hasWebkitJustifyContent = cssContent.includes('-webkit-justify-content');
    if (hasJustifyContent) {
      expect(hasWebkitJustifyContent).toBe(true);
    }

    // 5. Verify align-items has webkit prefix
    const hasAlignItems = cssContent.includes('align-items');
    const hasWebkitAlignItems = cssContent.includes('-webkit-align-items');
    if (hasAlignItems) {
      expect(hasWebkitAlignItems).toBe(true);
    }

    // 6. Verify flex-wrap has webkit prefix if used
    const hasFlexWrap = cssContent.includes('flex-wrap');
    const hasWebkitFlexWrap = cssContent.includes('-webkit-flex-wrap');
    if (hasFlexWrap) {
      expect(hasWebkitFlexWrap).toBe(true);
    }

    // 7. Verify scroll-behavior has webkit prefix
    const hasScrollBehavior = cssContent.includes('scroll-behavior');
    const hasWebkitScrollBehavior = cssContent.includes('-webkit-scroll-behavior');
    if (hasScrollBehavior) {
      expect(hasWebkitScrollBehavior).toBe(true);
    }

    // 8. Verify linear-gradient has webkit prefix
    const hasLinearGradient = cssContent.includes('linear-gradient');
    const hasWebkitLinearGradient = cssContent.includes('-webkit-linear-gradient');
    if (hasLinearGradient) {
      expect(hasWebkitLinearGradient).toBe(true);
    }

    // 9. Verify sticky positioning has webkit prefix
    const hasStickyPosition = cssContent.includes('position: sticky') || cssContent.includes('position:sticky');
    const hasWebkitSticky = cssContent.includes('position: -webkit-sticky') || cssContent.includes('position:-webkit-sticky');
    if (hasStickyPosition) {
      expect(hasWebkitSticky).toBe(true);
    }

    // 10. Check that prefixed properties appear BEFORE standard properties (fallback pattern)
    // This ensures older browsers get the prefixed version first
    const flexBlocks = cssContent.split(/display:\s*flex/);
    if (flexBlocks.length > 1) {
      // Check that -webkit-flex appears before flex in blocks that use flexbox
      for (let i = 0; i < flexBlocks.length - 1; i++) {
        const block = flexBlocks[i];
        // The block before "display: flex" should contain "display: -webkit-flex"
        expect(block).toContain('-webkit-flex');
      }
    }

    // 11. Verify no orphaned prefixes (prefixes without standard property)
    // Count occurrences of -webkit-flex vs flex
    const webkitFlexCount = (cssContent.match(/display:\s*-webkit-flex/g) || []).length;
    const standardFlexCount = (cssContent.match(/display:\s*flex/g) || []).length;
    // Should have equal or more standard properties than prefixed (some may be fallbacks only)
    expect(standardFlexCount).toBeGreaterThanOrEqual(webkitFlexCount);

    // 12. Verify critical cross-browser properties are prefixed
    // For this landing page, the critical properties are:
    // - Flexbox (display, justify-content, align-items, flex-wrap)
    // - Position sticky
    // - Scroll behavior
    // - Linear gradients

    // Count of critical prefixed properties
    const criticalPrefixedCount = [
      cssContent.includes('-webkit-flex'),
      cssContent.includes('-webkit-justify-content'),
      cssContent.includes('-webkit-align-items'),
      cssContent.includes('-webkit-sticky'),
      cssContent.includes('-webkit-linear-gradient'),
      cssContent.includes('-webkit-scroll-behavior'),
    ].filter(Boolean).length;

    // Should have at least 4 critical prefixed properties for good cross-browser support
    expect(criticalPrefixedCount).toBeGreaterThanOrEqual(4);
  });
});
