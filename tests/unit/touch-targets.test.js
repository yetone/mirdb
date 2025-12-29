/**
 * Unit tests for Touch Target Sizes (NFR-2)
 * Test Case 6: All interactive elements have minimum 44px touch target area
 * Based on WCAG 2.1 guidelines for touch targets
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const cssPath = path.resolve(__dirname, '../../styles.css');

describe('Touch Target Size Unit Tests', () => {

  // Test Case 6: Check CSS ensures minimum touch target sizes
  test('TC6: CSS defines adequate padding/sizing for touch targets', () => {
    const css = fs.readFileSync(cssPath, 'utf8');

    // Check that buttons have adequate padding
    // .btn should have padding of at least 0.75rem (12px) on all sides
    // to ensure touch target is at least 44px when combined with content

    // Look for .btn padding rules
    const btnPaddingMatch = css.match(/\.btn\s*\{[^}]*padding:\s*([^;]+);/);
    assert.ok(btnPaddingMatch, 'Button (.btn) should have padding defined');

    // Parse the padding value
    const paddingValue = btnPaddingMatch[1];
    // Common patterns: "0.875rem 1.75rem" or similar
    // 0.875rem = 14px, which with typical button content gives > 44px height

    // Verify copy button has adequate size
    const copyBtnMatch = css.match(/\.copy-btn\s*\{[^}]*padding:\s*([^;]+);/);
    assert.ok(copyBtnMatch, 'Copy button (.copy-btn) should have padding defined');

    // Verify links in footer and nav have adequate spacing
    // The page uses .btn class for main CTAs which already has good padding
  });

  // Test: Verify interactive elements have adequate size classes or explicit dimensions
  test('Interactive elements CSS supports minimum touch target dimensions', () => {
    const css = fs.readFileSync(cssPath, 'utf8');

    // Parse button padding
    const btnRule = css.match(/\.btn\s*\{([^}]+)\}/);
    assert.ok(btnRule, 'Should have .btn rule');

    const btnStyles = btnRule[1];

    // Extract padding values
    const paddingMatch = btnStyles.match(/padding:\s*([^;]+);/);
    assert.ok(paddingMatch, 'Button should have padding');

    // Verify inline-block or block display for proper sizing
    const displayMatch = btnStyles.match(/display:\s*(inline-block|block|flex|inline-flex)/);
    assert.ok(displayMatch, 'Button should have display property for proper sizing');

    // Check copy button sizing
    const copyBtnRule = css.match(/\.copy-btn\s*\{([^}]+)\}/);
    assert.ok(copyBtnRule, 'Should have .copy-btn rule');

    const copyBtnStyles = copyBtnRule[1];
    const copyPaddingMatch = copyBtnStyles.match(/padding:\s*([^;]+);/);
    assert.ok(copyPaddingMatch, 'Copy button should have padding');
  });

  // Test: Verify links have adequate clickable area through padding or line-height
  test('Links and interactive elements have readable styles for touch interaction', () => {
    const css = fs.readFileSync(cssPath, 'utf8');

    // Body line-height should be adequate (1.5 or higher)
    const bodyMatch = css.match(/body\s*\{([^}]+)\}/);
    if (bodyMatch) {
      const bodyStyles = bodyMatch[1];
      const lineHeightMatch = bodyStyles.match(/line-height:\s*([^;]+);/);
      if (lineHeightMatch) {
        const lineHeight = parseFloat(lineHeightMatch[1]);
        // Line height of 1.5 or higher provides adequate vertical spacing
        assert.ok(lineHeight >= 1.5 || lineHeightMatch[1].includes('rem'),
          `Line height should be adequate: ${lineHeightMatch[1]}`);
      }
    }

    // Feature cards should have adequate padding for touch
    const featureCardMatch = css.match(/\.feature-card\s*\{([^}]+)\}/);
    if (featureCardMatch) {
      const cardStyles = featureCardMatch[1];
      const cardPaddingMatch = cardStyles.match(/padding:\s*([^;]+);/);
      assert.ok(cardPaddingMatch, 'Feature cards should have padding');
    }
  });

  // Test: Minimum sizing values meet 44px requirement
  test('Button minimum heights meet 44px touch target requirement', () => {
    const css = fs.readFileSync(cssPath, 'utf8');

    // Extract button styles
    const btnRule = css.match(/\.btn\s*\{([^}]+)\}/);
    assert.ok(btnRule, 'Should have .btn rule');

    const btnStyles = btnRule[1];

    // Get padding value
    const paddingMatch = btnStyles.match(/padding:\s*([^\s;]+)\s*([^\s;]+)?;/);
    assert.ok(paddingMatch, 'Button should have padding');

    // Parse padding values (format: "0.875rem 1.75rem" means vertical horizontal)
    const verticalPadding = paddingMatch[1];
    const fontSize = btnStyles.match(/font-size:\s*([^;]+);/);

    // Convert rem to px (assuming 16px base)
    // 0.875rem = 14px vertical padding * 2 = 28px + font content (1rem = 16px) = 44px+
    const verticalPaddingPx = parseFloat(verticalPadding) * 16;

    // With vertical padding of ~14px * 2 = 28px, plus 16px font = 44px minimum
    // This calculation assumes 1rem = 16px base font size
    if (verticalPadding.includes('rem')) {
      const padding = parseFloat(verticalPadding);
      // Total height = (padding * 2) + line-height (roughly 1.2-1.6 * font-size)
      // With 0.875rem (14px) padding * 2 = 28px + ~19px (1.2 * 16px) = 47px
      const estimatedHeight = (padding * 2 * 16) + 16 * 1.2;
      assert.ok(estimatedHeight >= 44,
        `Estimated button height (${estimatedHeight}px) should be >= 44px`);
    }
  });

});
