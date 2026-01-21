# WCAG Contrast Testing

## Overview

Test color contrast ratios programmatically to verify WCAG accessibility compliance. This skill provides utility functions to calculate relative luminance and contrast ratios between colors, enabling automated accessibility testing in test suites.

## When to Use This Skill

Use this skill when:

- Testing that CTA buttons meet 4.5:1 contrast ratio (WCAG AA)
- Verifying text colors against backgrounds are accessible
- Auditing CSS color variables for accessibility
- Building automated accessibility testing into CI/CD

## Core Capabilities

### 1. Calculate Contrast Ratio

```javascript
/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - Hex color (e.g., '#ffffff')
 * @param {string} color2 - Hex color (e.g., '#000000')
 * @returns {number} - Contrast ratio (1 to 21)
 */
function calculateContrastRatio(color1, color2) {
  const lum1 = getLuminance(hexToRgb(color1));
  const lum2 = getLuminance(hexToRgb(color2));

  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);

  return (lighter + 0.05) / (darker + 0.05);
}
```

### 2. Convert Hex to RGB

```javascript
/**
 * Convert hex color to RGB object
 * @param {string} hex - Hex color string
 * @returns {object} - RGB object with r, g, b properties
 */
function hexToRgb(hex) {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result
    ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16),
      }
    : null;
}
```

### 3. Calculate Relative Luminance

```javascript
/**
 * Calculate relative luminance of an RGB color
 * Per WCAG 2.1 specification
 * @param {object} rgb - RGB object with r, g, b properties
 * @returns {number} - Relative luminance value (0 to 1)
 */
function getLuminance(rgb) {
  const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(val => {
    const sRGB = val / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });

  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}
```

## Usage in Tests

```javascript
import { describe, it, expect } from 'vitest';

describe('CTA Button Accessibility', () => {
  it('should have sufficient color contrast (4.5:1 minimum)', () => {
    const buttonBackground = '#2563eb'; // Blue
    const buttonText = '#ffffff';       // White

    const contrastRatio = calculateContrastRatio(buttonBackground, buttonText);

    // WCAG AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  it('should meet large text requirements (3:1 minimum)', () => {
    const headingBackground = '#f9fafb';
    const headingText = '#1f2937';

    const contrastRatio = calculateContrastRatio(headingBackground, headingText);

    // WCAG AA requires 3:1 for large text (18pt or 14pt bold)
    expect(contrastRatio).toBeGreaterThanOrEqual(3);
  });
});
```

## WCAG Requirements Reference

| Level | Normal Text | Large Text |
|-------|-------------|------------|
| AA | 4.5:1 | 3:1 |
| AAA | 7:1 | 4.5:1 |

**Large text**: 18pt (24px) or 14pt (18.67px) bold

## Common Color Combinations

| Background | Text | Ratio | AA? |
|------------|------|-------|-----|
| #2563eb | #ffffff | 5.3:1 | ✓ |
| #1f2937 | #ffffff | 14.3:1 | ✓ |
| #f9fafb | #1f2937 | 13.5:1 | ✓ |
| #4b5563 | #ffffff | 6.0:1 | ✓ |

## Best Practices

- Always test CTA buttons for 4.5:1 minimum contrast
- Extract CSS variable values for programmatic testing
- Include contrast tests in automated test suite
- Test hover/focus states as well as default states
- Consider users with low vision (aim for AAA when possible)

## Resources

- [WCAG 2.1 Contrast Requirements](https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html)
- [WebAIM Contrast Checker](https://webaim.org/resources/contrastchecker/)
- [WCAG Relative Luminance Definition](https://www.w3.org/TR/WCAG21/#dfn-relative-luminance)
