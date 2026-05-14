# CSS Contrast Ratio Testing

## Overview

Implements the WCAG 2.1 relative luminance and contrast ratio formulas for programmatic color contrast validation in test suites. Used to verify CSS custom properties meet AA/AAA thresholds without requiring heavyweight tools like axe-core.

## WCAG 2.1 Formulas

### sRGB to Linear RGB

```typescript
function relativeLuminance([r, g, b]: [number, number, number]): number {
  const srgb = [r, g, b].map((c) => {
    const s = c / 255;
    return s <= 0.04045 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * srgb[0] + 0.7152 * srgb[1] + 0.0722 * srgb[2];
}
```

### Contrast Ratio

```typescript
function contrastRatio(hex1: string, hex2: string): number {
  const rgb1 = hexToRgb(hex1);
  const rgb2 = hexToRgb(hex2);
  if (!rgb1 || !rgb2) return 0;
  const l1 = relativeLuminance(rgb1);
  const l2 = relativeLuminance(rgb2);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}
```

### Extracting CSS Variables

```typescript
function extractCssVar(stylesheet: string, varName: string): string | null {
  const regex = new RegExp(`${varName.replace(/-/g, '\\-')}\\s*:\\s*([^;]+);`);
  const match = stylesheet.match(regex);
  return match ? match[1].trim() : null;
}
```

## WCAG Thresholds

| Level | Normal Text | Large Text |
|-------|------------|------------|
| AA    | 4.5:1      | 3:1        |
| AAA   | 7:1        | 4.5:1      |

## Example: Validating Theme Colors

```typescript
it('primary text color has at least 4.5:1 contrast against background', () => {
  const textColor = extractCssVar(themeCss, '--color-text');
  const bgColor = extractCssVar(themeCss, '--color-bg');
  if (textColor && bgColor) {
    const ratio = contrastRatio(textColor, bgColor);
    expect(ratio).toBeGreaterThanOrEqual(4.5);
  }
});
```

## Reference

- [WCAG 2.1 Relative Luminance Definition](https://www.w3.org/TR/WCAG21/#dfn-relative-luminance)
- Coefficients: R=0.2126, G=0.7152, B=0.0722
- sRGB threshold: 0.04045 (linear below this)
