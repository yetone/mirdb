# E2E Color Brightness Testing

## Overview

When testing dark mode or any color-dependent UI feature with Playwright, `getComputedStyle` returns colors in different formats depending on the browser and CSS engine. This skill provides a robust `getBrightness()` helper that normalizes any format to a 0-255 brightness scale.

## When to Use This Skill

Use this skill when users request:
- Testing CSS colors in E2E tests
- Handling oklch/oklab color formats from getComputedStyle
- Writing assertions for dark/light mode color verification
- Parsing multiple CSS color formats in tests

## Core Capabilities

### 1. Multi-Format Color Brightness Extraction

```javascript
function getBrightness(colorStr) {
  if (!colorStr) return null;

  // rgb/rgba: rgb(255, 255, 255) or rgba(255, 255, 255, 0.5)
  const rgbMatch = colorStr.match(/rgba?\((\d+(?:\.\d+)?),\s*(\d+(?:\.\d+)?),\s*(\d+(?:\.\d+)?)/);
  if (rgbMatch) {
    const r = parseFloat(rgbMatch[1]);
    const g = parseFloat(rgbMatch[2]);
    const b = parseFloat(rgbMatch[3]);
    return (r + g + b) / 3;
  }

  // oklch: oklch(0.278078 0.029596 256.848) — first value is lightness 0-1
  const oklchMatch = colorStr.match(/oklch\(([\d.]+)/);
  if (oklchMatch) {
    return parseFloat(oklchMatch[1]) * 255;
  }

  // oklab: oklab(0.419385 0.00478227 -0.0474926) — first value is lightness 0-1
  const oklabMatch = colorStr.match(/oklab\(([\d.]+)/);
  if (oklabMatch) {
    return parseFloat(oklabMatch[1]) * 255;
  }

  // hsl: hsl(210, 100%, 50%)
  const hslMatch = colorStr.match(/hsl\([\d.]+,\s*[\d.]+%?,\s*([\d.]+)%?\)/);
  if (hslMatch) {
    return (parseFloat(hslMatch[1]) / 100) * 255;
  }

  return null;
}
```

### 2. Usage in Playwright Tests

```javascript
const bodyBg = await body.evaluate(el => {
  const computed = window.getComputedStyle(el);
  return computed.backgroundColor;
});

const brightness = getBrightness(bodyBg);
expect(brightness).not.toBeNull();
expect(brightness).toBeGreaterThan(200); // Light background
```

### 3. Handling Playwright Strict Mode with Multiple Elements

When multiple elements share the same `data-testid`, use the `:visible` pseudo-class:

```javascript
// Selects only the visible toggle based on viewport
const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
```

## Best Practices

- Always normalize colors to brightness rather than comparing raw strings
- Include parsers for oklch and oklab — modern Chromium returns these formats
- Use empirically-determined thresholds based on your design system
- Use `:visible` pseudo-class for elements that exist in both desktop and mobile layouts

## Recommended Brightness Thresholds (DaisyUI)

| Context | Threshold |
|---------|-----------|
| Light background | > 200 |
| Dark background | < 100 |
| Dark text (light mode) | < 120 |
| Light text (dark mode) | > 180 |

## Resources

### references/

- `README.md` - This documentation

### Related Files

- `tests/e2e/theme.spec.js` - Full implementation with getBrightness() helper
