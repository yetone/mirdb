# Playwright Responsive Testing

## Overview

This skill enables comprehensive responsive design verification using Playwright E2E tests. It validates layout behavior across viewports from 320px mobile to 2560px ultrawide, including grid column counts, navigation state, tap target sizes, horizontal scroll detection, and orientation changes.

## When to Use This Skill

Use this skill when users request:

- Adding responsive design tests to a homepage or web app
- Verifying CSS grid/flexbox layouts across breakpoints
- Testing navigation collapse/expand at viewport thresholds
- Ensuring tap targets meet WCAG accessibility standards on mobile

## Core Capabilities

### 1. Viewport Matrix Testing

Define a set of viewports and run assertions against each:

```js
const VIEWPORTS = {
  mobileSE: { width: 320, height: 568 },
  mobile8: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  smallDesktop: { width: 1024, height: 768 },
  standardDesktop: { width: 1920, height: 1080 },
  ultrawide: { width: 2560, height: 1440 },
};
```

Use `beforeEach` to set the viewport and reload the page so media queries re-evaluate:

```js
beforeEach(async () => {
  await page.setViewportSize(VIEWPORTS.tablet);
  await page.reload();
});
```

### 2. Grid Column Detection

Browsers resolve `grid-template-columns: 1fr` to pixel values in computed styles. Use a helper to count columns:

```js
function countGridColumns(gridTemplateColumns) {
  if (!gridTemplateColumns || gridTemplateColumns === 'none') return 0;
  return gridTemplateColumns.split(/\s+/).filter(s => s && s !== '0px').length;
}

// Usage:
const gridComputed = await page.evaluate(() => {
  const el = document.querySelector('.features-grid');
  return el ? window.getComputedStyle(el).gridTemplateColumns : '';
});
expect(countGridColumns(gridComputed)).toBe(4);
```

### 3. Visibility via boundingBox

Playwright's Jest integration lacks `toBeHidden()`. Use `boundingBox()` instead:

```js
const box = await page.locator('.mobile-menu-toggle').boundingBox();
const isHidden = !box || box.width === 0 || box.height === 0;
expect(isHidden).toBe(true);
```

Hidden elements return `null` from `boundingBox()`. Elements in the layout with zero dimensions return `{x, y, width: 0, height: 0}`.

### 4. Tap Target Validation

Iterate all interactive elements and assert minimum size:

```js
const tapTargets = await page.locator('button, a, .btn').all();
const failures = [];
for (const target of tapTargets) {
  const box = await target.boundingBox();
  if (box && box.width > 0 && box.height > 0) {
    if (box.width < 44 || box.height < 44) {
      failures.push(`${box.width}x${box.height}`);
    }
  }
}
expect(failures).toHaveLength(0);
```

### 5. Horizontal Scroll Detection

```js
const overflow = await page.evaluate(() => {
  return document.documentElement.scrollWidth > window.innerWidth;
});
expect(overflow).toBe(false);
```

### 6. Orientation Change Testing

Change viewport dimensions mid-test and verify layout adaptation:

```js
await page.setViewportSize({ width: 375, height: 667 });
await page.reload();
// ... portrait assertions ...
await page.setViewportSize({ width: 667, height: 375 });
await page.waitForTimeout(500);
// ... landscape assertions ...
```

## Best Practices

- Always call `page.reload()` after `setViewportSize()` so media queries re-evaluate correctly
- Use `page.evaluate()` for computed style checks; `page.locator().evaluate()` works on specific elements
- Batch related assertions in the same `it()` block to minimize browser cycles
- Use `file://` URLs for testing static HTML files without a dev server

## Resources

### references/

- `README.md` - This documentation
