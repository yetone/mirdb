---
name: playwright-responsive-testing
description: Pattern for E2E testing responsive design behavior across multiple viewport sizes using Playwright
---

# Playwright Responsive Design Testing

Pattern for E2E testing responsive design behavior across multiple viewport sizes using Playwright.

## When to Use

Use this skill when:
- Testing responsive layouts across desktop, tablet, and mobile viewports
- Verifying CSS Grid/Flexbox column changes at breakpoints
- Testing hamburger menu visibility on mobile
- Checking for horizontal overflow issues
- Verifying touch target accessibility requirements (44px minimum)

## Trigger Phrases

- "test responsive design"
- "test mobile layout"
- "viewport testing"
- "test breakpoints"
- "test hamburger menu"
- "verify touch targets"
- "test grid columns"

## Quick Reference

```typescript
// Set viewport
await page.setViewportSize({ width: 375, height: 667 })

// Check horizontal overflow
const hasOverflow = await page.evaluate(() => {
  return document.body.scrollWidth > document.documentElement.clientWidth
})

// Verify grid columns
const gridStyle = await grid.evaluate((el) => {
  return window.getComputedStyle(el).gridTemplateColumns
})
const columnCount = gridStyle.split(' ').filter(Boolean).length

// Check touch target size
const box = await element.boundingBox()
expect(box!.width).toBeGreaterThanOrEqual(44)
expect(box!.height).toBeGreaterThanOrEqual(44)
```

See [README.md](references/README.md) for full documentation.
