# Playwright Responsive Design Testing

## Overview

This skill provides patterns for comprehensive E2E testing of responsive web designs using Playwright. It covers viewport manipulation, CSS Grid verification, touch target accessibility compliance, and horizontal overflow detection.

## When to Use This Skill

Use this skill when users request:

- Testing responsive layouts across multiple device sizes
- Verifying CSS breakpoints work correctly
- Testing mobile navigation (hamburger menu)
- Checking WCAG accessibility touch target requirements
- Ensuring no horizontal scrolling issues on mobile

## Core Capabilities

### 1. Viewport Manipulation

Set viewport sizes to test different device categories:

```typescript
// Standard viewport sizes
const viewports = {
  desktop: { width: 1920, height: 1080 },
  laptop: { width: 1024, height: 768 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 667 },
  smallMobile: { width: 320, height: 568 },
}

// In test
await page.setViewportSize(viewports.mobile)
```

### 2. CSS Grid Column Verification

Verify responsive CSS Grid adapts columns correctly:

```typescript
const grid = page.locator('[data-testid="features-grid"]')
const gridStyle = await grid.evaluate((el) => {
  const style = window.getComputedStyle(el)
  return style.gridTemplateColumns
})
// gridTemplateColumns returns values like "300px 300px 300px" for 3 columns
const columnCount = gridStyle.split(' ').filter(Boolean).length
expect(columnCount).toBe(3) // Desktop
expect(columnCount).toBe(2) // Tablet
expect(columnCount).toBe(1) // Mobile
```

### 3. Horizontal Overflow Detection

Check for unwanted horizontal scrolling:

```typescript
async function hasHorizontalOverflow(page: Page): Promise<boolean> {
  return page.evaluate(() => {
    const body = document.body
    const html = document.documentElement
    return body.scrollWidth > html.clientWidth
  })
}

// In test
const hasOverflow = await hasHorizontalOverflow(page)
expect(hasOverflow).toBe(false)
```

### 4. Touch Target Accessibility (WCAG 2.1)

Verify minimum 44px touch targets for accessibility:

```typescript
const button = page.locator('[data-testid="hamburger-button"]')
const box = await button.boundingBox()
expect(box).not.toBeNull()
expect(box!.width).toBeGreaterThanOrEqual(44)
expect(box!.height).toBeGreaterThanOrEqual(44)
```

### 5. Navigation Visibility Testing

Test hamburger menu appears/disappears at breakpoints:

```typescript
// Mobile - hamburger visible
await page.setViewportSize({ width: 375, height: 667 })
const hamburger = page.locator('[data-testid="hamburger-button"]')
await expect(hamburger).toBeVisible()

// Desktop - hamburger hidden
await page.setViewportSize({ width: 1920, height: 1080 })
await expect(hamburger).not.toBeVisible()
```

### 6. Flex Direction Verification

Check buttons stack vertically on mobile:

```typescript
const ctaContainer = page.locator('[class*="cta"]')
const flexDirection = await ctaContainer.evaluate((el) => {
  return window.getComputedStyle(el).flexDirection
})
expect(flexDirection).toBe('column') // Mobile
```

### 7. Viewport Transition Testing

Test smooth transitions between viewport sizes:

```typescript
const viewports = [
  { width: 1920, height: 1080, expectedColumns: 3 },
  { width: 1024, height: 768, expectedColumns: 3 },
  { width: 768, height: 1024, expectedColumns: 2 },
  { width: 375, height: 667, expectedColumns: 1 },
]

for (const viewport of viewports) {
  await page.setViewportSize({ width: viewport.width, height: viewport.height })
  await page.waitForTimeout(100) // Allow CSS transitions

  const hasOverflow = await hasHorizontalOverflow(page)
  expect(hasOverflow).toBe(false)

  // Verify grid columns
  const columnCount = await getGridColumnCount(page)
  expect(columnCount).toBe(viewport.expectedColumns)
}
```

## Best Practices

- **Use data-testid attributes** for reliable element selection
- **Test multiple breakpoints** including edge cases (320px small mobile)
- **Allow time for CSS transitions** with small waitForTimeout after viewport changes
- **Test both visibility and computed styles** for comprehensive coverage
- **Verify no horizontal overflow** at every viewport size
- **Check touch targets on all interactive elements** for accessibility
- **Use describe blocks per viewport** for clear test organization

## Test File Structure

```typescript
import { test, expect, Page } from '@playwright/test'

// Helper functions
async function hasHorizontalOverflow(page: Page): Promise<boolean> { ... }

// Viewport-specific test suites
test.describe('Desktop Viewport (1920x1080)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/')
  })

  test('displays three-column grid', async ({ page }) => { ... })
})

test.describe('Mobile Viewport (375x667)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
  })

  test('displays single-column layout', async ({ page }) => { ... })
})
```

## Common Breakpoints

| Device | Width | Typical Grid Columns |
|--------|-------|---------------------|
| Large Desktop | 1920px | 3+ columns |
| Desktop | 1200px | 3 columns |
| Laptop | 1024px | 3 columns |
| Tablet Portrait | 768px | 2 columns |
| Mobile | 375px | 1 column |
| Small Mobile | 320px | 1 column |

## Resources

### References

- [Playwright Viewport API](https://playwright.dev/docs/api/class-page#page-set-viewport-size)
- [WCAG 2.1 Target Size](https://www.w3.org/WAI/WCAG21/Understanding/target-size.html)
- [CSS Grid Responsive Patterns](https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Grid_Layout/Responsive_Layouts)
