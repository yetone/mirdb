# React Smooth Scroll Navigation

## Overview

This skill provides the pattern for implementing smooth scrolling navigation links in React for single-page applications. It handles anchor links that scroll smoothly to page sections while supporting both internal (hash) links and external links.

## When to Use This Skill

Use this skill when:
- Creating navigation links that scroll to sections on the same page
- Building a landing page with section-based navigation
- Implementing smooth scroll behavior for anchor links
- Needing to distinguish between internal hash links and external URLs

## Core Capabilities

### 1. Navigation Component with Click Handler

```tsx
import type { NavItem } from '@/types'

interface NavigationProps {
  onNavClick?: () => void // Optional callback (useful for closing mobile menu)
}

export function Navigation({ onNavClick }: NavigationProps) {
  const handleClick = (e: React.MouseEvent<HTMLAnchorElement>, item: NavItem) => {
    // Only handle internal hash links
    if (!item.external && item.href.startsWith('#')) {
      e.preventDefault()
      const targetId = item.href.slice(1) // Remove the '#'
      const targetElement = document.getElementById(targetId)
      if (targetElement) {
        targetElement.scrollIntoView({ behavior: 'smooth' })
        onNavClick?.() // Close mobile menu if applicable
      }
    } else if (item.external) {
      // External links just trigger callback, don't prevent default
      onNavClick?.()
    }
  }

  return (
    <nav aria-label="Main navigation">
      <ul role="menubar">
        {navItems.map((item) => (
          <li key={item.label} role="none">
            <a
              href={item.href}
              onClick={(e) => handleClick(e, item)}
              target={item.external ? '_blank' : undefined}
              rel={item.external ? 'noopener noreferrer' : undefined}
              role="menuitem"
            >
              {item.label}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  )
}
```

### 2. NavItem Type Definition

```typescript
// src/types/index.ts
export interface NavItem {
  label: string
  href: string
  external?: boolean
}
```

### 3. Configuration Data

```typescript
// src/data/config.ts
import type { NavItem } from '@/types'

export const navItems: NavItem[] = [
  { label: 'Features', href: '#features' },
  { label: 'Quick Start', href: '#quick-start' },
  { label: 'Usage', href: '#usage' },
  { label: 'GitHub', href: 'https://github.com/org/repo', external: true },
]
```

### 4. CSS Enhancement (Optional)

Add to your global CSS for consistent smooth scroll behavior:

```css
html {
  scroll-behavior: smooth;
}
```

## Best Practices

- Use `scrollIntoView({ behavior: 'smooth' })` for JS-triggered scrolling
- Always check if target element exists before scrolling
- Use `e.preventDefault()` only for internal hash links
- Add `rel="noopener noreferrer"` for external links with `target="_blank"`
- Provide an `onNavClick` callback for mobile menu integration
- Keep navigation configuration in a separate data file
- Use `role="menuitem"` for ARIA accessibility on nav links

## Page Sections

Ensure your page has sections with matching IDs:

```tsx
// App.tsx
<main>
  <section id="features">...</section>
  <section id="quick-start">...</section>
  <section id="usage">...</section>
</main>
```

## Testing

Test with Playwright:

```typescript
test("click nav link scrolls to section", async ({ page }) => {
  const featuresLink = page.locator('header nav').getByRole('menuitem', { name: /features/i })
  await featuresLink.click()

  // Wait for smooth scroll
  await page.waitForTimeout(1000)

  // Verify section is in viewport
  const featuresSection = page.locator('#features')
  await expect(featuresSection).toBeInViewport()
})
```
