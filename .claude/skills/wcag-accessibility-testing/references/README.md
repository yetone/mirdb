# WCAG Accessibility Testing

## Overview

Comprehensive pattern for testing WCAG 2.1 AA compliance in CSS and static HTML. Covers automated validation of accessibility CSS rules, HTML semantic checks, and programmatic color contrast computation using the WCAG 2.1 relative luminance formula.

## Testing Pattern

This project tests accessibility by:
1. Building the Astro static site to generate HTML
2. Parsing the built HTML with `linkedom`
3. Reading CSS files directly for rule validation
4. Computing contrast ratios from CSS custom properties

## CSS Rule Validation

```typescript
// Check for sr-only utility
expect(css).toMatch(/\.sr-only\s*\{/);
expect(css).toMatch(/clip:\s*rect\(0,\s*0,\s*0,\s*0\)/);

// Check for skip-to-main link
expect(css).toMatch(/\.skip-to-main\s*\{/);
expect(css).toContain('top: -100%');

// Check for focus-visible
expect(css).toMatch(/:focus-visible\s*\{/);
expect(css).toMatch(/outline:\s*3px\s+solid/);

// Check for reduced motion
expect(css).toContain('prefers-reduced-motion: reduce');
expect(css).toMatch(/animation-duration:\s*0\.01ms\s*!important/);

// Check for forced colors
expect(css).toContain('forced-colors: active');
```

## HTML Semantic Validation

```typescript
// Alt text check
const images = document.querySelectorAll('img');
images.forEach(img => {
  expect(img.hasAttribute('alt')).toBe(true);
});

// Heading hierarchy
const h1s = document.querySelectorAll('h1');
expect(h1s.length).toBe(1);

// Section labeling
const sections = document.querySelectorAll('section');
sections.forEach(section => {
  const hasHeading = section.querySelector('h1, h2, h3, h4, h5, h6');
  const hasAriaLabel = section.hasAttribute('aria-label');
  expect(hasHeading !== null || hasAriaLabel).toBe(true);
});
```

## Color Contrast Computation

See the `css-contrast-ratio-testing` skill for the WCAG 2.1 contrast ratio calculation details.

## Key Libraries

- `linkedom` - HTML parsing in Node.js (lightweight JSDOM alternative)
- `vitest` - Test runner with globals
- `node:fs`, `node:path`, `node:child_process` - File system and build utilities
