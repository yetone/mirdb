---
name: wcag-contrast-testing
description: Test color contrast ratios programmatically for WCAG accessibility compliance
scope: project
---

# WCAG Contrast Testing

Programmatically verify that color combinations meet WCAG AA accessibility requirements (4.5:1 contrast ratio minimum for normal text, 3:1 for large text).

## When to Use

- Testing button and CTA color accessibility
- Verifying text/background color combinations
- Ensuring landing pages meet accessibility standards
- Auditing CSS color schemes for WCAG compliance

## Quick Reference

```javascript
// Calculate contrast ratio between two hex colors
const ratio = calculateContrastRatio('#2563eb', '#ffffff'); // Returns ~5.3
expect(ratio).toBeGreaterThanOrEqual(4.5); // WCAG AA requirement
```

See [references/README.md](references/README.md) for full implementation.
