---
name: wcag-skip-link
description: Implement the WCAG 2.4.1 Bypass Blocks skip-link pattern — visible only on keyboard focus, slides into view, focuses the main landmark. Use when a page needs a keyboard-accessible bypass for repeated navigation content.
---

Implements the WCAG 2.4.1 Bypass Blocks pattern: a skip-link as the first focusable element of the page, visually hidden until it receives keyboard focus, that moves focus to the main landmark on activation.

See [README.md](references/README.md) for the CSS + HTML pattern, why `display: none` and `visibility: hidden` cannot be used, and how to verify it under jsdom.
