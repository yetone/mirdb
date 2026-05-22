# Dark Mode Implementation with DaisyUI

## Overview

This project uses DaisyUI's theme system for dark mode. DaisyUI provides built-in light and dark themes that are activated by setting `data-theme` on the `<html>` element. All `bg-base-*` and `text-base-*` utility classes automatically adapt.

## When to Use This Skill

Use this skill when users request:
- Adding dark mode to a DaisyUI/Tailwind project
- Theme toggle with localStorage persistence
- System preference detection (prefers-color-scheme)
- Smooth theme transitions

## Core Capabilities

### 1. Theme Detection and Persistence

Location: `web/src/js/main.js`

```javascript
function initTheme() {
  const root = document.documentElement;
  const savedTheme = localStorage.getItem('theme');

  let theme;
  if (savedTheme) {
    theme = savedTheme;
  } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    theme = 'dark';
  } else {
    theme = 'light';
  }

  root.setAttribute('data-theme', theme);

  // Listen for system preference changes
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
  mediaQuery.addEventListener('change', function(event) {
    if (!localStorage.getItem('theme')) {
      const newTheme = event.matches ? 'dark' : 'light';
      root.setAttribute('data-theme', newTheme);
    }
  });

  initThemeToggle();
}
```

### 2. Toggle Button Handling

```javascript
function initThemeToggle() {
  // IMPORTANT: Use querySelectorAll for multiple toggle buttons (desktop + mobile)
  const toggleBtns = document.querySelectorAll('[data-testid="theme-toggle"]');
  if (!toggleBtns.length) return;

  toggleBtns.forEach(function(toggleBtn) {
    toggleBtn.addEventListener('click', function() {
      const root = document.documentElement;
      const currentTheme = root.getAttribute('data-theme') || 'light';
      const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

      root.setAttribute('data-theme', newTheme);
      localStorage.setItem('theme', newTheme);

      // Update ALL toggle button icons/labels
      toggleBtns.forEach(function(btn) {
        updateToggleLabel(btn, newTheme);
      });
    });

    const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
    updateToggleLabel(toggleBtn, currentTheme);
  });
}

function updateToggleLabel(button, theme) {
  const isDark = theme === 'dark';
  button.setAttribute('aria-label', isDark ? 'Switch to light mode' : 'Switch to dark mode');
  button.setAttribute('title', isDark ? 'Switch to light mode' : 'Switch to dark mode');

  const icon = button.querySelector('.theme-icon');
  if (icon) {
    icon.textContent = isDark ? '☀' : '☽';
  }
}
```

### 3. CSS Transitions

Location: `web/src/css/main.css`

```css
html {
  transition: background-color 0.3s ease, color 0.3s ease;
}
body, .navbar, .hero, .card, .footer, section {
  transition: background-color 0.3s ease, color 0.3s ease, border-color 0.3s ease;
}
```

## Best Practices

- Use `querySelectorAll` not `querySelector` for toggle buttons when there are multiple (desktop + mobile)
- Remove hardcoded `data-theme="light"` from `<html>` so JS can set it dynamically before render
- Use DaisyUI semantic colors (`bg-base-100`, `text-base-content`) rather than explicit light/dark classes
- Check localStorage before system preference — user's explicit choice should override OS settings
- Update ALL toggle button labels/icons when theme changes, not just the clicked one

## Resources

### references/

- `README.md` - This documentation

### Related Files

- `web/src/js/main.js` - Theme detection, toggle, and persistence logic
- `web/src/css/main.css` - Theme transition styles
- `web/templates/partials/nav.html` - Theme toggle buttons in nav
- `tests/e2e/theme.spec.js` - Full E2E test suite for dark mode
