/**
 * Theme Toggle Component.
 * Owner: Scenario 9 - Dark Mode Support
 *
 * Requirements: REQ-10
 *
 * Provides a toggle button with sun/moon icon to switch between
 * light and dark themes with localStorage persistence.
 */

import { getTheme, toggleTheme, initTheme } from '../utils/theme';
import type { Theme } from '../types';

/**
 * SVG icon for the sun (light mode indicator).
 */
const sunIcon = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="theme-icon theme-icon--sun" aria-hidden="true">
  <circle cx="12" cy="12" r="5"></circle>
  <line x1="12" y1="1" x2="12" y2="3"></line>
  <line x1="12" y1="21" x2="12" y2="23"></line>
  <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line>
  <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line>
  <line x1="1" y1="12" x2="3" y2="12"></line>
  <line x1="21" y1="12" x2="23" y2="12"></line>
  <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line>
  <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line>
</svg>
`;

/**
 * SVG icon for the moon (dark mode indicator).
 */
const moonIcon = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="theme-icon theme-icon--moon" aria-hidden="true">
  <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path>
</svg>
`;

/**
 * Get the icon for the current theme.
 * Shows sun when in dark mode (to indicate switching to light).
 * Shows moon when in light mode (to indicate switching to dark).
 */
function getIconForTheme(theme: Theme): string {
  return theme === 'dark' ? sunIcon : moonIcon;
}

/**
 * Get the aria-label for the button based on current theme.
 */
function getAriaLabel(theme: Theme): string {
  return theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme';
}

/**
 * Update the toggle button appearance based on current theme.
 */
function updateToggleButton(button: HTMLButtonElement): void {
  const theme = getTheme();
  button.innerHTML = getIconForTheme(theme);
  button.setAttribute('aria-label', getAriaLabel(theme));
  button.setAttribute('data-theme-state', theme);
}

/**
 * Create and render the theme toggle component.
 */
export function renderThemeToggle(): HTMLElement {
  // Initialize theme on first render
  initTheme();

  const container = document.createElement('div');
  container.className = 'theme-toggle-container';

  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'theme-toggle';
  button.setAttribute('aria-pressed', 'false');

  // Set initial state
  updateToggleButton(button);

  // Handle click events
  button.addEventListener('click', () => {
    toggleTheme();
    updateToggleButton(button);
  });

  container.appendChild(button);

  // Add inline styles for the component
  addThemeToggleStyles();

  return container;
}

/**
 * Add CSS styles for the theme toggle component.
 * Injected once to avoid duplicate style tags.
 */
function addThemeToggleStyles(): void {
  const styleId = 'theme-toggle-styles';
  if (document.getElementById(styleId)) {
    return;
  }

  const style = document.createElement('style');
  style.id = styleId;
  style.textContent = `
    .theme-toggle-container {
      display: inline-flex;
      align-items: center;
    }

    .theme-toggle {
      display: flex;
      align-items: center;
      justify-content: center;
      width: 40px;
      height: 40px;
      padding: 8px;
      border: none;
      border-radius: var(--radius-lg, 8px);
      background-color: transparent;
      color: var(--color-text);
      cursor: pointer;
      transition: background-color var(--transition-fast, 150ms ease),
                  color var(--transition-fast, 150ms ease),
                  transform var(--transition-fast, 150ms ease);
    }

    .theme-toggle:hover {
      background-color: var(--color-hover, rgba(0, 0, 0, 0.05));
    }

    .theme-toggle:focus {
      outline: 2px solid var(--color-focus-ring, #3b82f6);
      outline-offset: 2px;
    }

    .theme-toggle:active {
      transform: scale(0.95);
    }

    .theme-icon {
      width: 24px;
      height: 24px;
    }

    .theme-icon--sun {
      color: var(--color-accent, #f59e0b);
    }

    .theme-icon--moon {
      color: var(--color-primary, #3b82f6);
    }

    [data-theme="dark"] .theme-icon--sun {
      color: #fbbf24;
    }

    [data-theme="dark"] .theme-icon--moon {
      color: #60a5fa;
    }
  `;

  document.head.appendChild(style);
}
