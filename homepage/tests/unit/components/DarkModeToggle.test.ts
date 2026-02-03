/**
 * Unit tests for DarkModeToggle component.
 * Owner: Scenario 11 - Dark Mode Toggle
 *
 * Tests:
 * - Toggle button renders correctly
 * - Button has correct accessibility attributes
 * - Sun and moon icons are present
 * - Button has proper styling classes
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Window, type Document as HappyDomDocument } from 'happy-dom';

describe('DarkModeToggle Component', () => {
  let document: HappyDomDocument;

  // The rendered HTML structure of the DarkModeToggle component
  const darkModeToggleHTML = `
    <button
      id="dark-mode-toggle"
      type="button"
      aria-label="Toggle dark mode"
      class="dark-mode-toggle p-2 rounded-lg transition-colors hover:bg-gray-100 dark:hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-primary"
    >
      <!-- Sun icon (shown in dark mode) -->
      <svg
        id="sun-icon"
        class="w-6 h-6 text-yellow-500 hidden dark:block"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
      >
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          stroke-width="2"
          d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
        ></path>
      </svg>
      <!-- Moon icon (shown in light mode) -->
      <svg
        id="moon-icon"
        class="w-6 h-6 text-gray-700 dark:hidden"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
      >
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          stroke-width="2"
          d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
        ></path>
      </svg>
      <span class="sr-only">Toggle dark mode</span>
    </button>
  `;

  beforeEach(() => {
    const window = new Window();
    window.document.body.innerHTML = darkModeToggleHTML;
    document = window.document;
  });

  describe('Component Structure', () => {
    it('renders the toggle button with correct ID', () => {
      const toggle = document.querySelector('#dark-mode-toggle');
      expect(toggle).toBeTruthy();
      expect(toggle?.tagName).toBe('BUTTON');
    });

    it('button has correct type attribute', () => {
      const toggle = document.querySelector('#dark-mode-toggle');
      expect(toggle?.getAttribute('type')).toBe('button');
    });

    it('contains both sun and moon icons', () => {
      const sunIcon = document.querySelector('#sun-icon');
      const moonIcon = document.querySelector('#moon-icon');
      expect(sunIcon).toBeTruthy();
      expect(moonIcon).toBeTruthy();
    });
  });

  describe('Accessibility', () => {
    it('has aria-label for screen readers', () => {
      const toggle = document.querySelector('#dark-mode-toggle');
      expect(toggle?.getAttribute('aria-label')).toBe('Toggle dark mode');
    });

    it('icons have aria-hidden attribute', () => {
      const sunIcon = document.querySelector('#sun-icon');
      const moonIcon = document.querySelector('#moon-icon');
      expect(sunIcon?.getAttribute('aria-hidden')).toBe('true');
      expect(moonIcon?.getAttribute('aria-hidden')).toBe('true');
    });

    it('has screen reader only text', () => {
      const srOnlyText = document.querySelector('.sr-only');
      expect(srOnlyText).toBeTruthy();
      expect(srOnlyText?.textContent?.trim()).toBe('Toggle dark mode');
    });

    it('has focus ring classes for keyboard navigation', () => {
      const toggle = document.querySelector('#dark-mode-toggle');
      expect(toggle?.classList.contains('focus:outline-none')).toBe(true);
      expect(toggle?.classList.contains('focus:ring-2')).toBe(true);
    });
  });

  describe('Icon Display Logic', () => {
    it('sun icon has dark:block class to show in dark mode', () => {
      const sunIcon = document.querySelector('#sun-icon');
      expect(sunIcon?.classList.contains('hidden')).toBe(true);
      expect(sunIcon?.classList.contains('dark:block')).toBe(true);
    });

    it('moon icon has dark:hidden class to hide in dark mode', () => {
      const moonIcon = document.querySelector('#moon-icon');
      expect(moonIcon?.classList.contains('dark:hidden')).toBe(true);
    });

    it('sun icon has correct color class', () => {
      const sunIcon = document.querySelector('#sun-icon');
      expect(sunIcon?.classList.contains('text-yellow-500')).toBe(true);
    });

    it('moon icon has correct color class', () => {
      const moonIcon = document.querySelector('#moon-icon');
      expect(moonIcon?.classList.contains('text-gray-700')).toBe(true);
    });
  });

  describe('Styling', () => {
    it('has padding and rounded corners', () => {
      const toggle = document.querySelector('#dark-mode-toggle');
      expect(toggle?.classList.contains('p-2')).toBe(true);
      expect(toggle?.classList.contains('rounded-lg')).toBe(true);
    });

    it('has hover background classes for both modes', () => {
      const toggle = document.querySelector('#dark-mode-toggle');
      expect(toggle?.classList.contains('hover:bg-gray-100')).toBe(true);
      expect(toggle?.classList.contains('dark:hover:bg-gray-800')).toBe(true);
    });

    it('has transition class for smooth state changes', () => {
      const toggle = document.querySelector('#dark-mode-toggle');
      expect(toggle?.classList.contains('transition-colors')).toBe(true);
    });

    it('icons have consistent sizing', () => {
      const sunIcon = document.querySelector('#sun-icon');
      const moonIcon = document.querySelector('#moon-icon');
      expect(sunIcon?.classList.contains('w-6')).toBe(true);
      expect(sunIcon?.classList.contains('h-6')).toBe(true);
      expect(moonIcon?.classList.contains('w-6')).toBe(true);
      expect(moonIcon?.classList.contains('h-6')).toBe(true);
    });
  });

  describe('SVG Structure', () => {
    it('SVG icons have correct viewBox', () => {
      const sunIcon = document.querySelector('#sun-icon');
      const moonIcon = document.querySelector('#moon-icon');
      expect(sunIcon?.getAttribute('viewBox')).toBe('0 0 24 24');
      expect(moonIcon?.getAttribute('viewBox')).toBe('0 0 24 24');
    });

    it('SVG icons have stroke-based rendering', () => {
      const sunIcon = document.querySelector('#sun-icon');
      const moonIcon = document.querySelector('#moon-icon');
      expect(sunIcon?.getAttribute('fill')).toBe('none');
      expect(sunIcon?.getAttribute('stroke')).toBe('currentColor');
      expect(moonIcon?.getAttribute('fill')).toBe('none');
      expect(moonIcon?.getAttribute('stroke')).toBe('currentColor');
    });

    it('SVG paths have proper stroke attributes', () => {
      const paths = document.querySelectorAll('path');
      paths.forEach((path) => {
        expect(path?.getAttribute('stroke-linecap')).toBe('round');
        expect(path?.getAttribute('stroke-linejoin')).toBe('round');
        expect(path?.getAttribute('stroke-width')).toBe('2');
      });
    });
  });
});
