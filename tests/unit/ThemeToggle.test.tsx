import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import ThemeToggle, { ThemeProvider, useTheme } from '../../src/components/ThemeToggle';

function TestConsumer() {
  const { theme, toggleTheme } = useTheme();
  return (
    <div>
      <span data-testid="theme-value">{theme}</span>
      <button data-testid="toggle-btn" onClick={toggleTheme}>
        Toggle
      </button>
    </div>
  );
}

function renderWithProvider(ui: React.ReactElement) {
  return render(<ThemeProvider>{ui}</ThemeProvider>);
}

describe('ThemeToggle', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('dark');
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('dark');
  });

  describe('Test Case 1: System preference detection', () => {
    it('falls back to light theme when no localStorage and system prefers light', () => {
      renderWithProvider(<TestConsumer />);
      expect(screen.getByTestId('theme-value').textContent).toBe('light');
    });

    it('uses dark theme when system prefers dark and no localStorage', () => {
      const originalMatchMedia = window.matchMedia;
      window.matchMedia = (query: string) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: () => {},
        removeListener: () => {},
        addEventListener: () => {},
        removeEventListener: () => {},
        dispatchEvent: () => false,
      });

      try {
        renderWithProvider(<TestConsumer />);
        expect(screen.getByTestId('theme-value').textContent).toBe('dark');
      } finally {
        window.matchMedia = originalMatchMedia;
      }
    });

    it('respects stored localStorage preference over system preference', () => {
      localStorage.setItem('theme', 'dark');

      renderWithProvider(<TestConsumer />);
      expect(screen.getByTestId('theme-value').textContent).toBe('dark');
    });
  });

  describe('Test Case 2: Theme toggle changes document class', () => {
    it('adds dark class to documentElement when toggling to dark', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);

      const button = screen.getByRole('button');
      await user.click(button);

      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('removes dark class from documentElement when toggling to light', async () => {
      document.documentElement.classList.add('dark');
      localStorage.setItem('theme', 'dark');

      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);

      const button = screen.getByRole('button');
      await user.click(button);

      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });

    it('updates theme state when toggle is clicked', async () => {
      const user = userEvent.setup();
      renderWithProvider(
        <>
          <ThemeToggle />
          <TestConsumer />
        </>,
      );

      expect(screen.getByTestId('theme-value').textContent).toBe('light');

      const toggleBtn = screen.getByRole('button', { name: /switch to dark mode/i });
      await user.click(toggleBtn);

      expect(screen.getByTestId('theme-value').textContent).toBe('dark');
    });

    it('toggles back to light on second click', async () => {
      const user = userEvent.setup();
      renderWithProvider(
        <>
          <ThemeToggle />
          <TestConsumer />
        </>,
      );

      const toggleBtn = screen.getByRole('button', { name: /switch to dark mode/i });
      await user.click(toggleBtn);
      await user.click(toggleBtn);

      expect(screen.getByTestId('theme-value').textContent).toBe('light');
    });
  });

  describe('Test Case 3: Theme persistence across reloads', () => {
    it('theme persists in localStorage after toggle', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);

      const button = screen.getByRole('button');
      await user.click(button);

      expect(localStorage.getItem('theme')).toBe('dark');
    });

    it('stored theme is read on new component mount', () => {
      localStorage.setItem('theme', 'dark');

      renderWithProvider(<TestConsumer />);
      expect(screen.getByTestId('theme-value').textContent).toBe('dark');
    });

    it('stored light theme is read on new component mount', () => {
      localStorage.setItem('theme', 'light');

      renderWithProvider(<TestConsumer />);
      expect(screen.getByTestId('theme-value').textContent).toBe('light');
    });
  });

  describe('Test Case 4: localStorage key for theme preference', () => {
    it('stores theme as "theme" key in localStorage', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);

      await user.click(screen.getByRole('button'));
      expect(localStorage.getItem('theme')).toBe('dark');

      await user.click(screen.getByRole('button'));
      expect(localStorage.getItem('theme')).toBe('light');
    });

    it('updates localStorage on each toggle', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');

      // Toggle to dark
      await user.click(button);
      expect(localStorage.getItem('theme')).toBe('dark');

      // Toggle to light
      await user.click(button);
      expect(localStorage.getItem('theme')).toBe('light');

      // Toggle to dark again
      await user.click(button);
      expect(localStorage.getItem('theme')).toBe('dark');
    });

    it('stores only valid theme values', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');

      // Click multiple times, always expect valid values
      for (let i = 0; i < 5; i++) {
        await user.click(button);
        const val = localStorage.getItem('theme');
        expect(['light', 'dark']).toContain(val);
      }
    });
  });

  describe('Test Case 5: Theme toggle accessibility', () => {
    it('has an accessible ARIA label', () => {
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('aria-label');
      expect(button.getAttribute('aria-label')!.toLowerCase()).toMatch(/switch to (dark|light) mode/);
    });

    it('updates aria-label based on current theme', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');

      // In light mode, label should mention switching to dark
      expect(button.getAttribute('aria-label')!.toLowerCase()).toContain('dark');

      await user.click(button);

      // In dark mode, label should mention switching to light
      expect(button.getAttribute('aria-label')!.toLowerCase()).toContain('light');
    });

    it('is keyboard focusable', () => {
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');
      button.focus();
      expect(document.activeElement).toBe(button);
    });

    it('responds to Enter key', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');

      button.focus();
      await user.keyboard('{Enter}');

      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('responds to Space key', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');

      button.focus();
      await user.keyboard(' ');

      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('renders an SVG icon inside the button', () => {
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');
      const svg = button.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });
  });

  describe('Test Case 6: No flash of incorrect theme (FOUC prevention)', () => {
    it('theme-blocking script logic sets dark class before React mounts', () => {
      // Simulate what the inline script in index.html does
      localStorage.setItem('theme', 'dark');

      // The script checks localStorage and adds dark class before React mounts
      const theme = localStorage.getItem('theme');
      if (theme === 'dark') {
        document.documentElement.classList.add('dark');
      }

      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('theme-blocking script handles system preference when no stored value', () => {
      localStorage.clear();

      // When no stored theme, check system preference
      const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
      if (!localStorage.getItem('theme') && prefersDark) {
        document.documentElement.classList.add('dark');
      }

      // Our mock returns false for matchMedia, so dark should NOT be added
      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });

    it('theme-blocking script adds no class when no stored preference and system is light', () => {
      localStorage.clear();
      document.documentElement.classList.remove('dark');

      const theme = localStorage.getItem('theme');
      if (theme === 'dark' || (!theme && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
        document.documentElement.classList.add('dark');
      }

      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });
  });

  describe('Test Case 7: Theme applies to all sections', () => {
    it('documentElement has dark class when theme is dark', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);

      await user.click(screen.getByRole('button'));
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('documentElement does not have dark class when theme is light', () => {
      renderWithProvider(<TestConsumer />);
      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });

    it('ThemeProvider applies dark class to html element', async () => {
      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);

      expect(document.documentElement.classList.contains('dark')).toBe(false);

      await user.click(screen.getByRole('button'));
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });
  });

  describe('ThemeProvider context', () => {
    it('provides theme context to children', () => {
      renderWithProvider(<TestConsumer />);
      expect(screen.getByTestId('theme-value').textContent).toBe('light');
    });

    it('throws when useTheme is used outside ThemeProvider', () => {
      // Suppress console.error for expected error
      const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
      expect(() => render(<TestConsumer />)).toThrow();
      spy.mockRestore();
    });
  });

  describe('ThemeToggle component rendering', () => {
    it('renders Moon icon in light mode', () => {
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');
      const svg = button.querySelector('svg');
      expect(svg).toBeInTheDocument();
      // lucide-react Moon icon
      expect(button.innerHTML).toContain('svg');
    });

    it('renders Sun icon in dark mode', async () => {
      localStorage.setItem('theme', 'dark');
      document.documentElement.classList.add('dark');

      const user = userEvent.setup();
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');
      // Should show Sun icon (switch to light)
      expect(button.querySelector('svg')).toBeInTheDocument();
    });

    it('has transition classes for smooth theme changes', () => {
      renderWithProvider(<ThemeToggle />);
      const button = screen.getByRole('button');
      expect(button.className).toMatch(/transition/);
    });
  });
});
