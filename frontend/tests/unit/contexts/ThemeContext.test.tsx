import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';
import { ThemeProvider, useTheme } from '../../../src/contexts/ThemeContext';
import { SUPPORTED_THEMES, DEFAULT_THEME } from '../../../src/utils/constants';
import type { Theme } from '../../../src/types';

function TestConsumer() {
  const { theme, setTheme } = useTheme();
  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      {SUPPORTED_THEMES.map((t) => (
        <button
          key={t}
          data-testid={`set-${t}`}
          onClick={() => setTheme(t)}
        >
          Set {t}
        </button>
      ))}
    </div>
  );
}

describe('ThemeContext', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Render homepage with light theme', () => {
    it('sets data-theme attribute to light when localStorage has light', () => {
      localStorage.setItem('theme', 'light');
      render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      expect(screen.getByTestId('current-theme').textContent).toBe('light');
    });
  });

  describe('Test Case 2: Render homepage with dark theme', () => {
    it('sets data-theme attribute to dark when localStorage has dark', () => {
      localStorage.setItem('theme', 'dark');
      render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      expect(screen.getByTestId('current-theme').textContent).toBe('dark');
    });

    it('uses default theme (dark) when no localStorage value exists', () => {
      render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      expect(document.documentElement.getAttribute('data-theme')).toBe(DEFAULT_THEME);
      expect(screen.getByTestId('current-theme').textContent).toBe(DEFAULT_THEME);
    });
  });

  describe('Test Case 3: Toggle theme from light to cyberpunk via theme switcher', () => {
    it('updates theme without page reload when setTheme is called', () => {
      localStorage.setItem('theme', 'light');
      render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      expect(screen.getByTestId('current-theme').textContent).toBe('light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      fireEvent.click(screen.getByTestId('set-cyberpunk'));

      expect(screen.getByTestId('current-theme').textContent).toBe('cyberpunk');
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    it('updates theme for all supported themes', () => {
      render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      for (const theme of SUPPORTED_THEMES) {
        fireEvent.click(screen.getByTestId(`set-${theme}`));
        expect(screen.getByTestId('current-theme').textContent).toBe(theme);
        expect(document.documentElement.getAttribute('data-theme')).toBe(theme);
      }
    });
  });

  describe('Test Case 4: Select theme, reload page', () => {
    it('persists selected theme to localStorage', () => {
      localStorage.setItem('theme', 'light');
      render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      fireEvent.click(screen.getByTestId('set-synthwave'));

      expect(localStorage.getItem('theme')).toBe('synthwave');
    });

    it('restores theme from localStorage on re-render (simulating page reload)', () => {
      localStorage.setItem('theme', 'cyberpunk');

      const { unmount } = render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
      expect(screen.getByTestId('current-theme').textContent).toBe('cyberpunk');

      unmount();
      document.documentElement.removeAttribute('data-theme');

      // Simulate page reload by re-rendering
      render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
      expect(screen.getByTestId('current-theme').textContent).toBe('cyberpunk');
    });

    it('falls back to default theme when localStorage contains invalid theme value', () => {
      localStorage.setItem('theme', 'invalid-theme');
      render(
        <ThemeProvider>
          <TestConsumer />
        </ThemeProvider>
      );

      expect(document.documentElement.getAttribute('data-theme')).toBe(DEFAULT_THEME);
      expect(screen.getByTestId('current-theme').textContent).toBe(DEFAULT_THEME);
    });
  });
});
