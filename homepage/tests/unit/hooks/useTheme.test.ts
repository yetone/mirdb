/**
 * Unit tests for useTheme hook
 * Owner: Scenario 6 - Dark/Light Theme Toggle
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import React from 'react';
import { useTheme } from '../../../src/hooks/useTheme';
import { ThemeProvider } from '../../../src/context/ThemeContext';

// Helper function to render the hook with ThemeProvider
const renderUseTheme = (defaultTheme: 'light' | 'dark' = 'light') => {
  const wrapper = ({ children }: { children: React.ReactNode }) =>
    React.createElement(ThemeProvider, { defaultTheme }, children);

  return renderHook(() => useTheme(), { wrapper });
};

describe('useTheme Hook', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Initial State', () => {
    it('returns current theme', () => {
      const { result } = renderUseTheme('light');
      expect(result.current.theme).toBe('light');
    });

    it('returns isDark as false when theme is light', () => {
      const { result } = renderUseTheme('light');
      expect(result.current.isDark).toBe(false);
    });

    it('returns isLight as true when theme is light', () => {
      const { result } = renderUseTheme('light');
      expect(result.current.isLight).toBe(true);
    });

    it('returns isDark as true when theme is dark', () => {
      const { result } = renderUseTheme('dark');
      expect(result.current.isDark).toBe(true);
    });

    it('returns isLight as false when theme is dark', () => {
      const { result } = renderUseTheme('dark');
      expect(result.current.isLight).toBe(false);
    });
  });

  describe('toggleTheme Function', () => {
    it('provides toggleTheme function', () => {
      const { result } = renderUseTheme();
      expect(typeof result.current.toggleTheme).toBe('function');
    });

    it('toggles from light to dark', () => {
      const { result } = renderUseTheme('light');

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('dark');
      expect(result.current.isDark).toBe(true);
      expect(result.current.isLight).toBe(false);
    });

    it('toggles from dark to light', () => {
      const { result } = renderUseTheme('dark');

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('light');
      expect(result.current.isDark).toBe(false);
      expect(result.current.isLight).toBe(true);
    });

    it('toggles back and forth correctly', () => {
      const { result } = renderUseTheme('light');

      // Light -> Dark
      act(() => {
        result.current.toggleTheme();
      });
      expect(result.current.theme).toBe('dark');

      // Dark -> Light
      act(() => {
        result.current.toggleTheme();
      });
      expect(result.current.theme).toBe('light');

      // Light -> Dark
      act(() => {
        result.current.toggleTheme();
      });
      expect(result.current.theme).toBe('dark');
    });
  });

  describe('setTheme Function', () => {
    it('provides setTheme function', () => {
      const { result } = renderUseTheme();
      expect(typeof result.current.setTheme).toBe('function');
    });

    it('sets theme to dark', () => {
      const { result } = renderUseTheme('light');

      act(() => {
        result.current.setTheme('dark');
      });

      expect(result.current.theme).toBe('dark');
    });

    it('sets theme to light', () => {
      const { result } = renderUseTheme('dark');

      act(() => {
        result.current.setTheme('light');
      });

      expect(result.current.theme).toBe('light');
    });

    it('does not change state when setting same theme', () => {
      const { result } = renderUseTheme('light');

      act(() => {
        result.current.setTheme('light');
      });

      expect(result.current.theme).toBe('light');
    });
  });

  describe('localStorage Persistence', () => {
    it('persists theme to localStorage when toggled', () => {
      const { result } = renderUseTheme('light');

      act(() => {
        result.current.toggleTheme();
      });

      expect(localStorage.getItem('theme')).toBe('dark');
    });

    it('persists theme to localStorage when set directly', () => {
      const { result } = renderUseTheme('light');

      act(() => {
        result.current.setTheme('dark');
      });

      expect(localStorage.getItem('theme')).toBe('dark');
    });
  });

  describe('Document Theme Attribute', () => {
    it('applies theme attribute to document when toggled', () => {
      const { result } = renderUseTheme('light');

      act(() => {
        result.current.toggleTheme();
      });

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('applies theme attribute when set directly', () => {
      const { result } = renderUseTheme('light');

      act(() => {
        result.current.setTheme('dark');
      });

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });

  describe('Error Handling', () => {
    it('throws error when used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi
        .spyOn(console, 'error')
        .mockImplementation(() => {});

      expect(() => {
        renderHook(() => useTheme());
      }).toThrow('useThemeContext must be used within a ThemeProvider');

      consoleSpy.mockRestore();
    });
  });
});

// Need to import vi for the error handling test
import { vi } from 'vitest';
