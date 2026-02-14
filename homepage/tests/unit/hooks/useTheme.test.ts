import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import React from 'react';
import { useTheme } from '../../../src/hooks/useTheme';
import { ThemeProvider } from '../../../src/context/ThemeContext';
import { THEME_STORAGE_KEY } from '../../../src/utils/constants';

// Wrapper to provide ThemeProvider context
const wrapper = ({ children }: { children: React.ReactNode }) => (
  React.createElement(ThemeProvider, null, children)
);

describe('useTheme hook', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('dark', 'light');
  });

  it('should return theme, toggleTheme, and isDark', () => {
    const { result } = renderHook(() => useTheme(), { wrapper });

    expect(result.current).toHaveProperty('theme');
    expect(result.current).toHaveProperty('toggleTheme');
    expect(result.current).toHaveProperty('isDark');
    expect(typeof result.current.toggleTheme).toBe('function');
    expect(typeof result.current.isDark).toBe('boolean');
  });

  it('should default to light theme when no localStorage and no system preference', () => {
    // matchMedia mock is already set up in setup.ts to return false for dark preference
    const { result } = renderHook(() => useTheme(), { wrapper });

    expect(result.current.theme).toBe('light');
    expect(result.current.isDark).toBe(false);
  });

  it('should toggle theme from light to dark', () => {
    const { result } = renderHook(() => useTheme(), { wrapper });

    // Start with light theme
    if (result.current.theme === 'dark') {
      act(() => {
        result.current.toggleTheme();
      });
    }

    expect(result.current.theme).toBe('light');
    expect(result.current.isDark).toBe(false);

    act(() => {
      result.current.toggleTheme();
    });

    expect(result.current.theme).toBe('dark');
    expect(result.current.isDark).toBe(true);
  });

  it('should toggle theme from dark to light', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark');

    const { result } = renderHook(() => useTheme(), { wrapper });

    expect(result.current.theme).toBe('dark');
    expect(result.current.isDark).toBe(true);

    act(() => {
      result.current.toggleTheme();
    });

    expect(result.current.theme).toBe('light');
    expect(result.current.isDark).toBe(false);
  });

  it('should persist theme to localStorage when toggled', () => {
    const { result } = renderHook(() => useTheme(), { wrapper });

    // Ensure we start with light
    if (result.current.theme === 'dark') {
      act(() => {
        result.current.toggleTheme();
      });
    }

    act(() => {
      result.current.toggleTheme();
    });

    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');

    act(() => {
      result.current.toggleTheme();
    });

    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('light');
  });

  it('should read initial theme from localStorage', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark');

    const { result } = renderHook(() => useTheme(), { wrapper });

    expect(result.current.theme).toBe('dark');
    expect(result.current.isDark).toBe(true);
  });

  it('should apply theme class to document root', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark');

    renderHook(() => useTheme(), { wrapper });

    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(document.documentElement.classList.contains('light')).toBe(false);
  });

  it('should update document class when theme changes', () => {
    const { result } = renderHook(() => useTheme(), { wrapper });

    // Ensure we start with light
    if (result.current.theme === 'dark') {
      act(() => {
        result.current.toggleTheme();
      });
    }

    expect(document.documentElement.classList.contains('light')).toBe(true);

    act(() => {
      result.current.toggleTheme();
    });

    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(document.documentElement.classList.contains('light')).toBe(false);
  });
});
