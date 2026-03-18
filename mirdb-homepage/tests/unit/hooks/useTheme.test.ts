/**
 * useTheme Hook Unit Tests.
 * Owner: Scenario 7 - Dark Mode Toggle
 *
 * Tests:
 * - Initial state with default dark theme
 * - Theme toggling between light and dark
 * - Theme persistence to localStorage
 * - Loading theme from localStorage
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useTheme } from '../../../src/hooks/useTheme';
import { THEME_KEY } from '../../../src/utils/constants';

describe('useTheme', () => {
  // Store original localStorage
  const originalLocalStorage = global.localStorage;
  let mockStorage: Record<string, string> = {};

  beforeEach(() => {
    vi.resetAllMocks();
    mockStorage = {};

    // Mock localStorage
    const localStorageMock = {
      getItem: vi.fn((key: string) => mockStorage[key] || null),
      setItem: vi.fn((key: string, value: string) => {
        mockStorage[key] = value;
      }),
      removeItem: vi.fn((key: string) => {
        delete mockStorage[key];
      }),
      clear: vi.fn(() => {
        mockStorage = {};
      }),
      length: 0,
      key: vi.fn(),
    };

    Object.defineProperty(global, 'localStorage', {
      value: localStorageMock,
      writable: true,
      configurable: true,
    });

    // Clear dark class from document for clean tests
    document.documentElement.classList.remove('dark');
  });

  afterEach(() => {
    Object.defineProperty(global, 'localStorage', {
      value: originalLocalStorage,
      writable: true,
      configurable: true,
    });
    document.documentElement.classList.remove('dark');
  });

  it('TC6: should return theme state and toggle function', () => {
    const { result } = renderHook(() => useTheme());

    expect(result.current.theme).toBeDefined();
    expect(typeof result.current.toggleTheme).toBe('function');
    expect(typeof result.current.setTheme).toBe('function');
  });

  it('should default to dark theme when no stored preference', () => {
    const { result } = renderHook(() => useTheme());

    expect(result.current.theme).toBe('dark');
  });

  it('should apply dark class to document when theme is dark', () => {
    renderHook(() => useTheme());

    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('should toggle from dark to light theme', () => {
    const { result } = renderHook(() => useTheme());

    expect(result.current.theme).toBe('dark');

    act(() => {
      result.current.toggleTheme();
    });

    expect(result.current.theme).toBe('light');
  });

  it('should toggle from light back to dark theme', () => {
    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.toggleTheme();
    });
    expect(result.current.theme).toBe('light');

    act(() => {
      result.current.toggleTheme();
    });
    expect(result.current.theme).toBe('dark');
  });

  it('TC7: should save theme preference to localStorage when toggling', () => {
    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.toggleTheme();
    });

    expect(global.localStorage.setItem).toHaveBeenCalledWith(THEME_KEY, 'light');
  });

  it('TC7: should save theme preference to localStorage when using setTheme', () => {
    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.setTheme('light');
    });

    expect(global.localStorage.setItem).toHaveBeenCalledWith(THEME_KEY, 'light');
    expect(result.current.theme).toBe('light');
  });

  it('TC7: should load stored theme preference from localStorage', () => {
    // Pre-set localStorage value
    mockStorage[THEME_KEY] = 'light';

    const { result } = renderHook(() => useTheme());

    // Should load the stored light theme
    expect(result.current.theme).toBe('light');
  });

  it('should remove dark class from document when theme is light', () => {
    // Start with dark class
    document.documentElement.classList.add('dark');

    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.setTheme('light');
    });

    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('should add dark class to document when theme is dark', () => {
    // Start without dark class
    document.documentElement.classList.remove('dark');

    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.setTheme('dark');
    });

    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });
});
