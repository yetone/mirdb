/**
 * Tests for useTheme hook.
 * Owner: Scenario 11 - Dark Mode Theme Support
 */
import { renderHook, act } from '@testing-library/react';
import { useTheme } from '@/hooks/useTheme';

// Mock localStorage
const mockLocalStorage = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: jest.fn((key: string) => store[key] || null),
    setItem: jest.fn((key: string, value: string) => {
      store[key] = value;
    }),
    removeItem: jest.fn((key: string) => {
      delete store[key];
    }),
    clear: jest.fn(() => {
      store = {};
    }),
  };
})();

// Mock matchMedia
const mockMatchMedia = jest.fn();

describe('useTheme', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockLocalStorage.clear();

    Object.defineProperty(window, 'localStorage', {
      value: mockLocalStorage,
      writable: true,
    });

    Object.defineProperty(window, 'matchMedia', {
      value: mockMatchMedia,
      writable: true,
    });

    mockMatchMedia.mockReturnValue({
      matches: false,
      addEventListener: jest.fn(),
      removeEventListener: jest.fn(),
    });

    document.documentElement.removeAttribute('data-theme');
  });

  it('should initialize with light theme by default', () => {
    const { result } = renderHook(() => useTheme());

    // After mount, should be light theme
    expect(result.current.theme).toBe('light');
  });

  it('should read theme from localStorage if available', () => {
    mockLocalStorage.getItem.mockReturnValueOnce('dark');

    const { result } = renderHook(() => useTheme());

    // Wait for useEffect to run
    expect(mockLocalStorage.getItem).toHaveBeenCalledWith('mirdb-theme');
  });

  it('should respect system preference for dark mode', () => {
    mockMatchMedia.mockReturnValue({
      matches: true, // prefers dark mode
      addEventListener: jest.fn(),
      removeEventListener: jest.fn(),
    });

    renderHook(() => useTheme());

    // System prefers dark, should be applied
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  it('should toggle theme between light and dark', () => {
    const { result } = renderHook(() => useTheme());

    expect(result.current.theme).toBe('light');

    act(() => {
      result.current.toggleTheme();
    });

    expect(result.current.theme).toBe('dark');
    expect(mockLocalStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

    act(() => {
      result.current.toggleTheme();
    });

    expect(result.current.theme).toBe('light');
    expect(mockLocalStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'light');
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });

  it('should set theme directly via setTheme', () => {
    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.setTheme('dark');
    });

    expect(result.current.theme).toBe('dark');
    expect(mockLocalStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  it('should persist theme preference to localStorage', () => {
    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.setTheme('dark');
    });

    expect(mockLocalStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
  });

  it('should apply theme to document element', () => {
    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.setTheme('dark');
    });

    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

    act(() => {
      result.current.setTheme('light');
    });

    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });

  it('should indicate mounted state', () => {
    const { result } = renderHook(() => useTheme());

    // After mounting, mounted should be true
    expect(result.current.mounted).toBe(true);
  });

  it('should use stored preference over system preference', () => {
    mockLocalStorage.getItem.mockReturnValueOnce('light');
    mockMatchMedia.mockReturnValue({
      matches: true, // system prefers dark
      addEventListener: jest.fn(),
      removeEventListener: jest.fn(),
    });

    renderHook(() => useTheme());

    // Stored preference (light) should win over system preference (dark)
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });
});
