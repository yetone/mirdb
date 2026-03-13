/**
 * Dark Mode Integration Tests
 * Owner: Scenario 8 - Dark Mode Toggle
 *
 * Integration tests for:
 * - Theme context state management
 * - localStorage persistence
 * - Theme class application to document
 * - Initial load with stored preference
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import { ThemeProvider, useThemeContext } from '../../src/context/ThemeContext';
import { ThemeToggle } from '../../src/components/ui/ThemeToggle';

// Test component that displays current theme
function ThemeDisplay() {
  const { theme } = useThemeContext();
  return (
    <div data-testid="theme-display" data-theme={theme}>
      Current theme: {theme}
    </div>
  );
}

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value;
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key];
    }),
    clear: vi.fn(() => {
      store = {};
    }),
    get length() {
      return Object.keys(store).length;
    },
    key: vi.fn((index: number) => Object.keys(store)[index] || null),
  };
})();

// Mock matchMedia
const matchMediaMock = vi.fn().mockImplementation((query: string) => ({
  matches: query === '(prefers-color-scheme: dark)' ? false : false,
  media: query,
  onchange: null,
  addListener: vi.fn(),
  removeListener: vi.fn(),
  addEventListener: vi.fn(),
  removeEventListener: vi.fn(),
  dispatchEvent: vi.fn(),
}));

describe('Dark Mode Integration', () => {
  beforeEach(() => {
    // Setup mocks
    Object.defineProperty(window, 'localStorage', { value: localStorageMock });
    Object.defineProperty(window, 'matchMedia', { value: matchMediaMock, writable: true });
    localStorageMock.clear();
    document.documentElement.classList.remove('dark');
    vi.clearAllMocks();
  });

  afterEach(() => {
    document.documentElement.classList.remove('dark');
  });

  // Test Case 2: Click theme toggle when in light mode - switches to dark mode
  describe('Theme Toggle Behavior', () => {
    it('switches to dark mode when toggle is clicked in light mode', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle />
          <ThemeDisplay />
        </ThemeProvider>
      );

      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-theme', 'light');

      const toggleButton = screen.getByTestId('theme-toggle-button');
      fireEvent.click(toggleButton);

      expect(themeDisplay).toHaveAttribute('data-theme', 'dark');
    });

    // Test Case 3: Click theme toggle when in dark mode - switches to light mode
    it('switches to light mode when toggle is clicked in dark mode', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <ThemeToggle />
          <ThemeDisplay />
        </ThemeProvider>
      );

      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-theme', 'dark');

      const toggleButton = screen.getByTestId('theme-toggle-button');
      fireEvent.click(toggleButton);

      expect(themeDisplay).toHaveAttribute('data-theme', 'light');
    });
  });

  // Test Case 4: localStorage persistence
  describe('localStorage Persistence', () => {
    it('saves theme preference to localStorage when toggled to dark mode', async () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle />
          <ThemeDisplay />
        </ThemeProvider>
      );

      const toggleButton = screen.getByTestId('theme-toggle-button');

      await act(async () => {
        fireEvent.click(toggleButton);
      });

      expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
    });

    it('saves theme preference to localStorage when toggled to light mode', async () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <ThemeToggle />
          <ThemeDisplay />
        </ThemeProvider>
      );

      const toggleButton = screen.getByTestId('theme-toggle-button');

      await act(async () => {
        fireEvent.click(toggleButton);
      });

      expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'light');
    });
  });

  // Test Case 5: Page load with localStorage theme='dark'
  describe('Initial Theme Load', () => {
    it('renders in dark mode when localStorage has theme="dark"', () => {
      // Pre-set localStorage value
      localStorageMock.setItem('mirdb-theme', 'dark');
      localStorageMock.getItem.mockImplementation((key: string) => {
        if (key === 'mirdb-theme') return 'dark';
        return null;
      });

      render(
        <ThemeProvider>
          <ThemeDisplay />
        </ThemeProvider>
      );

      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-theme', 'dark');
    });

    it('renders in light mode when localStorage has theme="light"', () => {
      localStorageMock.getItem.mockImplementation((key: string) => {
        if (key === 'mirdb-theme') return 'light';
        return null;
      });

      render(
        <ThemeProvider>
          <ThemeDisplay />
        </ThemeProvider>
      );

      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-theme', 'light');
    });

    it('uses defaultTheme prop when provided', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <ThemeDisplay />
        </ThemeProvider>
      );

      const themeDisplay = screen.getByTestId('theme-display');
      expect(themeDisplay).toHaveAttribute('data-theme', 'dark');
    });
  });

  // Test Case: Document class application
  describe('Document Class Application', () => {
    it('adds "dark" class to document element when theme is dark', async () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <ThemeDisplay />
        </ThemeProvider>
      );

      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('removes "dark" class from document element when theme is light', () => {
      document.documentElement.classList.add('dark');

      render(
        <ThemeProvider defaultTheme="light">
          <ThemeDisplay />
        </ThemeProvider>
      );

      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });

    it('toggles document class when theme changes', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle />
        </ThemeProvider>
      );

      expect(document.documentElement.classList.contains('dark')).toBe(false);

      const toggleButton = screen.getByTestId('theme-toggle-button');
      fireEvent.click(toggleButton);

      expect(document.documentElement.classList.contains('dark')).toBe(true);

      fireEvent.click(toggleButton);

      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });
  });

  // Test Case: Context error handling
  describe('Context Error Handling', () => {
    it('throws error when useThemeContext is used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const originalError = console.error;
      console.error = vi.fn();

      expect(() => {
        render(<ThemeDisplay />);
      }).toThrow('useThemeContext must be used within a ThemeProvider');

      console.error = originalError;
    });
  });

  // Test Case: Multiple toggle operations
  describe('Multiple Toggle Operations', () => {
    it('handles multiple rapid toggles correctly', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle />
          <ThemeDisplay />
        </ThemeProvider>
      );

      const toggleButton = screen.getByTestId('theme-toggle-button');
      const themeDisplay = screen.getByTestId('theme-display');

      // Toggle multiple times
      fireEvent.click(toggleButton);
      expect(themeDisplay).toHaveAttribute('data-theme', 'dark');

      fireEvent.click(toggleButton);
      expect(themeDisplay).toHaveAttribute('data-theme', 'light');

      fireEvent.click(toggleButton);
      expect(themeDisplay).toHaveAttribute('data-theme', 'dark');

      fireEvent.click(toggleButton);
      expect(themeDisplay).toHaveAttribute('data-theme', 'light');
    });
  });
});
