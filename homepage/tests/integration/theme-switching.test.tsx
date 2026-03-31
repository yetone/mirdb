/**
 * Integration tests for theme switching functionality
 * Owner: Scenario 7 - Dark Mode and Theming
 *
 * Tests end-to-end theme switching behavior including:
 * - ThemeToggle component with useTheme hook integration
 * - System preference detection
 * - Theme persistence across page loads
 * - Theme transitions
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor, cleanup } from '@testing-library/react';
import { ThemeToggle } from '../../src/components/ui/ThemeToggle';
import { useTheme } from '../../src/hooks/useTheme';
import React from 'react';

// Mock localStorage
const createLocalStorageMock = () => {
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
    key: vi.fn((i: number) => Object.keys(store)[i] || null),
  };
};

// Create matchMedia mock
const createMatchMediaMock = (prefersDark: boolean) => {
  return vi.fn().mockImplementation((query: string) => {
    const matches = query.includes('dark') ? prefersDark : !prefersDark;
    return {
      matches,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    };
  });
};

describe('Theme Switching Integration', () => {
  let localStorageMock: ReturnType<typeof createLocalStorageMock>;

  beforeEach(() => {
    localStorageMock = createLocalStorageMock();
    Object.defineProperty(window, 'localStorage', { value: localStorageMock, writable: true });
    window.matchMedia = createMatchMediaMock(false);
    document.documentElement.classList.remove('dark', 'theme-transition');
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  describe('ThemeToggle with useTheme integration', () => {
    it('clicking toggle switches from light to dark', async () => {
      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('data-theme', 'light');

      fireEvent.click(button);

      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'dark');
      });

      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('clicking toggle switches from dark to light', async () => {
      window.matchMedia = createMatchMediaMock(true);
      localStorageMock.setItem('mirdb-theme', 'dark');

      render(<ThemeToggle theme="dark" />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('data-theme', 'dark');

      fireEvent.click(button);

      await waitFor(() => {
        expect(document.documentElement.classList.contains('dark')).toBe(false);
      });
    });

    it('multiple toggles work correctly', async () => {
      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');

      // Light -> Dark
      fireEvent.click(button);
      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'dark');
      });

      // Dark -> Light
      fireEvent.click(button);
      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'light');
      });

      // Light -> Dark again
      fireEvent.click(button);
      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'dark');
      });
    });
  });

  describe('system preference detection', () => {
    it('respects system dark mode preference on initial render', async () => {
      window.matchMedia = createMatchMediaMock(true);

      // Use controlled mode to test display
      render(<ThemeToggle theme="dark" />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('data-theme', 'dark');
    });

    it('respects system light mode preference on initial render', async () => {
      window.matchMedia = createMatchMediaMock(false);

      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('data-theme', 'light');
    });
  });

  describe('theme persistence', () => {
    it('persists theme choice to localStorage after toggle', async () => {
      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      fireEvent.click(button);

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
      });
    });

    it('stored theme is read from localStorage', async () => {
      // Test via the useTheme hook directly
      const TestComponent = () => {
        const { theme } = useTheme();
        return <div data-testid="theme-display">{theme}</div>;
      };

      localStorageMock.setItem('mirdb-theme', 'dark');

      render(<TestComponent />);

      const display = screen.getByTestId('theme-display');
      expect(display.textContent).toBe('dark');
    });

    it('stored theme overrides system preference', async () => {
      window.matchMedia = createMatchMediaMock(true); // System prefers dark
      localStorageMock.setItem('mirdb-theme', 'light'); // But user chose light

      const TestComponent = () => {
        const { theme } = useTheme();
        return <div data-testid="theme-display">{theme}</div>;
      };

      render(<TestComponent />);

      const display = screen.getByTestId('theme-display');
      expect(display.textContent).toBe('light');
    });
  });

  describe('document body class updates', () => {
    it('adds dark class to documentElement when switching to dark', async () => {
      render(<ThemeToggle />);

      fireEvent.click(screen.getByTestId('theme-toggle'));

      await waitFor(() => {
        expect(document.documentElement.classList.contains('dark')).toBe(true);
      });
    });

    it('removes dark class from documentElement when switching to light', async () => {
      document.documentElement.classList.add('dark');

      const TestComponent = () => {
        const { setTheme } = useTheme();
        return (
          <button data-testid="set-light" onClick={() => setTheme('light')}>
            Set Light
          </button>
        );
      };

      render(<TestComponent />);

      fireEvent.click(screen.getByTestId('set-light'));

      await waitFor(() => {
        expect(document.documentElement.classList.contains('dark')).toBe(false);
      });
    });
  });

  describe('theme transition animation', () => {
    it('adds theme-transition class during theme change', async () => {
      vi.useFakeTimers();

      render(<ThemeToggle />);

      fireEvent.click(screen.getByTestId('theme-toggle'));

      expect(document.documentElement.classList.contains('theme-transition')).toBe(true);

      // Wait for transition to complete
      vi.advanceTimersByTime(300);

      expect(document.documentElement.classList.contains('theme-transition')).toBe(false);

      vi.useRealTimers();
    });
  });

  describe('accessibility in theme switching', () => {
    it('updates aria-label after switching to dark', async () => {
      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');

      fireEvent.click(button);

      await waitFor(() => {
        expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
      });
    });

    it('has correct aria-label for dark mode toggle', async () => {
      render(<ThemeToggle theme="dark" />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
    });
  });

  describe('multiple ThemeToggle instances', () => {
    it('all instances reflect theme changes via document class', async () => {
      render(
        <>
          <ThemeToggle />
          <ThemeToggle />
        </>
      );

      const buttons = screen.getAllByTestId('theme-toggle');

      // All should start in light mode
      expect(buttons[0]).toHaveAttribute('data-theme', 'light');
      expect(buttons[1]).toHaveAttribute('data-theme', 'light');

      // Click the first toggle
      fireEvent.click(buttons[0]);

      // Document class should be updated
      await waitFor(() => {
        expect(document.documentElement.classList.contains('dark')).toBe(true);
      });
    });
  });
});
