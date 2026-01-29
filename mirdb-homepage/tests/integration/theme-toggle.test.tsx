/**
 * Integration tests for ThemeToggle component.
 * Owner: Scenario 11 - Dark Mode Support
 *
 * Tests:
 * - Theme toggle switches between light and dark modes
 * - Theme preference persists in localStorage
 * - Component displays correct icons for each theme
 * - Accessibility attributes are correct
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { render, screen, waitFor, cleanup } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeToggle } from '../../src/components/shared/ThemeToggle';

// Create fresh localStorage mock for each test
function createLocalStorageMock() {
  let store: Record<string, string> = {};
  return {
    getItem: vi.fn((key: string) => store[key] ?? null),
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
    key: vi.fn((index: number) => Object.keys(store)[index] ?? null),
  };
}

// Mock matchMedia
const createMatchMediaMock = (matches: boolean) =>
  vi.fn().mockImplementation((query: string) => ({
    matches,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));

describe('ThemeToggle', () => {
  let localStorageMock: ReturnType<typeof createLocalStorageMock>;

  beforeEach(() => {
    // Reset all mocks
    vi.clearAllMocks();

    // Create fresh localStorage mock
    localStorageMock = createLocalStorageMock();
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
      configurable: true,
    });

    // Default to dark mode preference
    Object.defineProperty(window, 'matchMedia', {
      value: createMatchMediaMock(true), // prefers-color-scheme: dark
      writable: true,
      configurable: true,
    });

    // Reset document class list
    document.documentElement.classList.remove('dark', 'light');
    document.documentElement.classList.add('dark');
  });

  afterEach(() => {
    cleanup();
    document.documentElement.classList.remove('dark', 'light');
    vi.clearAllMocks();
  });

  describe('Initial rendering', () => {
    it('renders theme toggle button', async () => {
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      });
    });

    it('shows sun icon in dark mode (to switch to light)', async () => {
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
      });
    });

    it('has correct aria-label in dark mode', async () => {
      render(<ThemeToggle />);

      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
      });
    });

    it('has data-theme attribute set to dark by default', async () => {
      render(<ThemeToggle />);

      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        expect(button).toHaveAttribute('data-theme', 'dark');
      });
    });
  });

  describe('Theme toggling', () => {
    it('switches to light mode when clicked in dark mode', async () => {
      const user = userEvent.setup();
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      });

      const button = screen.getByTestId('theme-toggle');
      await user.click(button);

      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'light');
        expect(screen.getByTestId('moon-icon')).toBeInTheDocument();
      });
    });

    it('switches back to dark mode when clicked twice', async () => {
      const user = userEvent.setup();
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      });

      const button = screen.getByTestId('theme-toggle');
      await user.click(button);

      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'light');
      });

      await user.click(button);

      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'dark');
        expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
      });
    });

    it('updates aria-label when switching to light mode', async () => {
      const user = userEvent.setup();
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      });

      const button = screen.getByTestId('theme-toggle');
      await user.click(button);

      await waitFor(() => {
        expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');
      });
    });
  });

  describe('localStorage persistence', () => {
    it('saves theme preference to localStorage when toggled', async () => {
      const user = userEvent.setup();
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      });

      const button = screen.getByTestId('theme-toggle');
      await user.click(button);

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'light');
      });
    });

    it('reads stored preference from localStorage on mount', async () => {
      // Pre-set localStorage before rendering
      localStorageMock.setItem('mirdb-theme', 'light');
      // Reset the mock to clear the call count but keep the stored value
      localStorageMock.setItem.mockClear();

      render(<ThemeToggle />);

      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        expect(button).toHaveAttribute('data-theme', 'light');
      });
    });

    it('saves dark preference when switching back', async () => {
      const user = userEvent.setup();
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      });

      const button = screen.getByTestId('theme-toggle');

      // First click - to light
      await user.click(button);
      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'light');
      });

      // Second click - back to dark
      await user.click(button);
      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'dark');
        expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
      });
    });
  });

  describe('System preference respect', () => {
    it('defaults to dark mode when system prefers dark', async () => {
      Object.defineProperty(window, 'matchMedia', {
        value: createMatchMediaMock(true),
        writable: true,
        configurable: true,
      });

      render(<ThemeToggle />);

      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        expect(button).toHaveAttribute('data-theme', 'dark');
      });
    });

    it('respects light system preference when no stored preference', async () => {
      Object.defineProperty(window, 'matchMedia', {
        value: createMatchMediaMock(false), // prefers-color-scheme: light
        writable: true,
        configurable: true,
      });

      render(<ThemeToggle />);

      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        expect(button).toHaveAttribute('data-theme', 'light');
      });
    });

    it('stored preference overrides system preference', async () => {
      // Pre-set localStorage to light
      localStorageMock.setItem('mirdb-theme', 'light');

      // System prefers dark
      Object.defineProperty(window, 'matchMedia', {
        value: createMatchMediaMock(true),
        writable: true,
        configurable: true,
      });

      render(<ThemeToggle />);

      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        // Stored preference should win over system preference
        expect(button).toHaveAttribute('data-theme', 'light');
      });
    });
  });

  describe('DOM class manipulation', () => {
    it('adds dark class to document root in dark mode', async () => {
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(document.documentElement.classList.contains('dark')).toBe(true);
      });
    });

    it('adds light class and removes dark class when switching to light', async () => {
      const user = userEvent.setup();
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      });

      await user.click(screen.getByTestId('theme-toggle'));

      await waitFor(() => {
        expect(document.documentElement.classList.contains('light')).toBe(true);
        expect(document.documentElement.classList.contains('dark')).toBe(false);
      });
    });
  });

  describe('Accessibility', () => {
    it('is keyboard accessible', async () => {
      const user = userEvent.setup();
      render(<ThemeToggle />);

      await waitFor(() => {
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      });

      const button = screen.getByTestId('theme-toggle');
      button.focus();
      expect(document.activeElement).toBe(button);

      await user.keyboard('{Enter}');

      await waitFor(() => {
        expect(button).toHaveAttribute('data-theme', 'light');
      });
    });

    it('has type="button" attribute', async () => {
      render(<ThemeToggle />);

      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        expect(button).toHaveAttribute('type', 'button');
      });
    });

    it('icons have aria-hidden attribute', async () => {
      // Ensure we start in dark mode
      document.documentElement.classList.remove('dark', 'light');
      document.documentElement.classList.add('dark');

      render(<ThemeToggle />);

      // Wait for component to mount and show the sun icon (in dark mode)
      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        expect(button).toHaveAttribute('data-theme', 'dark');
      });

      const icon = screen.getByTestId('sun-icon');
      expect(icon).toHaveAttribute('aria-hidden', 'true');
    });
  });

  describe('Custom className', () => {
    it('applies custom className to button', async () => {
      render(<ThemeToggle className="my-custom-class" />);

      await waitFor(() => {
        const button = screen.getByTestId('theme-toggle');
        expect(button).toHaveClass('my-custom-class');
      });
    });
  });
});
