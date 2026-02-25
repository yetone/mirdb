import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, cleanup } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeToggle } from '../../src/components/ThemeToggle';

// Use real useTheme implementation for integration tests
vi.unmock('../../src/hooks/useTheme');

describe('Theme Persistence Integration Tests', () => {
  let mockLocalStorage: { [key: string]: string };

  beforeEach(() => {
    // Reset localStorage mock
    mockLocalStorage = {};

    Object.defineProperty(window, 'localStorage', {
      value: {
        getItem: vi.fn((key: string) => mockLocalStorage[key] || null),
        setItem: vi.fn((key: string, value: string) => {
          mockLocalStorage[key] = value;
        }),
        removeItem: vi.fn((key: string) => {
          delete mockLocalStorage[key];
        }),
        clear: vi.fn(() => {
          mockLocalStorage = {};
        }),
      },
      writable: true,
      configurable: true,
    });

    // Default to light system preference
    Object.defineProperty(window, 'matchMedia', {
      value: vi.fn().mockImplementation((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
      writable: true,
      configurable: true,
    });

    // Clear any existing data-theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
    document.documentElement.removeAttribute('data-theme');
  });

  // Test Case 9: Click theme toggle then refresh page - Theme preference persists after page reload
  describe('Test Case 9: Theme persists after page reload', () => {
    it('theme preference persists in localStorage after toggle', async () => {
      const user = userEvent.setup();

      render(<ThemeToggle />);

      // Verify initial state (light mode)
      expect(screen.getByTestId('moon-icon')).toBeInTheDocument();

      // Click to switch to dark mode
      const button = screen.getByRole('button');
      await user.click(button);

      // Verify localStorage was updated
      expect(localStorage.setItem).toHaveBeenCalledWith('mirdb-theme-preference', 'dark');
      expect(mockLocalStorage['mirdb-theme-preference']).toBe('dark');
    });

    it('component initializes with persisted theme on remount', async () => {
      const user = userEvent.setup();

      // First render - toggle to dark
      const { unmount } = render(<ThemeToggle />);

      const button = screen.getByRole('button');
      await user.click(button);

      // Verify we're in dark mode
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
      expect(mockLocalStorage['mirdb-theme-preference']).toBe('dark');

      // Unmount (simulate page leaving)
      unmount();

      // Re-render (simulate page reload)
      render(<ThemeToggle />);

      // Should initialize with dark theme from localStorage
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
    });

    it('multiple toggles correctly update localStorage', async () => {
      const user = userEvent.setup();

      render(<ThemeToggle />);

      const button = screen.getByRole('button');

      // Toggle to dark
      await user.click(button);
      expect(mockLocalStorage['mirdb-theme-preference']).toBe('dark');

      // Toggle back to light
      await user.click(button);
      expect(mockLocalStorage['mirdb-theme-preference']).toBe('light');

      // Toggle to dark again
      await user.click(button);
      expect(mockLocalStorage['mirdb-theme-preference']).toBe('dark');
    });
  });

  // Test Case 10: Toggle theme - Body element receives appropriate theme class (dark/light)
  describe('Test Case 10: Document element receives data-theme attribute', () => {
    it('document element has data-theme light initially', () => {
      render(<ThemeToggle />);

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('document element updates to dark after toggle', async () => {
      const user = userEvent.setup();

      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      await user.click(button);

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('document element updates to light after toggling back', async () => {
      const user = userEvent.setup();

      render(<ThemeToggle />);

      const button = screen.getByRole('button');

      // Toggle to dark
      await user.click(button);
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Toggle back to light
      await user.click(button);
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('document element has correct data-theme when localStorage has preference', () => {
      // Pre-set localStorage preference
      mockLocalStorage['mirdb-theme-preference'] = 'dark';

      render(<ThemeToggle />);

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });

  // Additional integration tests
  describe('Theme integration with system preference', () => {
    it('respects system dark preference when no localStorage', () => {
      // Mock system preference to dark
      Object.defineProperty(window, 'matchMedia', {
        value: vi.fn().mockImplementation((query: string) => ({
          matches: query === '(prefers-color-scheme: dark)',
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
        writable: true,
        configurable: true,
      });

      render(<ThemeToggle />);

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
    });

    it('localStorage preference overrides system preference', () => {
      // Mock system preference to dark
      Object.defineProperty(window, 'matchMedia', {
        value: vi.fn().mockImplementation((query: string) => ({
          matches: query === '(prefers-color-scheme: dark)',
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
        writable: true,
        configurable: true,
      });

      // But localStorage has light preference
      mockLocalStorage['mirdb-theme-preference'] = 'light';

      render(<ThemeToggle />);

      // Should use localStorage preference over system preference
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      expect(screen.getByTestId('moon-icon')).toBeInTheDocument();
    });
  });

  describe('Complete user flow', () => {
    it('full toggle cycle: light -> dark -> light with persistence', async () => {
      const user = userEvent.setup();

      // Initial render - should be light (system default)
      const { unmount } = render(<ThemeToggle />);

      expect(screen.getByTestId('moon-icon')).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Toggle to dark
      await user.click(screen.getByRole('button'));

      expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      expect(mockLocalStorage['mirdb-theme-preference']).toBe('dark');

      // Simulate page reload
      unmount();
      document.documentElement.removeAttribute('data-theme');
      render(<ThemeToggle />);

      // Should persist dark mode
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Toggle back to light
      await user.click(screen.getByRole('button'));

      expect(screen.getByTestId('moon-icon')).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      expect(mockLocalStorage['mirdb-theme-preference']).toBe('light');
    });
  });
});
