/**
 * Theme Integration Tests
 * Owner: Scenario 6 - Theme Support
 *
 * Tests for dark mode and light mode theme consistency across the landing page.
 * Validates theme context integration, visual consistency, and theme switching.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeToggle } from '../../src/components/ThemeToggle';
import { Navbar } from '../../src/components/Navbar';

// Helper to get computed styles for theme validation
const getComputedTheme = () => {
  return document.documentElement.getAttribute('data-theme');
};

// Helper to render components with router context
const renderWithRouter = (ui: React.ReactElement) => {
  return render(
    <BrowserRouter>
      {ui}
    </BrowserRouter>
  );
};

describe('Theme Integration', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
    // Reset document attribute
    document.documentElement.removeAttribute('data-theme');
    // Reset matchMedia mock to default (light preference)
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: light)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    });
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Light Theme Rendering', () => {
    it('renders landing page with light theme colors when ThemeContext is set to light', async () => {
      // Set up light theme preference
      localStorage.setItem('theme', 'light');

      renderWithRouter(<Navbar />);

      // Wait for theme to be applied
      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });

      // Verify navbar is visible with light theme
      const navbar = screen.getByTestId('navbar');
      expect(navbar).toBeInTheDocument();

      // Verify theme toggle shows moon icon (indicating light mode, click for dark)
      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark theme');
    });

    it('applies light theme to document element', async () => {
      localStorage.setItem('theme', 'light');

      renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });
    });

    it('uses light theme by default when no preference is stored', async () => {
      // No localStorage value set, matchMedia returns light preference
      renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });
    });

    it('displays light theme background classes on navbar', async () => {
      localStorage.setItem('theme', 'light');

      renderWithRouter(<Navbar />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });

      const navbar = screen.getByTestId('navbar');
      expect(navbar).toHaveClass('bg-base-100/80');
    });
  });

  describe('Test Case 2: Dark Theme Rendering', () => {
    it('renders landing page with dark theme colors when ThemeContext is set to dark', async () => {
      // Set up dark theme preference
      localStorage.setItem('theme', 'dark');

      renderWithRouter(<Navbar />);

      // Wait for theme to be applied
      await waitFor(() => {
        expect(getComputedTheme()).toBe('dark');
      });

      // Verify navbar is visible with dark theme
      const navbar = screen.getByTestId('navbar');
      expect(navbar).toBeInTheDocument();

      // Verify theme toggle shows sun icon (indicating dark mode, click for light)
      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light theme');
    });

    it('applies dark theme to document element', async () => {
      localStorage.setItem('theme', 'dark');

      renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });

    it('uses system dark preference when no localStorage value exists', async () => {
      // Mock system preference as dark
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
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
      });

      renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('dark');
      });
    });

    it('displays dark theme background classes on navbar', async () => {
      localStorage.setItem('theme', 'dark');

      renderWithRouter(<Navbar />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('dark');
      });

      const navbar = screen.getByTestId('navbar');
      // DaisyUI applies theme via data-theme attribute, base-100 adapts
      expect(navbar).toHaveClass('bg-base-100/80');
    });
  });

  describe('Test Case 4: ThemeToggle reflects current theme state', () => {
    it('ThemeToggle button reflects current dark theme state', async () => {
      localStorage.setItem('theme', 'dark');

      renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('dark');
      });

      const themeToggle = screen.getByTestId('theme-toggle');
      // In dark mode, the button should show sun icon and label to switch to light
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light theme');
    });

    it('ThemeToggle button reflects current light theme state', async () => {
      localStorage.setItem('theme', 'light');

      renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });

      const themeToggle = screen.getByTestId('theme-toggle');
      // In light mode, the button should show moon icon and label to switch to dark
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark theme');
    });

    it('ThemeToggle icon changes based on current theme', async () => {
      localStorage.setItem('theme', 'light');

      const { rerender } = renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });

      // Click to switch to dark
      const themeToggle = screen.getByTestId('theme-toggle');
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('dark');
      });

      // Verify aria-label changed
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light theme');
    });
  });

  describe('Theme switching without page reload', () => {
    it('switches theme immediately on toggle click without reload', async () => {
      localStorage.setItem('theme', 'light');

      renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });

      const themeToggle = screen.getByTestId('theme-toggle');

      // Click to switch to dark
      fireEvent.click(themeToggle);

      // Theme should change immediately without any reload
      await waitFor(() => {
        expect(getComputedTheme()).toBe('dark');
      });

      // Click again to switch back to light
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });
    });

    it('updates localStorage when theme is toggled', async () => {
      localStorage.setItem('theme', 'light');

      renderWithRouter(<ThemeToggle />);

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light');
      });

      const themeToggle = screen.getByTestId('theme-toggle');
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('dark');
      });
    });
  });

  describe('Theme persistence', () => {
    it('persists theme preference in localStorage', async () => {
      renderWithRouter(<ThemeToggle />);

      const themeToggle = screen.getByTestId('theme-toggle');

      // Start with light (default)
      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });

      // Switch to dark
      fireEvent.click(themeToggle);

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('dark');
      });

      // Verify the value persists
      expect(localStorage.getItem('theme')).toBe('dark');
    });

    it('loads persisted theme on mount', async () => {
      // Set theme before component mounts
      localStorage.setItem('theme', 'dark');

      renderWithRouter(<ThemeToggle />);

      // Should load the persisted dark theme
      await waitFor(() => {
        expect(getComputedTheme()).toBe('dark');
      });
    });
  });

  describe('Navbar theme integration', () => {
    it('Navbar displays correctly in light theme', async () => {
      localStorage.setItem('theme', 'light');

      renderWithRouter(<Navbar />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });

      // All navbar elements should be visible
      expect(screen.getByTestId('navbar')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-logo')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register')).toBeInTheDocument();
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    });

    it('Navbar displays correctly in dark theme', async () => {
      localStorage.setItem('theme', 'dark');

      renderWithRouter(<Navbar />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('dark');
      });

      // All navbar elements should be visible
      expect(screen.getByTestId('navbar')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-logo')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-login')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register')).toBeInTheDocument();
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    });

    it('Theme toggle in navbar switches theme for entire page', async () => {
      localStorage.setItem('theme', 'light');

      renderWithRouter(<Navbar />);

      await waitFor(() => {
        expect(getComputedTheme()).toBe('light');
      });

      const themeToggle = screen.getByTestId('theme-toggle');
      fireEvent.click(themeToggle);

      // Document theme attribute should change
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });
  });
});
