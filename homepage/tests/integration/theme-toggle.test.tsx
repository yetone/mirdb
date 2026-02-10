/**
 * Integration tests for theme toggle functionality
 * Owner: Scenario 6 - Dark/Light Theme Toggle
 *
 * Tests the complete theme toggle flow including:
 * - Component rendering
 * - Theme switching
 * - localStorage persistence
 * - Document attribute updates
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeToggle } from '../../src/components/ui/ThemeToggle';
import { ThemeProvider } from '../../src/context/ThemeContext';

// Integration test wrapper that mimics the app structure
const TestApp: React.FC<{ defaultTheme?: 'light' | 'dark' }> = ({
  defaultTheme = 'light',
}) => {
  return (
    <ThemeProvider defaultTheme={defaultTheme}>
      <div data-testid="app-container">
        <header data-testid="header">
          <ThemeToggle />
        </header>
        <main data-testid="main-content">
          <h1>Test Content</h1>
          <p>Some content that should respond to theme changes</p>
        </main>
      </div>
    </ThemeProvider>
  );
};

describe('Theme Toggle Integration', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 2: Click theme toggle in light mode', () => {
    it('Theme switches to dark mode with updated styles', async () => {
      render(<TestApp defaultTheme="light" />);

      const toggle = screen.getByTestId('theme-toggle');

      // Verify initial light mode
      expect(toggle).toHaveAttribute('data-theme', 'light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Click to switch to dark mode
      fireEvent.click(toggle);

      // Verify dark mode is active
      await waitFor(() => {
        expect(toggle).toHaveAttribute('data-theme', 'dark');
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });

      // Verify icon changed (sun icon shows in dark mode)
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Click theme toggle - localStorage update', () => {
    it('localStorage theme key is updated to reflect new preference', async () => {
      render(<TestApp defaultTheme="light" />);

      const toggle = screen.getByTestId('theme-toggle');

      // Verify localStorage is initially light (or will be set after render)
      fireEvent.click(toggle);

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('dark');
      });

      // Toggle back
      fireEvent.click(toggle);

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light');
      });
    });
  });

  describe('Test Case 4: Load page with localStorage theme=dark', () => {
    it('Page loads in dark mode when localStorage has dark theme', () => {
      // Set localStorage before rendering
      localStorage.setItem('theme', 'dark');

      // Render without default theme to use localStorage
      render(
        <ThemeProvider>
          <ThemeToggle />
        </ThemeProvider>
      );

      const toggle = screen.getByTestId('theme-toggle');

      // Verify dark mode is loaded from localStorage
      expect(toggle).toHaveAttribute('data-theme', 'dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
    });
  });

  describe('Theme Transition and Consistency', () => {
    it('document theme attribute updates correctly', async () => {
      render(<TestApp defaultTheme="light" />);

      const toggle = screen.getByTestId('theme-toggle');

      // Initial state
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Toggle to dark
      fireEvent.click(toggle);
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });

      // Toggle back to light
      fireEvent.click(toggle);
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });
    });

    it('multiple rapid toggles work correctly without flickering', async () => {
      render(<TestApp defaultTheme="light" />);

      const toggle = screen.getByTestId('theme-toggle');

      // Rapidly toggle multiple times
      fireEvent.click(toggle); // dark
      fireEvent.click(toggle); // light
      fireEvent.click(toggle); // dark
      fireEvent.click(toggle); // light
      fireEvent.click(toggle); // dark

      // Final state should be dark
      await waitFor(() => {
        expect(toggle).toHaveAttribute('data-theme', 'dark');
        expect(localStorage.getItem('theme')).toBe('dark');
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });

    it('aria attributes update consistently with theme', async () => {
      render(<TestApp defaultTheme="light" />);

      const toggle = screen.getByTestId('theme-toggle');

      // Initial state
      expect(toggle).toHaveAttribute('aria-label', 'Switch to dark mode');
      expect(toggle).toHaveAttribute('aria-pressed', 'false');

      // Toggle to dark
      fireEvent.click(toggle);

      await waitFor(() => {
        expect(toggle).toHaveAttribute('aria-label', 'Switch to light mode');
        expect(toggle).toHaveAttribute('aria-pressed', 'true');
      });
    });
  });

  describe('System Preference Detection', () => {
    it('respects system dark mode preference when no localStorage', () => {
      // Mock matchMedia to return dark preference
      const mockMatchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      const originalMatchMedia = window.matchMedia;
      window.matchMedia = mockMatchMedia;

      // Make sure localStorage is clear
      localStorage.clear();

      render(
        <ThemeProvider>
          <ThemeToggle />
        </ThemeProvider>
      );

      const toggle = screen.getByTestId('theme-toggle');

      // Should detect dark mode from system preference
      expect(toggle).toHaveAttribute('data-theme', 'dark');

      // Restore
      window.matchMedia = originalMatchMedia;
    });

    it('localStorage preference takes precedence over system preference', () => {
      // Mock matchMedia to return dark preference
      const mockMatchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      const originalMatchMedia = window.matchMedia;
      window.matchMedia = mockMatchMedia;

      // Set localStorage to light
      localStorage.setItem('theme', 'light');

      render(
        <ThemeProvider>
          <ThemeToggle />
        </ThemeProvider>
      );

      const toggle = screen.getByTestId('theme-toggle');

      // Should use localStorage light preference over system dark preference
      expect(toggle).toHaveAttribute('data-theme', 'light');

      // Restore
      window.matchMedia = originalMatchMedia;
    });
  });

  describe('Component Integration with Layout', () => {
    it('theme toggle works within a layout structure', async () => {
      render(<TestApp defaultTheme="light" />);

      // Verify layout structure
      expect(screen.getByTestId('app-container')).toBeInTheDocument();
      expect(screen.getByTestId('header')).toBeInTheDocument();
      expect(screen.getByTestId('main-content')).toBeInTheDocument();

      // Verify theme toggle is accessible
      const toggle = screen.getByTestId('theme-toggle');
      expect(toggle).toBeInTheDocument();

      // Toggle should work in context
      fireEvent.click(toggle);

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });
  });
});
