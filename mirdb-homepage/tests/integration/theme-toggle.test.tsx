/**
 * Integration tests for ThemeToggle component.
 * Owner: Scenario 11 - Dark Mode Support
 *
 * Tests:
 * - Theme toggle switches between light and dark modes
 * - Theme selection persists across page reloads via localStorage
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import ThemeToggle from '../../src/components/shared/ThemeToggle';

describe('ThemeToggle Integration', () => {
  let originalMatchMedia: typeof window.matchMedia;

  beforeEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('dark', 'light');
    document.documentElement.classList.add('dark');

    originalMatchMedia = window.matchMedia;
    window.matchMedia = vi.fn().mockImplementation((query: string) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));
  });

  afterEach(() => {
    window.matchMedia = originalMatchMedia;
    localStorage.clear();
    document.documentElement.classList.remove('dark', 'light');
  });

  it('should switch theme from dark to light when toggle is clicked', async () => {
    render(<ThemeToggle />);

    await waitFor(() => {
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    });

    const toggle = screen.getByTestId('theme-toggle');
    expect(toggle).toHaveAttribute('data-theme', 'dark');
    expect(document.documentElement.classList.contains('dark')).toBe(true);

    fireEvent.click(toggle);

    await waitFor(() => {
      expect(toggle).toHaveAttribute('data-theme', 'light');
    });
    expect(document.documentElement.classList.contains('light')).toBe(true);
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('should switch theme from light to dark when toggle is clicked', async () => {
    document.documentElement.classList.remove('dark');
    document.documentElement.classList.add('light');
    localStorage.setItem('mirdb-theme', 'light');

    render(<ThemeToggle />);

    await waitFor(() => {
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    });

    const toggle = screen.getByTestId('theme-toggle');

    await waitFor(() => {
      expect(toggle).toHaveAttribute('data-theme', 'light');
    });

    fireEvent.click(toggle);

    await waitFor(() => {
      expect(toggle).toHaveAttribute('data-theme', 'dark');
    });
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(document.documentElement.classList.contains('light')).toBe(false);
  });

  it('should persist theme selection in localStorage', async () => {
    render(<ThemeToggle />);

    await waitFor(() => {
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    });

    const toggle = screen.getByTestId('theme-toggle');

    expect(localStorage.getItem('mirdb-theme')).toBe(null);

    fireEvent.click(toggle);

    await waitFor(() => {
      expect(localStorage.getItem('mirdb-theme')).toBe('light');
    });

    fireEvent.click(toggle);

    await waitFor(() => {
      expect(localStorage.getItem('mirdb-theme')).toBe('dark');
    });
  });

  it('should restore theme from localStorage on mount', async () => {
    localStorage.setItem('mirdb-theme', 'light');
    document.documentElement.classList.remove('dark');
    document.documentElement.classList.add('light');

    render(<ThemeToggle />);

    await waitFor(() => {
      const toggle = screen.getByTestId('theme-toggle');
      expect(toggle).toHaveAttribute('data-theme', 'light');
    });

    expect(document.documentElement.classList.contains('light')).toBe(true);
  });

  it('should have accessible button with aria-label', async () => {
    render(<ThemeToggle />);

    await waitFor(() => {
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    });

    const toggle = screen.getByTestId('theme-toggle');
    expect(toggle).toHaveAttribute('aria-label', 'Switch to light mode');

    fireEvent.click(toggle);

    await waitFor(() => {
      expect(toggle).toHaveAttribute('aria-label', 'Switch to dark mode');
    });
  });
});
