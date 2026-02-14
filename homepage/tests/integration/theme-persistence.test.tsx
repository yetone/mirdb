import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, cleanup } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from '../../src/context/ThemeContext';
import { ThemeToggle } from '../../src/components/common/ThemeToggle';
import { THEME_STORAGE_KEY } from '../../src/utils/constants';

// Test component that shows both the toggle and current theme state
const TestApp: React.FC = () => {
  return (
    <ThemeProvider>
      <div>
        <ThemeToggle />
        <span data-testid="theme-indicator">Theme loaded</span>
      </div>
    </ThemeProvider>
  );
};

describe('Theme Persistence Integration Tests', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('dark', 'light');

    // Mock matchMedia
    vi.stubGlobal('matchMedia', vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })));
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it('should persist dark theme to localStorage after toggling', () => {
    render(<TestApp />);

    const button = screen.getByRole('button');

    // Toggle to dark mode
    fireEvent.click(button);

    // Verify localStorage contains the dark theme
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');
  });

  it('should persist light theme to localStorage after toggling back', () => {
    render(<TestApp />);

    const button = screen.getByRole('button');

    // Toggle to dark
    fireEvent.click(button);
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');

    // Toggle back to light
    fireEvent.click(button);
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('light');
  });

  it('should load dark theme when localStorage has dark theme set', () => {
    // Pre-set localStorage to dark theme before rendering
    localStorage.setItem(THEME_STORAGE_KEY, 'dark');

    render(<TestApp />);

    // Verify dark class is applied to document
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(document.documentElement.classList.contains('light')).toBe(false);
  });

  it('should load light theme when localStorage has light theme set', () => {
    // Pre-set localStorage to light theme before rendering
    localStorage.setItem(THEME_STORAGE_KEY, 'light');

    render(<TestApp />);

    // Verify light class is applied to document
    expect(document.documentElement.classList.contains('light')).toBe(true);
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('should use system preference (dark) when no localStorage theme exists', () => {
    // Override matchMedia to return dark preference
    vi.stubGlobal('matchMedia', vi.fn().mockImplementation((query: string) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })));

    // Clear localStorage to ensure system preference is used
    localStorage.clear();

    render(<TestApp />);

    // Should default to dark theme based on system preference
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('should use system preference (light) when no localStorage theme exists', () => {
    // Clear localStorage to ensure system preference is used
    localStorage.clear();

    // matchMedia is already mocked to return false for dark preference

    render(<TestApp />);

    // Should default to light theme based on system preference
    expect(document.documentElement.classList.contains('light')).toBe(true);
  });

  it('should prefer localStorage over system preference', () => {
    // Set system preference to dark
    vi.stubGlobal('matchMedia', vi.fn().mockImplementation((query: string) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })));

    // But localStorage has light theme
    localStorage.setItem(THEME_STORAGE_KEY, 'light');

    render(<TestApp />);

    // Should use localStorage value (light) over system preference (dark)
    expect(document.documentElement.classList.contains('light')).toBe(true);
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('should maintain theme across re-renders', () => {
    const { rerender } = render(<TestApp />);

    const button = screen.getByRole('button');

    // Toggle to dark
    fireEvent.click(button);
    expect(document.documentElement.classList.contains('dark')).toBe(true);

    // Re-render the component
    rerender(<TestApp />);

    // Theme should still be dark
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('should update document class immediately after theme change', () => {
    render(<TestApp />);

    // Initial state: light theme
    expect(document.documentElement.classList.contains('light')).toBe(true);

    const button = screen.getByRole('button');

    // Click to toggle to dark
    fireEvent.click(button);

    // Document class should immediately update
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(document.documentElement.classList.contains('light')).toBe(false);
  });
});
