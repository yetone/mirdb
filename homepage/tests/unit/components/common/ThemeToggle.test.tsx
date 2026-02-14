import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';
import { ThemeToggle } from '../../../../src/components/common/ThemeToggle';
import { ThemeProvider } from '../../../../src/context/ThemeContext';
import { THEME_STORAGE_KEY } from '../../../../src/utils/constants';

// Wrapper component for providing theme context
const renderWithThemeProvider = (ui: React.ReactElement) => {
  return render(
    <ThemeProvider>
      {ui}
    </ThemeProvider>
  );
};

describe('ThemeToggle component', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('dark', 'light');

    // Mock matchMedia to return false for dark preference by default
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
    vi.unstubAllGlobals();
  });

  it('should render a button element', () => {
    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');
    expect(button).toBeInTheDocument();
  });

  it('should display moon icon when in light mode', () => {
    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');
    // Moon icon should be displayed in light mode
    const svg = button.querySelector('svg');
    expect(svg).toBeInTheDocument();
    // The moon path has a specific d attribute for the moon icon
    expect(button.querySelector('svg path')?.getAttribute('d')).toContain('21.752');
  });

  it('should display sun icon when in dark mode', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark');

    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');
    // Sun icon should be displayed in dark mode
    const svg = button.querySelector('svg');
    expect(svg).toBeInTheDocument();
    // The sun path starts with M12 3v2.25
    expect(button.querySelector('svg path')?.getAttribute('d')).toContain('M12 3v2.25');
  });

  it('should toggle theme from light to dark on click', () => {
    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');

    // Initially in light mode, should show moon icon
    expect(button.querySelector('svg path')?.getAttribute('d')).toContain('21.752');

    fireEvent.click(button);

    // After click, should be in dark mode and show sun icon
    expect(button.querySelector('svg path')?.getAttribute('d')).toContain('M12 3v2.25');
  });

  it('should toggle theme from dark to light on click', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark');

    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');

    // Initially in dark mode, should show sun icon
    expect(button.querySelector('svg path')?.getAttribute('d')).toContain('M12 3v2.25');

    fireEvent.click(button);

    // After click, should be in light mode and show moon icon
    expect(button.querySelector('svg path')?.getAttribute('d')).toContain('21.752');
  });

  it('should have aria-label for accessibility in light mode', () => {
    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');
    expect(button).toHaveAttribute('aria-label', 'Switch to dark theme');
  });

  it('should have aria-label for accessibility in dark mode', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark');

    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');
    expect(button).toHaveAttribute('aria-label', 'Switch to light theme');
  });

  it('should update aria-label after toggle', () => {
    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');
    expect(button).toHaveAttribute('aria-label', 'Switch to dark theme');

    fireEvent.click(button);

    expect(button).toHaveAttribute('aria-label', 'Switch to light theme');
  });

  it('should save theme to localStorage on toggle', () => {
    renderWithThemeProvider(<ThemeToggle />);

    const button = screen.getByRole('button');

    fireEvent.click(button);

    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark');

    fireEvent.click(button);

    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('light');
  });

  it('should apply dark class to document when dark theme is set', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark');

    renderWithThemeProvider(<ThemeToggle />);

    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('should apply light class to document when light theme is set', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'light');

    renderWithThemeProvider(<ThemeToggle />);

    expect(document.documentElement.classList.contains('light')).toBe(true);
  });
});
