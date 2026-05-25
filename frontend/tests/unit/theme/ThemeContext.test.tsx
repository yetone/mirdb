/**
 * Unit tests for ThemeContext and useTheme hook.
 * Covers REQ-12 (theme toggle, localStorage persistence, no flash).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { ThemeProvider, useTheme } from '../../../src/contexts/ThemeContext';

const TestComponent = () => {
  const { theme, toggleTheme, setTheme } = useTheme();
  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      <button data-testid="toggle-btn" onClick={toggleTheme}>
        Toggle
      </button>
      <button data-testid="set-light-btn" onClick={() => setTheme('light')}>
        Set Light
      </button>
      <button data-testid="set-dark-btn" onClick={() => setTheme('dark')}>
        Set Dark
      </button>
    </div>
  );
};

describe('ThemeContext', () => {
  beforeEach(() => {
    document.documentElement.classList.remove('dark');
    window.localStorage.clear();
    vi.stubGlobal(
      'matchMedia',
      vi.fn((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('renders with light theme by default when no localStorage or system preference', () => {
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
  });

  it('reads theme from localStorage on mount', () => {
    window.localStorage.setItem('theme', 'dark');
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
  });

  it('respects system dark mode preference when no localStorage value', () => {
    vi.stubGlobal(
      'matchMedia',
      vi.fn((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))
    );
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
  });

  it('toggles from light to dark', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
    await user.click(screen.getByTestId('toggle-btn'));
    expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
  });

  it('toggles from dark to light', async () => {
    const user = userEvent.setup();
    window.localStorage.setItem('theme', 'dark');
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
    await user.click(screen.getByTestId('toggle-btn'));
    expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
  });

  it('adds dark class to document.documentElement in dark mode', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(document.documentElement.classList.contains('dark')).toBe(false);
    await user.click(screen.getByTestId('toggle-btn'));
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('removes dark class from document.documentElement in light mode', async () => {
    const user = userEvent.setup();
    window.localStorage.setItem('theme', 'dark');
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    await user.click(screen.getByTestId('toggle-btn'));
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('persists dark theme to localStorage', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    await user.click(screen.getByTestId('toggle-btn'));
    expect(window.localStorage.getItem('theme')).toBe('dark');
  });

  it('persists light theme to localStorage', async () => {
    const user = userEvent.setup();
    window.localStorage.setItem('theme', 'dark');
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    await user.click(screen.getByTestId('toggle-btn'));
    expect(window.localStorage.getItem('theme')).toBe('light');
  });

  it('setTheme allows explicit theme setting', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    await user.click(screen.getByTestId('set-dark-btn'));
    expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
    expect(window.localStorage.getItem('theme')).toBe('dark');
    await user.click(screen.getByTestId('set-light-btn'));
    expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
    expect(window.localStorage.getItem('theme')).toBe('light');
  });

  it('throws error when useTheme is called outside ThemeProvider', () => {
    const OriginalConsoleError = console.error;
    console.error = vi.fn();
    expect(() => {
      render(<TestComponent />);
    }).toThrow('useTheme must be used within a ThemeProvider');
    console.error = OriginalConsoleError;
  });

  it('does not throw when useTheme is called inside ThemeProvider', () => {
    expect(() => {
      render(
        <ThemeProvider>
          <TestComponent />
        </ThemeProvider>
      );
    }).not.toThrow();
  });

  it('initializes with light theme when localStorage has invalid value', () => {
    window.localStorage.setItem('theme', 'invalid-value');
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
  });

  it('provides accessible theme state for child components', () => {
    render(
      <ThemeProvider>
        <TestComponent />
      </ThemeProvider>
    );
    expect(screen.getByTestId('current-theme')).toBeInTheDocument();
    expect(screen.getByTestId('toggle-btn')).toBeInTheDocument();
  });
});
