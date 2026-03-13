/**
 * Theme Toggle Component Unit Tests
 * Owner: Scenario 8 - Dark Mode Toggle
 *
 * Tests for:
 * - Toggle button rendering with appropriate icon
 * - Correct icon display based on current theme
 * - Accessible aria-label
 * - Theme toggle functionality
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeToggle } from '../../../../src/components/ui/ThemeToggle';
import { ThemeProvider } from '../../../../src/context/ThemeContext';

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

describe('ThemeToggle Component', () => {
  beforeEach(() => {
    // Setup mocks
    Object.defineProperty(window, 'localStorage', { value: localStorageMock });
    Object.defineProperty(window, 'matchMedia', { value: matchMediaMock, writable: true });
    localStorageMock.clear();
    document.documentElement.classList.remove('dark');
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Toggle button is rendered with appropriate icon
  it('renders toggle button with appropriate icon', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle />
      </ThemeProvider>
    );

    const button = screen.getByTestId('theme-toggle-button');
    expect(button).toBeInTheDocument();
    expect(button).toHaveAttribute('type', 'button');
  });

  // Test Case: Shows moon icon when in light mode
  it('shows moon icon when in light mode', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle />
      </ThemeProvider>
    );

    const moonIcon = screen.getByTestId('moon-icon');
    expect(moonIcon).toBeInTheDocument();
    expect(screen.queryByTestId('sun-icon')).not.toBeInTheDocument();
  });

  // Test Case: Shows sun icon when in dark mode
  it('shows sun icon when in dark mode', () => {
    render(
      <ThemeProvider defaultTheme="dark">
        <ThemeToggle />
      </ThemeProvider>
    );

    const sunIcon = screen.getByTestId('sun-icon');
    expect(sunIcon).toBeInTheDocument();
    expect(screen.queryByTestId('moon-icon')).not.toBeInTheDocument();
  });

  // Test Case: Button has correct aria-label for light mode
  it('has aria-label "Switch to dark mode" when in light mode', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle />
      </ThemeProvider>
    );

    const button = screen.getByTestId('theme-toggle-button');
    expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');
  });

  // Test Case: Button has correct aria-label for dark mode
  it('has aria-label "Switch to light mode" when in dark mode', () => {
    render(
      <ThemeProvider defaultTheme="dark">
        <ThemeToggle />
      </ThemeProvider>
    );

    const button = screen.getByTestId('theme-toggle-button');
    expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
  });

  // Test Case: Clicking button toggles theme
  it('toggles theme when button is clicked', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle />
      </ThemeProvider>
    );

    const button = screen.getByTestId('theme-toggle-button');

    // Initially in light mode - should show moon icon
    expect(screen.getByTestId('moon-icon')).toBeInTheDocument();
    expect(button).toHaveAttribute('data-theme', 'light');

    // Click to toggle to dark mode
    fireEvent.click(button);

    // Should now show sun icon
    expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
    expect(button).toHaveAttribute('data-theme', 'dark');
  });

  // Test Case: Button has proper styling classes for transitions
  it('has transition classes for smooth animation', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle />
      </ThemeProvider>
    );

    const button = screen.getByTestId('theme-toggle-button');
    expect(button.className).toContain('transition');
  });

  // Test Case: Accepts custom className prop
  it('accepts custom className prop', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle className="custom-class" />
      </ThemeProvider>
    );

    const button = screen.getByTestId('theme-toggle-button');
    expect(button.className).toContain('custom-class');
  });

  // Test Case: Screen reader only text is present
  it('has screen reader only text', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle />
      </ThemeProvider>
    );

    const srText = screen.getByText('Switch to dark mode');
    expect(srText).toBeInTheDocument();
    expect(srText).toHaveClass('sr-only');
  });
});
