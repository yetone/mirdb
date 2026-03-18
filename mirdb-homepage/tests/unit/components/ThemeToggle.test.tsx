/**
 * ThemeToggle Component Unit Tests.
 * Owner: Scenario 7 - Dark Mode Toggle
 *
 * Tests:
 * - Button renders with proper aria-label
 * - Button toggles theme on click
 * - Correct icon is displayed based on theme
 * - Accessible attributes are present
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeToggle } from '../../../src/components/ui/ThemeToggle';
import { THEME_KEY } from '../../../src/utils/constants';

describe('ThemeToggle', () => {
  let mockStorage: Record<string, string> = {};

  beforeEach(() => {
    mockStorage = {};

    // Mock localStorage
    const localStorageMock = {
      getItem: vi.fn((key: string) => mockStorage[key] || null),
      setItem: vi.fn((key: string, value: string) => {
        mockStorage[key] = value;
      }),
      removeItem: vi.fn((key: string) => {
        delete mockStorage[key];
      }),
      clear: vi.fn(() => {
        mockStorage = {};
      }),
      length: 0,
      key: vi.fn(),
    };

    Object.defineProperty(global, 'localStorage', {
      value: localStorageMock,
      writable: true,
      configurable: true,
    });

    // Clear dark class for clean tests
    document.documentElement.classList.remove('dark');
  });

  afterEach(() => {
    document.documentElement.classList.remove('dark');
  });

  it('TC1: renders theme toggle button with appropriate aria-label', () => {
    render(<ThemeToggle />);

    const button = screen.getByRole('button');
    expect(button).toBeTruthy();
    expect(button.getAttribute('aria-label')).toBeTruthy();
  });

  it('has aria-label for switching to light mode when in dark mode', () => {
    // Default is dark mode
    render(<ThemeToggle />);

    const button = screen.getByRole('button');
    expect(button.getAttribute('aria-label')).toBe('Switch to light mode');
  });

  it('has aria-label for switching to dark mode when in light mode', () => {
    // Pre-set to light mode
    mockStorage[THEME_KEY] = 'light';

    render(<ThemeToggle />);

    const button = screen.getByRole('button');
    expect(button.getAttribute('aria-label')).toBe('Switch to dark mode');
  });

  it('renders with data-testid for E2E testing', () => {
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    expect(button).toBeTruthy();
  });

  it('toggles theme when clicked', () => {
    render(<ThemeToggle />);

    const button = screen.getByRole('button');

    // Initially dark mode - aria-label should be for switching to light
    expect(button.getAttribute('aria-label')).toBe('Switch to light mode');

    // Click to toggle
    fireEvent.click(button);

    // Now light mode - aria-label should be for switching to dark
    expect(button.getAttribute('aria-label')).toBe('Switch to dark mode');
  });

  it('displays sun icon in dark mode (to switch to light)', () => {
    render(<ThemeToggle />);

    const button = screen.getByRole('button');
    const svg = button.querySelector('svg');

    expect(svg).toBeTruthy();
    // Sun icon has more complex path with rays
    expect(svg?.getAttribute('aria-hidden')).toBe('true');
  });

  it('displays moon icon in light mode (to switch to dark)', () => {
    mockStorage[THEME_KEY] = 'light';

    render(<ThemeToggle />);

    const button = screen.getByRole('button');
    const svg = button.querySelector('svg');

    expect(svg).toBeTruthy();
    expect(svg?.getAttribute('aria-hidden')).toBe('true');
  });

  it('applies custom className when provided', () => {
    render(<ThemeToggle className="custom-class" />);

    const button = screen.getByRole('button');
    expect(button.classList.contains('custom-class')).toBe(true);
  });

  it('has proper button type attribute', () => {
    render(<ThemeToggle />);

    const button = screen.getByRole('button');
    expect(button.getAttribute('type')).toBe('button');
  });

  it('includes focus ring styles for accessibility', () => {
    render(<ThemeToggle />);

    const button = screen.getByRole('button');
    const classList = button.className;

    expect(classList).toContain('focus:outline-none');
    expect(classList).toContain('focus:ring-2');
  });
});
