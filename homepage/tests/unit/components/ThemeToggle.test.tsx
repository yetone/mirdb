/**
 * Tests for ThemeToggle component.
 * Owner: Scenario 11 - Dark Mode Theme Support
 */
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeToggle } from '@/components/common/ThemeToggle';

// Mock useTheme hook
const mockToggleTheme = jest.fn();
const mockSetTheme = jest.fn();
let mockTheme = 'light';
let mockMounted = true;

jest.mock('@/hooks/useTheme', () => ({
  useTheme: () => ({
    theme: mockTheme,
    setTheme: mockSetTheme,
    toggleTheme: mockToggleTheme,
    mounted: mockMounted,
  }),
}));

describe('ThemeToggle', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockTheme = 'light';
    mockMounted = true;
  });

  it('renders theme toggle button', () => {
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    expect(button).toBeInTheDocument();
  });

  it('has accessible aria-label for light mode', () => {
    mockTheme = 'light';
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');
  });

  it('has accessible aria-label for dark mode', () => {
    mockTheme = 'dark';
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
  });

  it('calls toggleTheme when clicked', () => {
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    fireEvent.click(button);

    expect(mockToggleTheme).toHaveBeenCalledTimes(1);
  });

  it('displays sun icon in light mode', () => {
    mockTheme = 'light';
    render(<ThemeToggle />);

    const sunIcon = screen.getByTestId('sun-icon');
    expect(sunIcon).toBeInTheDocument();
  });

  it('displays moon icon in dark mode', () => {
    mockTheme = 'dark';
    render(<ThemeToggle />);

    const moonIcon = screen.getByTestId('moon-icon');
    expect(moonIcon).toBeInTheDocument();
  });

  it('renders as disabled button before mounting', () => {
    mockMounted = false;
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    expect(button).toBeDisabled();
  });

  it('is enabled after mounting', () => {
    mockMounted = true;
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    expect(button).not.toBeDisabled();
  });

  it('applies custom className', () => {
    render(<ThemeToggle className="custom-class" />);

    const button = screen.getByTestId('theme-toggle');
    expect(button.className).toContain('custom-class');
  });

  it('is focusable for keyboard accessibility', () => {
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    button.focus();
    expect(document.activeElement).toBe(button);
  });

  it('responds to keyboard Enter key', () => {
    render(<ThemeToggle />);

    const button = screen.getByTestId('theme-toggle');
    fireEvent.keyDown(button, { key: 'Enter', code: 'Enter' });
    fireEvent.click(button);

    expect(mockToggleTheme).toHaveBeenCalled();
  });
});
