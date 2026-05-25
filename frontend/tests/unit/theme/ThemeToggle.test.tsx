/**
 * Unit tests for ThemeToggle component.
 * Covers REQ-12 (theme toggle button, icon display, accessibility).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { ThemeProvider, useTheme } from '../../../src/contexts/ThemeContext';
import ThemeToggle from '../../../src/components/theme/ThemeToggle';

describe('ThemeToggle', () => {
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

  it('renders the theme toggle button', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-toggle-button')).toBeInTheDocument();
  });

  it('shows moon icon in light mode (default)', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-toggle-moon-icon')).toBeInTheDocument();
    expect(screen.queryByTestId('theme-toggle-sun-icon')).not.toBeInTheDocument();
  });

  it('shows sun icon after toggling to dark mode', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-toggle-moon-icon')).toBeInTheDocument();
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(screen.queryByTestId('theme-toggle-moon-icon')).not.toBeInTheDocument();
    expect(screen.getByTestId('theme-toggle-sun-icon')).toBeInTheDocument();
  });

  it('shows moon icon after toggling back to light mode', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(screen.getByTestId('theme-toggle-sun-icon')).toBeInTheDocument();
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(screen.getByTestId('theme-toggle-moon-icon')).toBeInTheDocument();
    expect(screen.queryByTestId('theme-toggle-sun-icon')).not.toBeInTheDocument();
  });

  it('has correct aria-label in light mode', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    const button = screen.getByTestId('theme-toggle-button');
    expect(button).toHaveAttribute('aria-label', 'Switch to dark theme');
  });

  it('has correct aria-label in dark mode', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    await user.click(screen.getByTestId('theme-toggle-button'));
    const button = screen.getByTestId('theme-toggle-button');
    expect(button).toHaveAttribute('aria-label', 'Switch to light theme');
  });

  it('has correct title attribute in light mode', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    const button = screen.getByTestId('theme-toggle-button');
    expect(button).toHaveAttribute('title', 'Switch to dark theme');
  });

  it('has correct title attribute in dark mode', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    await user.click(screen.getByTestId('theme-toggle-button'));
    const button = screen.getByTestId('theme-toggle-button');
    expect(button).toHaveAttribute('title', 'Switch to light theme');
  });

  it('shows Dark label in light mode', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-toggle-label')).toHaveTextContent('Dark');
  });

  it('shows Light label in dark mode', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(screen.getByTestId('theme-toggle-label')).toHaveTextContent('Light');
  });

  it('is a button element with type button', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    const button = screen.getByTestId('theme-toggle-button');
    expect(button.tagName).toBe('BUTTON');
    expect(button).toHaveAttribute('type', 'button');
  });

  it('applies custom className when provided', () => {
    render(
      <ThemeProvider>
        <ThemeToggle className="my-custom-class" />
      </ThemeProvider>
    );
    const button = screen.getByTestId('theme-toggle-button');
    expect(button.classList.contains('my-custom-class')).toBe(true);
  });

  it('toggles theme when clicked and persists to localStorage', async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    expect(window.localStorage.getItem('theme')).toBe('light');
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(window.localStorage.getItem('theme')).toBe('dark');
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(window.localStorage.getItem('theme')).toBe('light');
  });

  it('icons have aria-hidden attribute', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    const icon = screen.getByTestId('theme-toggle-moon-icon');
    expect(icon).toHaveAttribute('aria-hidden', 'true');
  });

  it('initializes with dark theme from localStorage and shows sun icon', () => {
    window.localStorage.setItem('theme', 'dark');
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-toggle-sun-icon')).toBeInTheDocument();
    expect(screen.queryByTestId('theme-toggle-moon-icon')).not.toBeInTheDocument();
  });

  it('icon wrapper is rendered', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-toggle-icon-wrapper')).toBeInTheDocument();
  });

  it('theme label is rendered', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );
    expect(screen.getByTestId('theme-toggle-label')).toBeInTheDocument();
  });
});
