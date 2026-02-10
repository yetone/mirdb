/**
 * Unit tests for ThemeToggle component
 * Owner: Scenario 6 - Dark/Light Theme Toggle
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeToggle } from '../../../../src/components/ui/ThemeToggle';
import { ThemeProvider } from '../../../../src/context/ThemeContext';

// Helper function to render ThemeToggle with ThemeProvider
const renderThemeToggle = (defaultTheme: 'light' | 'dark' = 'light') => {
  return render(
    <ThemeProvider defaultTheme={defaultTheme}>
      <ThemeToggle />
    </ThemeProvider>
  );
};

describe('ThemeToggle Component', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Rendering', () => {
    it('renders theme toggle button', () => {
      renderThemeToggle();
      const button = screen.getByTestId('theme-toggle');
      expect(button).toBeInTheDocument();
    });

    it('renders with appropriate icon in light mode', () => {
      renderThemeToggle('light');
      // In light mode, should show moon icon (to switch to dark)
      const moonIcon = screen.getByTestId('moon-icon');
      expect(moonIcon).toBeInTheDocument();
    });

    it('renders with appropriate icon in dark mode', () => {
      renderThemeToggle('dark');
      // In dark mode, should show sun icon (to switch to light)
      const sunIcon = screen.getByTestId('sun-icon');
      expect(sunIcon).toBeInTheDocument();
    });

    it('has correct aria-label in light mode', () => {
      renderThemeToggle('light');
      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');
    });

    it('has correct aria-label in dark mode', () => {
      renderThemeToggle('dark');
      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
    });

    it('has aria-pressed attribute reflecting theme state', () => {
      renderThemeToggle('light');
      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('aria-pressed', 'false');
    });

    it('has aria-pressed=true in dark mode', () => {
      renderThemeToggle('dark');
      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('aria-pressed', 'true');
    });
  });

  describe('Theme Toggle Functionality', () => {
    it('toggles from light to dark when clicked', () => {
      renderThemeToggle('light');
      const button = screen.getByTestId('theme-toggle');

      // Initially in light mode
      expect(button).toHaveAttribute('data-theme', 'light');

      // Click to toggle
      fireEvent.click(button);

      // Should now be in dark mode
      expect(button).toHaveAttribute('data-theme', 'dark');
    });

    it('toggles from dark to light when clicked', () => {
      renderThemeToggle('dark');
      const button = screen.getByTestId('theme-toggle');

      // Initially in dark mode
      expect(button).toHaveAttribute('data-theme', 'dark');

      // Click to toggle
      fireEvent.click(button);

      // Should now be in light mode
      expect(button).toHaveAttribute('data-theme', 'light');
    });

    it('switches icon when theme changes', () => {
      renderThemeToggle('light');

      // Initially shows moon icon (switch to dark)
      expect(screen.getByTestId('moon-icon')).toBeInTheDocument();

      // Click to toggle
      fireEvent.click(screen.getByTestId('theme-toggle'));

      // Should now show sun icon (switch to light)
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument();
    });

    it('updates aria-label when theme changes', () => {
      renderThemeToggle('light');
      const button = screen.getByTestId('theme-toggle');

      expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');

      fireEvent.click(button);

      expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
    });
  });

  describe('Accessibility', () => {
    it('is a button element', () => {
      renderThemeToggle();
      const button = screen.getByTestId('theme-toggle');
      expect(button.tagName).toBe('BUTTON');
    });

    it('has type="button" attribute', () => {
      renderThemeToggle();
      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('type', 'button');
    });

    it('is focusable', () => {
      renderThemeToggle();
      const button = screen.getByTestId('theme-toggle');
      button.focus();
      expect(document.activeElement).toBe(button);
    });

    it('can be activated with keyboard', () => {
      renderThemeToggle('light');
      const button = screen.getByTestId('theme-toggle');

      button.focus();
      fireEvent.keyDown(button, { key: 'Enter', code: 'Enter' });
      fireEvent.click(button); // simulate the click that would follow Enter

      expect(button).toHaveAttribute('data-theme', 'dark');
    });
  });

  describe('Custom className', () => {
    it('accepts and applies custom className', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle className="custom-class" />
        </ThemeProvider>
      );

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveClass('custom-class');
    });
  });
});
