import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ThemeToggle } from '../../../src/components/ThemeToggle';

// Mock the useTheme hook
vi.mock('../../../src/hooks/useTheme', () => ({
  useTheme: vi.fn(),
}));

import { useTheme } from '../../../src/hooks/useTheme';

describe('ThemeToggle Component', () => {
  const mockToggleTheme = vi.fn();
  const mockSetTheme = vi.fn();

  beforeEach(() => {
    vi.mocked(useTheme).mockReturnValue({
      theme: 'light',
      toggleTheme: mockToggleTheme,
      setTheme: mockSetTheme,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Render ThemeToggle component - Component renders with theme toggle button
  describe('Test Case 1: Component renders with theme toggle button', () => {
    it('renders a button element', () => {
      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      expect(button).toBeInTheDocument();
    });

    it('button has accessible label', () => {
      render(<ThemeToggle />);

      const button = screen.getByRole('button', { name: /switch to dark mode/i });
      expect(button).toBeInTheDocument();
    });

    it('button has type="button" attribute', () => {
      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('type', 'button');
    });
  });

  // Test Case 2: Render ThemeToggle in light mode - Moon icon is displayed (to switch to dark)
  describe('Test Case 2: Light mode shows moon icon', () => {
    it('displays moon icon in light mode', () => {
      vi.mocked(useTheme).mockReturnValue({
        theme: 'light',
        toggleTheme: mockToggleTheme,
        setTheme: mockSetTheme,
      });

      render(<ThemeToggle />);

      const moonIcon = screen.getByTestId('moon-icon');
      expect(moonIcon).toBeInTheDocument();
    });

    it('does not display sun icon in light mode', () => {
      vi.mocked(useTheme).mockReturnValue({
        theme: 'light',
        toggleTheme: mockToggleTheme,
        setTheme: mockSetTheme,
      });

      render(<ThemeToggle />);

      expect(screen.queryByTestId('sun-icon')).not.toBeInTheDocument();
    });

    it('button has correct aria-label in light mode', () => {
      vi.mocked(useTheme).mockReturnValue({
        theme: 'light',
        toggleTheme: mockToggleTheme,
        setTheme: mockSetTheme,
      });

      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');
    });

    it('button has correct title in light mode', () => {
      vi.mocked(useTheme).mockReturnValue({
        theme: 'light',
        toggleTheme: mockToggleTheme,
        setTheme: mockSetTheme,
      });

      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('title', 'Switch to dark mode');
    });
  });

  // Test Case 3: Render ThemeToggle in dark mode - Sun icon is displayed (to switch to light)
  describe('Test Case 3: Dark mode shows sun icon', () => {
    it('displays sun icon in dark mode', () => {
      vi.mocked(useTheme).mockReturnValue({
        theme: 'dark',
        toggleTheme: mockToggleTheme,
        setTheme: mockSetTheme,
      });

      render(<ThemeToggle />);

      const sunIcon = screen.getByTestId('sun-icon');
      expect(sunIcon).toBeInTheDocument();
    });

    it('does not display moon icon in dark mode', () => {
      vi.mocked(useTheme).mockReturnValue({
        theme: 'dark',
        toggleTheme: mockToggleTheme,
        setTheme: mockSetTheme,
      });

      render(<ThemeToggle />);

      expect(screen.queryByTestId('moon-icon')).not.toBeInTheDocument();
    });

    it('button has correct aria-label in dark mode', () => {
      vi.mocked(useTheme).mockReturnValue({
        theme: 'dark',
        toggleTheme: mockToggleTheme,
        setTheme: mockSetTheme,
      });

      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
    });

    it('button has correct title in dark mode', () => {
      vi.mocked(useTheme).mockReturnValue({
        theme: 'dark',
        toggleTheme: mockToggleTheme,
        setTheme: mockSetTheme,
      });

      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      expect(button).toHaveAttribute('title', 'Switch to light mode');
    });
  });

  // User interaction tests
  describe('User interactions', () => {
    it('calls toggleTheme when button is clicked', async () => {
      const user = userEvent.setup();

      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      await user.click(button);

      expect(mockToggleTheme).toHaveBeenCalledTimes(1);
    });

    it('calls toggleTheme when button is activated with keyboard', async () => {
      const user = userEvent.setup();

      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      button.focus();
      await user.keyboard('{Enter}');

      expect(mockToggleTheme).toHaveBeenCalledTimes(1);
    });

    it('calls toggleTheme when button is activated with space', async () => {
      const user = userEvent.setup();

      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      button.focus();
      await user.keyboard(' ');

      expect(mockToggleTheme).toHaveBeenCalledTimes(1);
    });
  });

  // Accessibility tests
  describe('Accessibility', () => {
    it('icon has aria-hidden attribute', () => {
      render(<ThemeToggle />);

      const icon = screen.getByTestId('moon-icon');
      expect(icon).toHaveAttribute('aria-hidden', 'true');
    });

    it('button is focusable', () => {
      render(<ThemeToggle />);

      const button = screen.getByRole('button');
      button.focus();

      expect(document.activeElement).toBe(button);
    });
  });
});
