/**
 * Unit tests for ThemeToggle component
 * Owner: Scenario 7 - Dark Mode and Theming
 *
 * Tests:
 * - Toggle button rendering with correct icon
 * - Click behavior toggling theme
 * - Accessibility attributes
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeToggle } from '../../../src/components/ui/ThemeToggle';

// Mock useTheme hook
vi.mock('../../../src/hooks/useTheme', () => ({
  useTheme: vi.fn(() => ({
    theme: 'light',
    toggleTheme: vi.fn(),
    setTheme: vi.fn(),
    systemTheme: 'light',
  })),
}));

// Import after mock setup
import { useTheme } from '../../../src/hooks/useTheme';

const mockUseTheme = useTheme as ReturnType<typeof vi.fn>;

// Mock matchMedia for tests
const createMatchMediaMock = (matches: boolean) => {
  return vi.fn().mockImplementation((query: string) => ({
    matches: query.includes('dark') ? matches : !matches,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
};

describe('ThemeToggle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.matchMedia = createMatchMediaMock(false);
  });

  describe('rendering', () => {
    it('renders toggle button', () => {
      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toBeInTheDocument();
    });

    it('displays moon icon in light mode', () => {
      mockUseTheme.mockReturnValue({
        theme: 'light',
        toggleTheme: vi.fn(),
        setTheme: vi.fn(),
        systemTheme: 'light',
      });

      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('data-theme', 'light');
      expect(button).toHaveAttribute('aria-label', 'Switch to dark mode');
    });

    it('displays sun icon in dark mode', () => {
      mockUseTheme.mockReturnValue({
        theme: 'dark',
        toggleTheme: vi.fn(),
        setTheme: vi.fn(),
        systemTheme: 'dark',
      });

      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('data-theme', 'dark');
      expect(button).toHaveAttribute('aria-label', 'Switch to light mode');
    });
  });

  describe('interaction', () => {
    it('calls toggleTheme when clicked', () => {
      const mockToggle = vi.fn();
      mockUseTheme.mockReturnValue({
        theme: 'light',
        toggleTheme: mockToggle,
        setTheme: vi.fn(),
        systemTheme: 'light',
      });

      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      fireEvent.click(button);

      expect(mockToggle).toHaveBeenCalledTimes(1);
    });

    it('uses custom onToggle when provided', () => {
      const customToggle = vi.fn();

      render(<ThemeToggle theme="light" onToggle={customToggle} />);

      const button = screen.getByTestId('theme-toggle');
      fireEvent.click(button);

      expect(customToggle).toHaveBeenCalledTimes(1);
    });

    it('uses custom theme prop when provided', () => {
      render(<ThemeToggle theme="dark" />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('data-theme', 'dark');
    });
  });

  describe('accessibility', () => {
    it('has accessible label for light mode', () => {
      mockUseTheme.mockReturnValue({
        theme: 'light',
        toggleTheme: vi.fn(),
        setTheme: vi.fn(),
        systemTheme: 'light',
      });

      render(<ThemeToggle />);

      const button = screen.getByRole('button', { name: /switch to dark mode/i });
      expect(button).toBeInTheDocument();
    });

    it('has accessible label for dark mode', () => {
      mockUseTheme.mockReturnValue({
        theme: 'dark',
        toggleTheme: vi.fn(),
        setTheme: vi.fn(),
        systemTheme: 'dark',
      });

      render(<ThemeToggle />);

      const button = screen.getByRole('button', { name: /switch to light mode/i });
      expect(button).toBeInTheDocument();
    });

    it('has appropriate button type', () => {
      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle');
      expect(button).toHaveAttribute('type', 'button');
    });

    it('meets minimum touch target size (44x44px)', () => {
      render(<ThemeToggle size="md" />);

      const button = screen.getByTestId('theme-toggle');
      // Check for the size classes
      expect(button.className).toContain('min-w-[44px]');
      expect(button.className).toContain('min-h-[44px]');
    });
  });

  describe('sizes', () => {
    it('renders small size correctly', () => {
      render(<ThemeToggle size="sm" />);

      const button = screen.getByTestId('theme-toggle');
      expect(button.className).toContain('min-w-[36px]');
    });

    it('renders medium size correctly', () => {
      render(<ThemeToggle size="md" />);

      const button = screen.getByTestId('theme-toggle');
      expect(button.className).toContain('min-w-[44px]');
    });

    it('renders large size correctly', () => {
      render(<ThemeToggle size="lg" />);

      const button = screen.getByTestId('theme-toggle');
      expect(button.className).toContain('min-w-[52px]');
    });
  });

  describe('label display', () => {
    it('hides label by default', () => {
      mockUseTheme.mockReturnValue({
        theme: 'light',
        toggleTheme: vi.fn(),
        setTheme: vi.fn(),
        systemTheme: 'light',
      });

      render(<ThemeToggle />);

      expect(screen.queryByText('Dark')).not.toBeInTheDocument();
    });

    it('shows label when showLabel is true', () => {
      mockUseTheme.mockReturnValue({
        theme: 'light',
        toggleTheme: vi.fn(),
        setTheme: vi.fn(),
        systemTheme: 'light',
      });

      render(<ThemeToggle showLabel />);

      expect(screen.getByText('Dark')).toBeInTheDocument();
    });

    it('shows "Light" label in dark mode', () => {
      mockUseTheme.mockReturnValue({
        theme: 'dark',
        toggleTheme: vi.fn(),
        setTheme: vi.fn(),
        systemTheme: 'dark',
      });

      render(<ThemeToggle showLabel />);

      expect(screen.getByText('Light')).toBeInTheDocument();
    });
  });

  describe('custom className', () => {
    it('applies custom className', () => {
      render(<ThemeToggle className="custom-class" />);

      const button = screen.getByTestId('theme-toggle');
      expect(button.className).toContain('custom-class');
    });
  });
});
