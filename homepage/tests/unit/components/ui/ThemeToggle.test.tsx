/**
 * Unit tests for ThemeToggle component.
 * Owner: Scenario 11 - Dark and Light Mode Toggle
 */

import React from 'react';
import { render, screen, fireEvent, act } from '@testing-library/react';
import { ThemeToggle } from '@/components/ui/ThemeToggle';

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: jest.fn((key: string) => store[key] || null),
    setItem: jest.fn((key: string, value: string) => {
      store[key] = value;
    }),
    removeItem: jest.fn((key: string) => {
      delete store[key];
    }),
    clear: jest.fn(() => {
      store = {};
    }),
  };
})();

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
});

// Mock matchMedia
const mockMatchMedia = jest.fn().mockImplementation((query: string) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener: jest.fn(),
  removeListener: jest.fn(),
  addEventListener: jest.fn(),
  removeEventListener: jest.fn(),
  dispatchEvent: jest.fn(),
}));

Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: mockMatchMedia,
});

describe('ThemeToggle Component', () => {
  beforeEach(() => {
    localStorageMock.clear();
    localStorageMock.getItem.mockClear();
    localStorageMock.setItem.mockClear();
    document.documentElement.classList.remove('dark');
    mockMatchMedia.mockClear();
  });

  // Test Case 1: Render ThemeToggle component
  describe('TC1: Render ThemeToggle component', () => {
    it('should render theme toggle button', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton).toBeInTheDocument();
    });

    it('should have accessible button with aria-label', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton).toHaveAttribute('aria-label');
      expect(toggleButton.getAttribute('aria-label')).toMatch(/switch to (light|dark) mode/i);
    });

    it('should be a button element', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton.tagName.toLowerCase()).toBe('button');
    });

    it('should have type="button" attribute', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton).toHaveAttribute('type', 'button');
    });

    it('should have aria-pressed attribute', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton).toHaveAttribute('aria-pressed');
    });

    it('should render with icon elements', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      // Should have SVG icons inside (Sun and Moon from lucide-react)
      const svgIcons = toggleButton.querySelectorAll('svg');
      expect(svgIcons.length).toBe(2);
    });

    it('should accept custom className prop', () => {
      render(<ThemeToggle className="custom-class" />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton).toHaveClass('custom-class');
    });

    it('should have focus ring styles for keyboard accessibility', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton.className).toContain('focus:');
    });
  });

  // Test for theme toggling functionality
  describe('Theme Toggle Functionality', () => {
    it('should be clickable', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(() => fireEvent.click(toggleButton)).not.toThrow();
    });

    it('should update aria-label after toggle', async () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      const initialLabel = toggleButton.getAttribute('aria-label');

      await act(async () => {
        fireEvent.click(toggleButton);
      });

      // After toggling, the label should change
      const newLabel = toggleButton.getAttribute('aria-label');
      expect(newLabel).not.toBe(initialLabel);
    });

    it('should toggle aria-pressed value when clicked', async () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      const initialPressed = toggleButton.getAttribute('aria-pressed');

      await act(async () => {
        fireEvent.click(toggleButton);
      });

      const newPressed = toggleButton.getAttribute('aria-pressed');
      expect(newPressed).not.toBe(initialPressed);
    });

    it('should persist theme preference to localStorage', async () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');

      await act(async () => {
        fireEvent.click(toggleButton);
      });

      expect(localStorageMock.setItem).toHaveBeenCalled();
    });
  });

  // Test for icon visibility
  describe('Icon Display', () => {
    it('should have both Sun and Moon icons', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      const icons = toggleButton.querySelectorAll('svg');

      expect(icons.length).toBe(2);
    });

    it('should have icons with aria-hidden for accessibility', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      const icons = toggleButton.querySelectorAll('svg');

      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });

  // Test for hover and focus states
  describe('Interaction States', () => {
    it('should have hover state styles', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton.className).toContain('hover:');
    });

    it('should have transition for smooth animation', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle');
      expect(toggleButton.className).toContain('transition');
    });
  });
});
