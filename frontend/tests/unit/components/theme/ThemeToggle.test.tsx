/**
 * ThemeToggle Component Tests
 * Owner: Scenario 4 - Theme Switching
 *
 * Comprehensive tests for the ThemeToggle component and theme switching functionality.
 */

import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import ThemeToggle from '../../../../src/components/ThemeToggle';
import { useThemeStore } from '../../../../src/stores/themeStore';
import { AVAILABLE_THEMES } from '../../../../src/types/home';

// Reset store state before each test
beforeEach(() => {
  // Reset the Zustand store
  useThemeStore.setState({ theme: 'light' });
  // Reset localStorage
  localStorage.clear();
  // Reset document theme
  document.documentElement.setAttribute('data-theme', 'light');
});

afterEach(() => {
  vi.clearAllMocks();
});

describe('ThemeToggle Component', () => {
  /**
   * Test Case 1: Renders ThemeToggle component
   */
  describe('Test Case 1: Component Rendering', () => {
    it('should render the theme toggle button', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle-button');
      expect(toggleButton).toBeInTheDocument();
    });

    it('should render the theme toggle wrapper', () => {
      render(<ThemeToggle />);

      const toggleWrapper = screen.getByTestId('theme-toggle');
      expect(toggleWrapper).toBeInTheDocument();
    });

    it('should have accessible aria-label on the toggle button', () => {
      render(<ThemeToggle />);

      const toggleButton = screen.getByTestId('theme-toggle-button');
      expect(toggleButton).toHaveAttribute('aria-label');
      expect(toggleButton.getAttribute('aria-label')).toContain('theme');
    });
  });

  /**
   * Test Case 2: Theme dropdown displays all available themes
   */
  describe('Test Case 2: Theme Dropdown Options', () => {
    it('should display all available themes in the dropdown', () => {
      render(<ThemeToggle />);

      // Check that all theme options are rendered
      AVAILABLE_THEMES.forEach((theme) => {
        const themeOption = screen.getByTestId(`theme-option-${theme}`);
        expect(themeOption).toBeInTheDocument();
      });
    });

    it('should show theme names with capitalized first letter', () => {
      render(<ThemeToggle />);

      expect(screen.getByText('Light')).toBeInTheDocument();
      expect(screen.getByText('Dark')).toBeInTheDocument();
      expect(screen.getByText('Cyberpunk')).toBeInTheDocument();
      expect(screen.getByText('Synthwave')).toBeInTheDocument();
      expect(screen.getByText('Forest')).toBeInTheDocument();
    });

    it('should mark the current theme as active', () => {
      useThemeStore.setState({ theme: 'dark' });
      render(<ThemeToggle />);

      const darkOption = screen.getByTestId('theme-option-dark');
      expect(darkOption).toHaveAttribute('aria-current', 'true');
    });
  });

  /**
   * Test Case 3: Theme switching functionality
   */
  describe('Test Case 3: Theme Switching', () => {
    it('should switch to dark theme when dark option is clicked', async () => {
      render(<ThemeToggle />);

      const darkOption = screen.getByTestId('theme-option-dark');
      await userEvent.click(darkOption);

      expect(useThemeStore.getState().theme).toBe('dark');
    });

    it('should switch to cyberpunk theme when cyberpunk option is clicked', async () => {
      render(<ThemeToggle />);

      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk');
      await userEvent.click(cyberpunkOption);

      expect(useThemeStore.getState().theme).toBe('cyberpunk');
    });

    it('should switch to synthwave theme when synthwave option is clicked', async () => {
      render(<ThemeToggle />);

      const synthwaveOption = screen.getByTestId('theme-option-synthwave');
      await userEvent.click(synthwaveOption);

      expect(useThemeStore.getState().theme).toBe('synthwave');
    });

    it('should switch to forest theme when forest option is clicked', async () => {
      render(<ThemeToggle />);

      const forestOption = screen.getByTestId('theme-option-forest');
      await userEvent.click(forestOption);

      expect(useThemeStore.getState().theme).toBe('forest');
    });

    it('should switch back to light theme when light option is clicked', async () => {
      useThemeStore.setState({ theme: 'dark' });
      render(<ThemeToggle />);

      const lightOption = screen.getByTestId('theme-option-light');
      await userEvent.click(lightOption);

      expect(useThemeStore.getState().theme).toBe('light');
    });
  });

  /**
   * Test Case 4: Theme applies to document
   */
  describe('Test Case 4: Theme Application to Document', () => {
    it('should set data-theme attribute on document when theme changes', async () => {
      render(<ThemeToggle />);

      const darkOption = screen.getByTestId('theme-option-dark');
      await userEvent.click(darkOption);

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should apply cyberpunk theme to document', async () => {
      render(<ThemeToggle />);

      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk');
      await userEvent.click(cyberpunkOption);

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });
  });

  /**
   * Test Case 5: Custom className prop
   */
  describe('Test Case 5: Custom className', () => {
    it('should apply custom className to the wrapper', () => {
      render(<ThemeToggle className="custom-class" />);

      const toggleWrapper = screen.getByTestId('theme-toggle');
      expect(toggleWrapper).toHaveClass('custom-class');
    });
  });

  /**
   * Test Case 6: Icon display based on theme
   */
  describe('Test Case 6: Icon Display', () => {
    it('should display sun icon for light theme', () => {
      useThemeStore.setState({ theme: 'light' });
      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle-button');
      const svg = button.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });

    it('should display moon icon for dark theme', () => {
      useThemeStore.setState({ theme: 'dark' });
      render(<ThemeToggle />);

      const button = screen.getByTestId('theme-toggle-button');
      const svg = button.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });
  });
});

describe('Theme Store', () => {
  beforeEach(() => {
    useThemeStore.setState({ theme: 'light' });
    localStorage.clear();
    document.documentElement.setAttribute('data-theme', 'light');
  });

  /**
   * Test setTheme function
   */
  describe('setTheme', () => {
    it('should update theme state', () => {
      const { setTheme } = useThemeStore.getState();
      setTheme('dark');

      expect(useThemeStore.getState().theme).toBe('dark');
    });

    it('should apply theme to document', () => {
      const { setTheme } = useThemeStore.getState();
      setTheme('cyberpunk');

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });
  });

  /**
   * Test toggleTheme function
   */
  describe('toggleTheme', () => {
    it('should cycle through themes in order', () => {
      const { toggleTheme } = useThemeStore.getState();

      // Start at light
      expect(useThemeStore.getState().theme).toBe('light');

      // Toggle to dark
      toggleTheme();
      expect(useThemeStore.getState().theme).toBe('dark');

      // Toggle to cyberpunk
      toggleTheme();
      expect(useThemeStore.getState().theme).toBe('cyberpunk');

      // Toggle to synthwave
      toggleTheme();
      expect(useThemeStore.getState().theme).toBe('synthwave');

      // Toggle to forest
      toggleTheme();
      expect(useThemeStore.getState().theme).toBe('forest');

      // Cycle back to light
      toggleTheme();
      expect(useThemeStore.getState().theme).toBe('light');
    });
  });
});
