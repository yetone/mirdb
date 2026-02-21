/**
 * Theme integration tests.
 * Owner: Scenario 4 - Theme Integration and Switching
 *
 * Test coverage:
 * - Homepage inherits theme from ThemeContext
 * - setTheme updates document data-theme attribute
 * - Theme changes without page reload
 * - Theme persists via localStorage
 * - BackgroundEffect uses theme colors
 */

import React from 'react';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { ThemeProvider, useTheme, ThemeContext } from '@/contexts/ThemeContext';
import { AuthProvider } from '@/contexts/AuthContext';
import { Home } from '@/pages/Home';
import { BackgroundEffect } from '@/components/common/BackgroundEffect';
import { Navbar } from '@/components/layout/Navbar';
import { Theme } from '@/types';
import { getAvailableThemes } from '../utils/mockTheme';

// Helper to render components with providers
function renderWithThemeProvider(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          {ui}
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  );
}

// Test component to access useTheme hook
function ThemeTestComponent({ onMount }: { onMount?: (theme: string, setTheme: (t: Theme) => void) => void }) {
  const { theme, setTheme } = useTheme();

  React.useEffect(() => {
    if (onMount) {
      onMount(theme, setTheme);
    }
  }, [onMount, theme, setTheme]);

  return (
    <div data-testid="theme-test-component">
      <span data-testid="current-theme">{theme}</span>
      <button data-testid="set-dark-btn" onClick={() => setTheme('dark')}>
        Set Dark
      </button>
      <button data-testid="set-light-btn" onClick={() => setTheme('light')}>
        Set Light
      </button>
    </div>
  );
}

describe('Theme Integration and Switching', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    vi.mocked(localStorage.getItem).mockReturnValue(null);
    vi.mocked(localStorage.setItem).mockClear();
    // Reset data-theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Homepage inherits theme from ThemeContext', () => {
    it('should render homepage wrapped in ThemeProvider and inherit theme', () => {
      renderWithThemeProvider(<Home />);

      // Verify homepage renders successfully
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      // Verify theme context is accessible (navbar theme toggle exists)
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();

      // Verify document has data-theme attribute
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('should apply the current theme to homepage components', () => {
      renderWithThemeProvider(<Home />);

      // The page should render with the default light theme
      const dataTheme = document.documentElement.getAttribute('data-theme');
      expect(dataTheme).toBe('light');
    });
  });

  describe('Test Case 2: setTheme updates document data-theme attribute', () => {
    it('should update document data-theme attribute when setTheme is called with dark', async () => {
      let capturedSetTheme: ((t: Theme) => void) | null = null;

      renderWithThemeProvider(
        <ThemeTestComponent
          onMount={(_, setTheme) => { capturedSetTheme = setTheme; }}
        />
      );

      // Initial state should be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Click the set dark button
      fireEvent.click(screen.getByTestId('set-dark-btn'));

      // data-theme attribute should update to dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });

    it('should call localStorage.setItem when theme is changed', () => {
      renderWithThemeProvider(<ThemeTestComponent />);

      // Click the set dark button
      fireEvent.click(screen.getByTestId('set-dark-btn'));

      // localStorage should be called with new theme
      expect(localStorage.setItem).toHaveBeenCalledWith('theme', 'dark');
    });
  });

  describe('Test Case 3: Theme updates immediately without full page reload', () => {
    it('should toggle theme multiple times without page reload', async () => {
      renderWithThemeProvider(<ThemeTestComponent />);

      // Initial theme is light
      expect(screen.getByTestId('current-theme').textContent).toBe('light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Switch to dark
      fireEvent.click(screen.getByTestId('set-dark-btn'));
      await waitFor(() => {
        expect(screen.getByTestId('current-theme').textContent).toBe('dark');
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });

      // Switch back to light
      fireEvent.click(screen.getByTestId('set-light-btn'));
      await waitFor(() => {
        expect(screen.getByTestId('current-theme').textContent).toBe('light');
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });

      // Switch to dark again
      fireEvent.click(screen.getByTestId('set-dark-btn'));
      await waitFor(() => {
        expect(screen.getByTestId('current-theme').textContent).toBe('dark');
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });

      // Component should still be mounted (no reload)
      expect(screen.getByTestId('theme-test-component')).toBeInTheDocument();
    });

    it('should update theme via navbar theme selector without reload', async () => {
      renderWithThemeProvider(<Home />);

      // Initial theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Click on dark theme option
      fireEvent.click(screen.getByTestId('theme-option-dark'));

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });

      // Page should still be rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Theme preference persists via localStorage', () => {
    it('should store theme in localStorage when changed', () => {
      renderWithThemeProvider(<ThemeTestComponent />);

      // Change theme to dark
      fireEvent.click(screen.getByTestId('set-dark-btn'));

      expect(localStorage.setItem).toHaveBeenCalledWith('theme', 'dark');
    });

    it('should restore theme from localStorage on mount', () => {
      // Simulate stored theme
      vi.mocked(localStorage.getItem).mockReturnValue('cyberpunk');

      renderWithThemeProvider(<ThemeTestComponent />);

      // Theme should be restored from localStorage
      expect(screen.getByTestId('current-theme').textContent).toBe('cyberpunk');
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    it('should persist each theme change to localStorage', () => {
      renderWithThemeProvider(<ThemeTestComponent />);

      // Change multiple themes
      fireEvent.click(screen.getByTestId('set-dark-btn'));
      expect(localStorage.setItem).toHaveBeenLastCalledWith('theme', 'dark');

      fireEvent.click(screen.getByTestId('set-light-btn'));
      expect(localStorage.setItem).toHaveBeenLastCalledWith('theme', 'light');
    });
  });

  describe('Test Case 5: BackgroundEffect respects theme colors', () => {
    it('should render BackgroundEffect with DaisyUI theme color classes', () => {
      renderWithThemeProvider(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();

      // Check that the container has the gradient orbs
      const orbs = backgroundEffect.querySelectorAll('div');
      expect(orbs.length).toBeGreaterThanOrEqual(3);
    });

    it('should use primary, secondary, and accent colors for gradient orbs', () => {
      const { container } = renderWithThemeProvider(<BackgroundEffect />);

      // Check for DaisyUI theme color classes
      const primaryOrb = container.querySelector('.bg-primary\\/20');
      const secondaryOrb = container.querySelector('.bg-secondary\\/20');
      const accentOrb = container.querySelector('.bg-accent\\/20');

      expect(primaryOrb).toBeInTheDocument();
      expect(secondaryOrb).toBeInTheDocument();
      expect(accentOrb).toBeInTheDocument();
    });

    it('should have fixed positioning and negative z-index', () => {
      renderWithThemeProvider(<BackgroundEffect />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // Check for fixed positioning and z-index classes
      expect(backgroundEffect).toHaveClass('fixed');
      expect(backgroundEffect).toHaveClass('-z-10');
    });

    it('should render BackgroundEffect within themed homepage', () => {
      renderWithThemeProvider(<Home />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();

      // Verify it's part of the homepage structure
      const homePage = screen.getByTestId('home-page');
      expect(homePage).toContainElement(backgroundEffect);
    });
  });

  describe('All available themes', () => {
    it('should support all 6 available themes', () => {
      const themes = getAvailableThemes();
      expect(themes).toEqual(['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine']);
      expect(themes.length).toBe(6);
    });

    it('should display all theme options in navbar', () => {
      renderWithThemeProvider(<Home />);

      const availableThemes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine'];

      availableThemes.forEach((themeName) => {
        const themeOption = screen.getByTestId(`theme-option-${themeName}`);
        expect(themeOption).toBeInTheDocument();
      });
    });

    it('should apply each theme correctly when selected', async () => {
      renderWithThemeProvider(<Home />);

      const availableThemes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine'];

      for (const themeName of availableThemes) {
        fireEvent.click(screen.getByTestId(`theme-option-${themeName}`));

        await waitFor(() => {
          expect(document.documentElement.getAttribute('data-theme')).toBe(themeName);
        });

        expect(localStorage.setItem).toHaveBeenCalledWith('theme', themeName);
      }
    });
  });
});
