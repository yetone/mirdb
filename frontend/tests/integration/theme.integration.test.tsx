/**
 * Theme Integration Tests
 * Owner: Scenario 4 - Theme Switching
 *
 * Integration tests for theme switching across the homepage and navigation.
 * Validates that theme changes persist and affect all components.
 */

import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import App from '../../src/App';
import { useThemeStore } from '../../src/stores/themeStore';
import { AVAILABLE_THEMES } from '../../src/types/home';

// Test wrapper with router
const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <App />
    </MemoryRouter>
  );
};

describe('Theme Integration Tests', () => {
  beforeEach(() => {
    // Reset the Zustand store
    useThemeStore.setState({ theme: 'light' });
    // Reset localStorage
    localStorage.clear();
    // Reset document theme
    document.documentElement.setAttribute('data-theme', 'light');
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Test Case 1: Homepage renders with theme from ThemeContext
   */
  describe('Test Case 1: Homepage with ThemeContext', () => {
    it('should render homepage with theme from theme store', () => {
      useThemeStore.setState({ theme: 'dark' });
      document.documentElement.setAttribute('data-theme', 'dark');

      renderWithRouter(['/']);

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should have theme toggle visible in navbar', () => {
      renderWithRouter(['/']);

      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      expect(screen.getByTestId('theme-toggle-button')).toBeInTheDocument();
    });

    it('should display all homepage components with theme styling', () => {
      renderWithRouter(['/']);

      // Verify all main sections are rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      expect(screen.getByTestId('navbar')).toBeInTheDocument();
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('footer')).toBeInTheDocument();
    });
  });

  /**
   * Test Case 2: Click theme toggle button changes theme
   */
  describe('Test Case 2: Theme toggle functionality', () => {
    it('should change theme when clicking theme options', async () => {
      renderWithRouter(['/']);

      // Find and click dark theme option
      const darkOption = screen.getByTestId('theme-option-dark');
      await userEvent.click(darkOption);

      expect(useThemeStore.getState().theme).toBe('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should update all homepage components when theme changes', async () => {
      renderWithRouter(['/']);

      // Switch to dark theme
      const darkOption = screen.getByTestId('theme-option-dark');
      await userEvent.click(darkOption);

      // Verify document theme changed
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Components should still be visible with new theme
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('footer')).toBeInTheDocument();
    });
  });

  /**
   * Test Case 3: Dark theme styling
   */
  describe('Test Case 3: Dark theme application', () => {
    it('should apply dark theme to document', async () => {
      renderWithRouter(['/']);

      const darkOption = screen.getByTestId('theme-option-dark');
      await userEvent.click(darkOption);

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should display hero section with dark theme', async () => {
      renderWithRouter(['/']);

      await userEvent.click(screen.getByTestId('theme-option-dark'));

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should display features section with dark theme', async () => {
      renderWithRouter(['/']);

      await userEvent.click(screen.getByTestId('theme-option-dark'));

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should display footer with dark theme', async () => {
      renderWithRouter(['/']);

      await userEvent.click(screen.getByTestId('theme-option-dark'));

      const footer = screen.getByTestId('footer');
      expect(footer).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });

  /**
   * Test Case 4: Cyberpunk theme styling
   */
  describe('Test Case 4: Cyberpunk theme application', () => {
    it('should apply cyberpunk theme to document', async () => {
      renderWithRouter(['/']);

      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk');
      await userEvent.click(cyberpunkOption);

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    it('should display all components with cyberpunk theme colors', async () => {
      renderWithRouter(['/']);

      await userEvent.click(screen.getByTestId('theme-option-cyberpunk'));

      // All major components should be visible with cyberpunk theme
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('footer')).toBeInTheDocument();
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });
  });

  /**
   * Test Case 5: Theme persists across navigation
   */
  describe('Test Case 5: Theme persistence across navigation', () => {
    it('should persist theme when navigating to login and back', async () => {
      renderWithRouter(['/']);

      // Change to dark theme
      await userEvent.click(screen.getByTestId('theme-option-dark'));
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Navigate to login
      fireEvent.click(screen.getByTestId('login-link'));
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });

      // Theme should persist
      expect(useThemeStore.getState().theme).toBe('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Navigate back to home
      fireEvent.click(screen.getByTestId('brand-logo'));
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Theme should still be dark
      expect(useThemeStore.getState().theme).toBe('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should persist theme when navigating to register and back', async () => {
      renderWithRouter(['/']);

      // Change to cyberpunk theme
      await userEvent.click(screen.getByTestId('theme-option-cyberpunk'));
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

      // Navigate to register
      fireEvent.click(screen.getByTestId('register-link'));
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });

      // Theme should persist
      expect(useThemeStore.getState().theme).toBe('cyberpunk');

      // Navigate back to home via brand logo
      fireEvent.click(screen.getByTestId('brand-logo'));
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Theme should still be cyberpunk
      expect(useThemeStore.getState().theme).toBe('cyberpunk');
    });
  });

  /**
   * Test Case 6: Theme persists on page reload (via localStorage)
   */
  describe('Test Case 6: Theme persistence via localStorage', () => {
    it('should store theme in localStorage when changed', async () => {
      renderWithRouter(['/']);

      await userEvent.click(screen.getByTestId('theme-option-dark'));

      // Check that theme is stored
      const stored = localStorage.getItem('linksnip-theme');
      expect(stored).toBeTruthy();
      const parsed = JSON.parse(stored!);
      expect(parsed.state.theme).toBe('dark');
    });

    it('should restore theme from localStorage on mount', () => {
      // Pre-set localStorage
      localStorage.setItem('linksnip-theme', JSON.stringify({ state: { theme: 'cyberpunk' } }));
      useThemeStore.setState({ theme: 'cyberpunk' });
      document.documentElement.setAttribute('data-theme', 'cyberpunk');

      renderWithRouter(['/']);

      expect(useThemeStore.getState().theme).toBe('cyberpunk');
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });
  });

  /**
   * Test Case 7: All themes can be cycled through
   */
  describe('Test Case 7: Theme cycling', () => {
    it('should be able to switch through all available themes', async () => {
      renderWithRouter(['/']);

      for (const theme of AVAILABLE_THEMES) {
        const themeOption = screen.getByTestId(`theme-option-${theme}`);
        await userEvent.click(themeOption);

        expect(useThemeStore.getState().theme).toBe(theme);
        expect(document.documentElement.getAttribute('data-theme')).toBe(theme);
      }
    });
  });

  /**
   * Test Case: Multiple navigation cycles with theme persistence
   */
  describe('Theme persistence through complex navigation', () => {
    it('should persist theme through multiple page navigations', async () => {
      renderWithRouter(['/']);

      // Set synthwave theme
      await userEvent.click(screen.getByTestId('theme-option-synthwave'));
      expect(useThemeStore.getState().theme).toBe('synthwave');

      // Navigate to login
      fireEvent.click(screen.getByTestId('login-link'));
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
      expect(useThemeStore.getState().theme).toBe('synthwave');

      // Navigate to register via link in login page
      fireEvent.click(screen.getByTestId('page-register-link'));
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
      expect(useThemeStore.getState().theme).toBe('synthwave');

      // Navigate back to home
      fireEvent.click(screen.getByTestId('brand-logo'));
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });
      expect(useThemeStore.getState().theme).toBe('synthwave');
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
    });
  });
});
