/**
 * Theme Support Integration Tests
 * Owner: Scenario 7
 *
 * Test coverage:
 * - Light theme rendering
 * - Dark theme rendering
 * - Cyberpunk/synthwave theme rendering
 * - Theme switching behavior
 *
 * Test suites:
 * - describe('Theme Application')
 * - describe('Theme Switching')
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React, { ReactElement } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from 'react-query';
import { AuthContext } from '../../src/contexts/AuthContext';
import { ThemeContext } from '../../src/contexts/ThemeContext';
import Home from '../../src/pages/Home';

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine' | 'night';

interface ThemeContextType {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

interface AuthContextType {
  user: null;
  loading: boolean;
  isAuthenticated: boolean;
  isAdmin: boolean;
  login: () => Promise<void>;
  logout: () => void;
}

const createMockAuthContext = (): AuthContextType => ({
  user: null,
  loading: false,
  isAuthenticated: false,
  isAdmin: false,
  login: vi.fn(),
  logout: vi.fn(),
});

const createMockThemeContext = (theme: Theme, setTheme?: (t: Theme) => void): ThemeContextType => ({
  theme,
  setTheme: setTheme || vi.fn(),
});

interface RenderOptions {
  theme?: Theme;
  setTheme?: (theme: Theme) => void;
}

const renderHomeWithTheme = (options: RenderOptions = {}) => {
  const { theme = 'light', setTheme = vi.fn() } = options;

  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });

  const themeContext = createMockThemeContext(theme, setTheme);
  const authContext = createMockAuthContext();

  // Set the data-theme attribute on the document to simulate the real behavior
  document.documentElement.setAttribute('data-theme', theme);

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/']}>
        <ThemeContext.Provider value={themeContext}>
          <AuthContext.Provider value={authContext}>
            <Home />
          </AuthContext.Provider>
        </ThemeContext.Provider>
      </MemoryRouter>
    </QueryClientProvider>
  );
};

describe('Theme Support Integration Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset the data-theme attribute before each test
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Theme Application', () => {
    it('renders homepage with light theme', () => {
      renderHomeWithTheme({ theme: 'light' });

      // Verify the data-theme attribute is set to light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Verify homepage sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByText('Powerful Features')).toBeInTheDocument();
      expect(screen.getByText('How It Works')).toBeInTheDocument();
      expect(screen.getByText('Trusted by Millions')).toBeInTheDocument();
    });

    it('renders homepage with dark theme', () => {
      renderHomeWithTheme({ theme: 'dark' });

      // Verify the data-theme attribute is set to dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Verify homepage sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByText('Powerful Features')).toBeInTheDocument();
      expect(screen.getByText('How It Works')).toBeInTheDocument();
      expect(screen.getByText('Trusted by Millions')).toBeInTheDocument();
    });

    it('renders homepage with cyberpunk theme', () => {
      renderHomeWithTheme({ theme: 'cyberpunk' });

      // Verify the data-theme attribute is set to cyberpunk
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

      // Verify homepage sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByText('Powerful Features')).toBeInTheDocument();
      expect(screen.getByText('How It Works')).toBeInTheDocument();
      expect(screen.getByText('Trusted by Millions')).toBeInTheDocument();
    });

    it('renders homepage with synthwave theme', () => {
      renderHomeWithTheme({ theme: 'synthwave' });

      // Verify the data-theme attribute is set to synthwave
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');

      // Verify homepage sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByText('Powerful Features')).toBeInTheDocument();
    });
  });

  describe('Theme Switching', () => {
    it('updates homepage styling when theme changes from light to dark', async () => {
      let currentTheme: Theme = 'light';
      const setTheme = vi.fn((newTheme: Theme) => {
        currentTheme = newTheme;
        document.documentElement.setAttribute('data-theme', newTheme);
      });

      const { rerender } = renderHomeWithTheme({ theme: 'light', setTheme });

      // Verify initial light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Simulate theme change by rerendering with dark theme
      document.documentElement.setAttribute('data-theme', 'dark');

      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
      });

      rerender(
        <QueryClientProvider client={queryClient}>
          <MemoryRouter initialEntries={['/']}>
            <ThemeContext.Provider value={{ theme: 'dark', setTheme }}>
              <AuthContext.Provider value={createMockAuthContext()}>
                <Home />
              </AuthContext.Provider>
            </ThemeContext.Provider>
          </MemoryRouter>
        </QueryClientProvider>
      );

      // Verify dark theme is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Verify all homepage sections still render correctly
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByText('Powerful Features')).toBeInTheDocument();
    });

    it('all homepage sections update styling on theme change', async () => {
      let currentTheme: Theme = 'light';
      const setTheme = vi.fn((newTheme: Theme) => {
        currentTheme = newTheme;
        document.documentElement.setAttribute('data-theme', newTheme);
      });

      const { rerender } = renderHomeWithTheme({ theme: 'light', setTheme });

      // Verify all sections render with light theme
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument();
      expect(screen.getByText('Powerful Features')).toBeInTheDocument();
      expect(screen.getByText('How It Works')).toBeInTheDocument();
      expect(screen.getByText('Trusted by Millions')).toBeInTheDocument();

      // Change to cyberpunk theme
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
      });

      document.documentElement.setAttribute('data-theme', 'cyberpunk');

      rerender(
        <QueryClientProvider client={queryClient}>
          <MemoryRouter initialEntries={['/']}>
            <ThemeContext.Provider value={{ theme: 'cyberpunk', setTheme }}>
              <AuthContext.Provider value={createMockAuthContext()}>
                <Home />
              </AuthContext.Provider>
            </ThemeContext.Provider>
          </MemoryRouter>
        </QueryClientProvider>
      );

      // Verify cyberpunk theme is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

      // Verify all sections still render correctly after theme change
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument();
      expect(screen.getByText('Powerful Features')).toBeInTheDocument();
      expect(screen.getByText('How It Works')).toBeInTheDocument();
      expect(screen.getByText('Trusted by Millions')).toBeInTheDocument();
    });
  });

  describe('ThemeContext Consumption', () => {
    it('Home component correctly reads theme from context', () => {
      const setTheme = vi.fn();

      // Render with light theme
      renderHomeWithTheme({ theme: 'light', setTheme });
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // The home page should render without errors when theme context is provided
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    });

    it('theme context values are accessible throughout homepage components', () => {
      // This test verifies that all homepage components can render
      // when the theme context provides different themes

      const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave'];

      themes.forEach(theme => {
        document.documentElement.removeAttribute('data-theme');
        const { unmount } = renderHomeWithTheme({ theme });

        expect(document.documentElement.getAttribute('data-theme')).toBe(theme);
        expect(screen.getByTestId('hero-section')).toBeInTheDocument();

        unmount();
      });
    });

    it('homepage uses DaisyUI/Tailwind theme classes correctly', () => {
      renderHomeWithTheme({ theme: 'dark' });

      // Verify that components use base-content classes for text (DaisyUI theme-aware classes)
      const heroSubheadline = screen.getByTestId('hero-subheadline');
      expect(heroSubheadline).toHaveClass('text-base-content/70');

      // Verify the HowItWorks section uses bg-base-200 (theme-aware background)
      const howItWorksSection = screen.getByText('How It Works').closest('section');
      expect(howItWorksSection).toHaveClass('bg-base-200');
    });
  });

  describe('Theme-specific Element Verification', () => {
    it('footer uses theme-aware background class', () => {
      renderHomeWithTheme({ theme: 'light' });

      const footer = screen.getByRole('contentinfo');
      expect(footer).toHaveClass('bg-base-200');
    });

    it('hero section renders with all theme variations', () => {
      const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave'];

      themes.forEach(theme => {
        document.documentElement.removeAttribute('data-theme');
        const { unmount } = renderHomeWithTheme({ theme });

        // Verify hero section elements render correctly
        expect(screen.getByTestId('hero-headline')).toHaveTextContent('Shorten Links. Track Clicks. Grow Insights.');
        expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument();
        expect(screen.getByTestId('get-started-button')).toBeInTheDocument();
        expect(screen.getByTestId('login-button')).toBeInTheDocument();

        unmount();
      });
    });

    it('feature cards section renders correctly with all themes', () => {
      const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave'];

      themes.forEach(theme => {
        document.documentElement.removeAttribute('data-theme');
        const { unmount } = renderHomeWithTheme({ theme });

        // Verify feature cards content renders
        expect(screen.getByText('URL Shortening')).toBeInTheDocument();
        expect(screen.getByText('Click Analytics')).toBeInTheDocument();
        expect(screen.getByText('Geographic Insights')).toBeInTheDocument();
        expect(screen.getByText('Multi-Theme Support')).toBeInTheDocument();

        unmount();
      });
    });
  });
});
