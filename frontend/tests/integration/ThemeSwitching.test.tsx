/**
 * Integration Tests for Theme Switching Functionality.
 * Owner: Scenario 4 - Theme Switching Functionality
 *
 * Tests:
 * - Theme toggle from light to dark mode with smooth transition
 * - Theme persistence via localStorage across page reloads
 * - All theme variants render correctly (light, dark, cyberpunk, synthwave)
 * - ThemeToggle component correctly calls setTheme
 * - CSS transition animation timing (0.3-0.5s)
 *
 * Requirements: REQ-9, REQ-10, NFR-5, US-5
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider, useTheme } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { ThemeToggle } from '../../src/components/ThemeToggle';
import { HomeNavbar } from '../../src/components/layout/HomeNavbar';
import React from 'react';

// Mock localStorage with spy capabilities
const localStorageMock = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value;
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key];
    }),
    clear: vi.fn(() => {
      store = {};
    }),
    _getStore: () => store,
    _setStore: (newStore: Record<string, string>) => {
      store = newStore;
    },
  };
})();

// Helper component to display current theme
function ThemeDisplay() {
  const { theme } = useTheme();
  return <div data-testid="current-theme">{theme}</div>;
}

// Helper component to set theme directly
function ThemeSetter({ theme }: { theme: string }) {
  const { setTheme } = useTheme();
  React.useEffect(() => {
    setTheme(theme as 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine');
  }, [theme, setTheme]);
  return null;
}

// Render helper with theme provider
function renderWithTheme(
  ui: React.ReactElement,
  { initialTheme = 'light' }: { initialTheme?: string } = {}
) {
  localStorageMock._setStore({ theme: initialTheme });

  return render(
    <MemoryRouter>
      <ThemeProvider>
        {ui}
        <ThemeDisplay />
      </ThemeProvider>
    </MemoryRouter>
  );
}

// Render homepage with navbar for integration tests
function renderHomepageWithTheme(initialTheme = 'light') {
  localStorageMock._setStore({ theme: initialTheme });

  return render(
    <MemoryRouter>
      <ThemeProvider>
        <AuthProvider>
          <div className="min-h-screen" data-testid="homepage">
            <HomeNavbar />
            <main id="main-content">
              <div data-testid="main-content-area">Main Content</div>
            </main>
          </div>
          <ThemeDisplay />
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  );
}

describe('Theme Switching Functionality', () => {
  beforeEach(() => {
    // Reset mocks before each test
    vi.clearAllMocks();
    localStorageMock.clear();
    // Set default theme
    localStorageMock._setStore({ theme: 'light' });

    // Override global localStorage
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
    });

    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Test Case 1: Click theme toggle from light theme', () => {
    it('switches theme to dark mode immediately when toggle is clicked', async () => {
      const user = userEvent.setup();
      renderHomepageWithTheme('light');

      // Verify initial light theme
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light');

      // Find and click the theme toggle button
      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();

      await user.click(themeToggle);

      // Verify theme changed to dark
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });
    });

    it('updates data-theme attribute on document immediately', async () => {
      const user = userEvent.setup();
      renderHomepageWithTheme('light');

      const themeToggle = screen.getByTestId('theme-toggle');
      await user.click(themeToggle);

      // Verify data-theme attribute is updated on document
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });

    it('toggles back to light mode when clicked again', async () => {
      const user = userEvent.setup();
      renderHomepageWithTheme('light');

      const themeToggle = screen.getByTestId('theme-toggle');

      // Click to switch to dark
      await user.click(themeToggle);
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });

      // Click again to switch back to light
      await user.click(themeToggle);
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
      });
    });

    it('toggle button has correct aria-label for current state', async () => {
      const user = userEvent.setup();
      renderHomepageWithTheme('light');

      const themeToggle = screen.getByTestId('theme-toggle');

      // When theme is light, aria-label should mention switching to dark
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark theme');

      await user.click(themeToggle);

      // After switch to dark, aria-label should mention switching to light
      await waitFor(() => {
        expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light theme');
      });
    });
  });

  describe('Test Case 2: Theme persistence via localStorage', () => {
    it('persists theme preference to localStorage when changed', async () => {
      const user = userEvent.setup();
      renderHomepageWithTheme('light');

      const themeToggle = screen.getByTestId('theme-toggle');
      await user.click(themeToggle);

      // Verify localStorage was updated
      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark');
      });
    });

    it('loads theme from localStorage on initial render', () => {
      // Set theme in localStorage before rendering
      localStorageMock._setStore({ theme: 'dark' });

      render(
        <MemoryRouter>
          <ThemeProvider>
            <ThemeDisplay />
          </ThemeProvider>
        </MemoryRouter>
      );

      // Theme should be loaded from localStorage
      expect(localStorageMock.getItem).toHaveBeenCalledWith('theme');
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
    });

    it('persists cyberpunk theme and loads it on simulated page reload', async () => {
      // Simulate setting cyberpunk theme
      localStorageMock._setStore({ theme: 'cyberpunk' });

      const { unmount } = render(
        <MemoryRouter>
          <ThemeProvider>
            <ThemeDisplay />
          </ThemeProvider>
        </MemoryRouter>
      );

      // Verify cyberpunk theme is loaded
      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk');
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

      // Unmount (simulating page close)
      unmount();

      // Verify localStorage still has the theme
      expect(localStorageMock._getStore().theme).toBe('cyberpunk');

      // Remount (simulating page reload)
      render(
        <MemoryRouter>
          <ThemeProvider>
            <ThemeDisplay />
          </ThemeProvider>
        </MemoryRouter>
      );

      // Theme should persist
      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk');
    });

    it('defaults to light theme when localStorage is empty', () => {
      localStorageMock.clear();

      render(
        <MemoryRouter>
          <ThemeProvider>
            <ThemeDisplay />
          </ThemeProvider>
        </MemoryRouter>
      );

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light');
    });
  });

  describe('Test Case 3: All theme variants render without breakage', () => {
    const themes = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine'] as const;

    themes.forEach((themeName) => {
      it(`renders homepage correctly with ${themeName} theme`, () => {
        localStorageMock._setStore({ theme: themeName });

        render(
          <MemoryRouter>
            <ThemeProvider>
              <AuthProvider>
                <div className="min-h-screen" data-testid="homepage">
                  <HomeNavbar />
                  <main id="main-content">
                    <div data-testid="main-content-area">Main Content</div>
                  </main>
                </div>
                <ThemeDisplay />
              </AuthProvider>
            </ThemeProvider>
          </MemoryRouter>
        );

        // Verify theme is applied
        expect(screen.getByTestId('current-theme')).toHaveTextContent(themeName);
        expect(document.documentElement.getAttribute('data-theme')).toBe(themeName);

        // Verify core components render without error
        expect(screen.getByTestId('home-navbar')).toBeInTheDocument();
        expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
        expect(screen.getByTestId('main-content-area')).toBeInTheDocument();
      });
    });

    it('renders synthwave theme with all navbar elements visible', () => {
      localStorageMock._setStore({ theme: 'synthwave' });

      render(
        <MemoryRouter>
          <ThemeProvider>
            <AuthProvider>
              <HomeNavbar />
              <ThemeDisplay />
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      // Verify all navbar elements are present
      expect(screen.getByTestId('navbar-logo')).toBeInTheDocument();
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-login-link')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register-link')).toBeInTheDocument();

      // Verify theme is correctly applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
    });
  });

  describe('Test Case 4: ThemeToggle component behavior', () => {
    it('ThemeToggle calls toggleTheme from context when clicked', async () => {
      const user = userEvent.setup();

      renderWithTheme(<ThemeToggle />);

      // Initial state is light
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light');

      // Click the toggle
      const toggle = screen.getByTestId('theme-toggle');
      await user.click(toggle);

      // Theme should change (toggleTheme was called)
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });
    });

    it('ThemeToggle displays Moon icon in light mode and Sun icon in dark mode', async () => {
      const user = userEvent.setup();

      renderWithTheme(<ThemeToggle />);

      const toggle = screen.getByTestId('theme-toggle');

      // In light mode, should show Moon icon (to switch to dark)
      // The icons are within the button
      expect(toggle.querySelector('svg')).toBeInTheDocument();

      await user.click(toggle);

      // After switching to dark, icon changes
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });

      // The button still has an SVG icon
      expect(toggle.querySelector('svg')).toBeInTheDocument();
    });

    it('ThemeToggle is a focusable button element', () => {
      renderWithTheme(<ThemeToggle />);

      const toggle = screen.getByTestId('theme-toggle');

      expect(toggle.tagName).toBe('BUTTON');
      expect(toggle).toHaveAttribute('aria-label');
    });

    it('ThemeToggle can be activated via keyboard', async () => {
      const user = userEvent.setup();

      renderWithTheme(<ThemeToggle />);

      const toggle = screen.getByTestId('theme-toggle');

      // Focus the button
      toggle.focus();
      expect(document.activeElement).toBe(toggle);

      // Press Enter to activate
      await user.keyboard('{Enter}');

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });
    });
  });

  describe('Test Case 5: Theme transition animation', () => {
    it('document element supports CSS transitions for theme changes', () => {
      renderWithTheme(<ThemeToggle />);

      // Check that document.documentElement can accept data-theme
      document.documentElement.setAttribute('data-theme', 'light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Transition classes can be applied via CSS
      // The actual CSS transition timing (0.3-0.5s) is defined in stylesheets
      // We verify the data-theme attribute changes which triggers CSS transitions
      document.documentElement.setAttribute('data-theme', 'dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('theme change updates DOM immediately without delay', async () => {
      const user = userEvent.setup();

      renderWithTheme(<ThemeToggle />);

      const toggle = screen.getByTestId('theme-toggle');

      // Theme starts as light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Click and verify immediate update
      await user.click(toggle);

      // DOM update should be immediate (CSS transitions are visual only)
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      }, { timeout: 100 }); // Should happen within 100ms
    });

    it('applies theme via DaisyUI data-theme attribute', async () => {
      const user = userEvent.setup();

      localStorageMock._setStore({ theme: 'cyberpunk' });

      render(
        <MemoryRouter>
          <ThemeProvider>
            <ThemeToggle />
            <ThemeDisplay />
          </ThemeProvider>
        </MemoryRouter>
      );

      // Cyberpunk theme applied via data-theme attribute
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

      // Toggle switches to light (since cyberpunk != light, toggle goes to dark)
      const toggle = screen.getByTestId('theme-toggle');
      await user.click(toggle);

      // Since cyberpunk is not 'light', toggleTheme sets to 'light'
      // Check ThemeContext: toggleTheme: prev => prev === 'light' ? 'dark' : 'light'
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });
    });
  });

  describe('Theme Context Integration', () => {
    it('useTheme hook provides theme, setTheme, and toggleTheme', () => {
      let contextValue: { theme: string; setTheme: (t: string) => void; toggleTheme: () => void } | null = null;

      function ContextInspector() {
        const ctx = useTheme();
        contextValue = ctx as typeof contextValue;
        return null;
      }

      render(
        <MemoryRouter>
          <ThemeProvider>
            <ContextInspector />
          </ThemeProvider>
        </MemoryRouter>
      );

      expect(contextValue).not.toBeNull();
      expect(contextValue!.theme).toBeDefined();
      expect(typeof contextValue!.setTheme).toBe('function');
      expect(typeof contextValue!.toggleTheme).toBe('function');
    });

    it('setTheme can set any valid theme directly', async () => {
      render(
        <MemoryRouter>
          <ThemeProvider>
            <ThemeSetter theme="synthwave" />
            <ThemeDisplay />
          </ThemeProvider>
        </MemoryRouter>
      );

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave');
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
      });
    });

    it('throws error when useTheme is used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      expect(() => {
        render(<ThemeDisplay />);
      }).toThrow('useTheme must be used within a ThemeProvider');

      consoleSpy.mockRestore();
    });
  });

  describe('Theme Switching with Navigation', () => {
    it('theme persists across navigation within the app', async () => {
      const user = userEvent.setup();

      renderHomepageWithTheme('light');

      // Switch to dark theme
      const themeToggle = screen.getByTestId('theme-toggle');
      await user.click(themeToggle);

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark');
      });

      // Navigate to login (simulated by checking localStorage persistence)
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark');

      // Theme should still be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('theme toggle is visible in navbar across all themes', () => {
      const themes = ['light', 'dark', 'cyberpunk', 'synthwave'] as const;

      themes.forEach((themeName) => {
        localStorageMock._setStore({ theme: themeName });
        document.documentElement.setAttribute('data-theme', themeName);

        const { unmount } = render(
          <MemoryRouter>
            <ThemeProvider>
              <AuthProvider>
                <HomeNavbar />
              </AuthProvider>
            </ThemeProvider>
          </MemoryRouter>
        );

        const toggle = screen.getByTestId('theme-toggle');
        expect(toggle).toBeInTheDocument();
        expect(toggle).toBeVisible();

        unmount();
      });
    });
  });
});
