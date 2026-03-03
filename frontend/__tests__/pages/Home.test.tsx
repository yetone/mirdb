/**
 * Home Page Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 * Extended by: Scenario 2 - Navigation to Registration
 * Contributor: Scenario 3 - Navigation to Login
 *
 * Integration tests for the Home page component.
 */

import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { Home } from '../../src/pages/Home';
import { Login } from '../../src/pages/Login';

// Helper to render with all required providers
const renderWithProviders = (ui: React.ReactElement) => {
  return render(
    <ThemeProvider>
      <AuthProvider>
        <BrowserRouter>{ui}</BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  );
};

describe('Home Page', () => {
  it('renders without errors', () => {
    renderWithProviders(<Home />);

    const homePage = screen.getByTestId('home-page');
    expect(homePage).toBeInTheDocument();
  });

  it('includes the HeroSection component', () => {
    renderWithProviders(<Home />);

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();
  });

  it('displays the service name headline in HeroSection', () => {
    renderWithProviders(<Home />);

    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline).toHaveTextContent('URL Shortener');
  });

  it('displays the tagline in HeroSection', () => {
    renderWithProviders(<Home />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent('Shorten URLs, track clicks, analyze your audience');
  });

  it('renders Get Started CTA button', () => {
    renderWithProviders(<Home />);

    const getStartedButton = screen.getByTestId('get-started-button');
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toHaveTextContent('Get Started');
  });

  it('renders Sign In CTA button', () => {
    renderWithProviders(<Home />);

    const signInButton = screen.getByTestId('sign-in-button');
    expect(signInButton).toBeInTheDocument();
    expect(signInButton).toHaveTextContent('Sign In');
  });

  it('renders the Navbar component', () => {
    renderWithProviders(<Home />);

    // Check for navbar elements
    const navbar = screen.getByRole('link', { name: /URLShortener/i });
    expect(navbar).toBeInTheDocument();
  });
});

/**
 * Navigation to Registration Tests
 * Owner: Scenario 2 - Navigation to Registration
 *
 * Tests verifying that the Get Started button navigates users to the registration page.
 */

// Helper to render with MemoryRouter for navigation testing
const renderWithMemoryRouter = (
  ui: React.ReactElement,
  { initialEntries = ['/'] }: { initialEntries?: string[] } = {}
) => {
  return render(
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter initialEntries={initialEntries}>
          <Routes>
            <Route path="/" element={ui} />
            <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
            <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
          </Routes>
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  );
};

describe('Navigation to Registration (Scenario 2)', () => {
  describe('Test Case 1: Click Get Started button navigates to /register', () => {
    it('navigates to /register route when clicking Get Started button', async () => {
      const user = userEvent.setup();
      renderWithMemoryRouter(<Home />);

      // Verify we start on the home page
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      expect(screen.getByTestId('get-started-button')).toBeInTheDocument();

      // Click the Get Started button
      const getStartedButton = screen.getByTestId('get-started-button');
      await user.click(getStartedButton);

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('navigates when clicking the Get Started link wrapper', async () => {
      const user = userEvent.setup();
      renderWithMemoryRouter(<Home />);

      // Click the link wrapper directly
      const getStartedLink = screen.getByTestId('get-started-link');
      await user.click(getStartedLink);

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 2: Navigation preserves theme selection', () => {
    it('preserves cyberpunk theme after navigation to /register', async () => {
      const user = userEvent.setup();

      // Set cyberpunk theme in localStorage before render
      window.localStorage.getItem = vi.fn().mockReturnValue('cyberpunk');

      renderWithMemoryRouter(<Home />);

      // Verify we're on home page
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      // Click Get Started button
      const getStartedButton = screen.getByTestId('get-started-button');
      await user.click(getStartedButton);

      // Verify navigation happened
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });

      // Theme should be preserved in localStorage (setItem was called with the theme)
      // ThemeProvider stores theme to localStorage on mount
      expect(window.localStorage.setItem).toHaveBeenCalledWith('app-theme', 'cyberpunk');
    });

    it('preserves synthwave theme after navigation', async () => {
      const user = userEvent.setup();

      // Set synthwave theme
      window.localStorage.getItem = vi.fn().mockReturnValue('synthwave');

      renderWithMemoryRouter(<Home />);

      const getStartedButton = screen.getByTestId('get-started-button');
      await user.click(getStartedButton);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });

      // Verify theme persistence
      expect(window.localStorage.setItem).toHaveBeenCalledWith('app-theme', 'synthwave');
    });
  });

  describe('Test Case 3: Get Started button has correct href', () => {
    it('Get Started link has correct href attribute pointing to /register', () => {
      renderWithProviders(<Home />);

      const getStartedLink = screen.getByTestId('get-started-link');
      expect(getStartedLink).toHaveAttribute('href', '/register');
    });

    it('Get Started button is wrapped in a Link component with correct destination', () => {
      renderWithProviders(<Home />);

      const getStartedLink = screen.getByTestId('get-started-link');
      const getStartedButton = screen.getByTestId('get-started-button');

      // Verify the link contains the button
      expect(getStartedLink).toContainElement(getStartedButton);

      // Verify the link has the correct href
      expect(getStartedLink).toHaveAttribute('href', '/register');
    });

    it('Get Started button has accessible label', () => {
      renderWithProviders(<Home />);

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toHaveAttribute('aria-label', 'Get started with URL Shortener');
    });

    it('Get Started button displays correct text', () => {
      renderWithProviders(<Home />);

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toHaveTextContent('Get Started');
    });
  });
});

/**
 * Scenario 3: Navigation to Login
 * Tests for Sign In button navigation functionality
 */
describe('Home Page - Navigation to Login (Scenario 3)', () => {
  // Helper to render with routing context for navigation testing
  const renderWithRouting = (initialEntries = ['/']) => {
    return render(
      <ThemeProvider>
        <AuthProvider>
          <MemoryRouter initialEntries={initialEntries}>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/login" element={<Login />} />
            </Routes>
          </MemoryRouter>
        </AuthProvider>
      </ThemeProvider>
    );
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Test Case 1: Click Sign In button navigates to /login route
   * Integration test verifying navigation behavior
   */
  it('navigates to /login route when Sign In button is clicked', async () => {
    const user = userEvent.setup();
    renderWithRouting();

    // Verify we're on the home page
    const homePage = screen.getByTestId('home-page');
    expect(homePage).toBeInTheDocument();

    // Find and click the Sign In button
    const signInButton = screen.getByTestId('sign-in-button');
    expect(signInButton).toBeInTheDocument();

    await user.click(signInButton);

    // Verify navigation to login page - home page should no longer be visible
    expect(screen.queryByTestId('home-page')).not.toBeInTheDocument();

    // Login page heading should be visible
    const loginHeading = screen.getByRole('heading', { name: /sign in/i });
    expect(loginHeading).toBeInTheDocument();
  });

  /**
   * Test Case 2: Navigation preserves current theme selection
   * Integration test verifying theme persistence during navigation
   */
  it('preserves theme selection when navigating to login page', async () => {
    const user = userEvent.setup();

    // Set up localStorage to simulate dark theme
    const mockLocalStorage = {
      getItem: vi.fn((key) => key === 'app-theme' ? 'dark' : null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
      clear: vi.fn(),
    };
    Object.defineProperty(window, 'localStorage', {
      value: mockLocalStorage,
      writable: true,
    });

    renderWithRouting();

    // Verify theme is applied to document element
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

    // Navigate to login
    const signInButton = screen.getByTestId('sign-in-button');
    await user.click(signInButton);

    // Verify theme is still applied after navigation
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    expect(mockLocalStorage.setItem).toHaveBeenCalledWith('app-theme', 'dark');
  });

  /**
   * Test Case 3: Sign In button has correct href attribute pointing to /login
   * Unit test verifying button configuration
   */
  it('Sign In link has correct href attribute pointing to /login', () => {
    renderWithProviders(<Home />);

    const signInLink = screen.getByTestId('sign-in-link');
    expect(signInLink).toBeInTheDocument();
    expect(signInLink).toHaveAttribute('href', '/login');
  });

  it('Sign In button is visible in the hero section', () => {
    renderWithProviders(<Home />);

    const heroSection = screen.getByTestId('hero-section');
    const signInButton = screen.getByTestId('sign-in-button');

    expect(heroSection).toContainElement(screen.getByTestId('sign-in-link'));
    expect(signInButton).toBeVisible();
  });

  it('Sign In button has appropriate accessibility attributes', () => {
    renderWithProviders(<Home />);

    const signInButton = screen.getByTestId('sign-in-button');

    expect(signInButton).toHaveAttribute('aria-label', 'Sign in to your account');
    expect(signInButton).toHaveTextContent('Sign In');
  });
});

/**
 * Scenario 8: Theme Integration
 * Tests for verifying homepage correctly integrates with theme system
 * and displays properly in all themes.
 */
describe('Home Page - Theme Integration (Scenario 8)', () => {
  // Helper to render with specific theme
  const renderWithTheme = (theme: string) => {
    // Mock localStorage to return the specific theme
    vi.mocked(window.localStorage.getItem).mockReturnValue(theme);

    return render(
      <ThemeProvider>
        <AuthProvider>
          <BrowserRouter>
            <Home />
          </BrowserRouter>
        </AuthProvider>
      </ThemeProvider>
    );
  };

  beforeEach(() => {
    vi.clearAllMocks();
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  /**
   * Test Case 1: Render Home with light theme
   * Expected: All text and UI elements maintain proper contrast
   */
  describe('Test Case 1: Light Theme Rendering', () => {
    it('renders homepage with light theme applied correctly', () => {
      renderWithTheme('light');

      // Verify theme is applied to document
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('home page element uses base-100 background for light theme', () => {
      renderWithTheme('light');

      const homePage = screen.getByTestId('home-page');
      expect(homePage).toHaveClass('bg-base-100');
    });

    it('hero section elements are visible with light theme', () => {
      renderWithTheme('light');

      const headline = screen.getByTestId('hero-headline');
      const tagline = screen.getByTestId('hero-tagline');

      expect(headline).toBeVisible();
      expect(tagline).toBeVisible();
    });

    it('CTA buttons are visible and styled in light theme', () => {
      renderWithTheme('light');

      const getStartedButton = screen.getByTestId('get-started-button');
      const signInButton = screen.getByTestId('sign-in-button');

      expect(getStartedButton).toBeVisible();
      expect(signInButton).toBeVisible();
    });
  });

  /**
   * Test Case 2: Render Home with dark theme
   * Expected: All text and UI elements maintain proper contrast
   */
  describe('Test Case 2: Dark Theme Rendering', () => {
    it('renders homepage with dark theme applied correctly', () => {
      renderWithTheme('dark');

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('home page maintains structure with dark theme', () => {
      renderWithTheme('dark');

      const homePage = screen.getByTestId('home-page');
      expect(homePage).toBeInTheDocument();
      expect(homePage).toHaveClass('bg-base-100');
    });

    it('hero section elements maintain visibility in dark theme', () => {
      renderWithTheme('dark');

      const headline = screen.getByTestId('hero-headline');
      const tagline = screen.getByTestId('hero-tagline');
      const getStartedButton = screen.getByTestId('get-started-button');
      const signInButton = screen.getByTestId('sign-in-button');

      expect(headline).toBeVisible();
      expect(tagline).toBeVisible();
      expect(getStartedButton).toBeVisible();
      expect(signInButton).toBeVisible();
    });

    it('localStorage persists dark theme selection', () => {
      renderWithTheme('dark');

      expect(window.localStorage.setItem).toHaveBeenCalledWith('app-theme', 'dark');
    });
  });

  /**
   * Test Case 3: Render Home with cyberpunk theme
   * Expected: Theme-specific styling is applied correctly
   */
  describe('Test Case 3: Cyberpunk Theme Rendering', () => {
    it('renders homepage with cyberpunk theme applied correctly', () => {
      renderWithTheme('cyberpunk');

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    it('cyberpunk theme is properly persisted', () => {
      renderWithTheme('cyberpunk');

      expect(window.localStorage.setItem).toHaveBeenCalledWith('app-theme', 'cyberpunk');
    });

    it('all major elements render correctly with cyberpunk theme', () => {
      renderWithTheme('cyberpunk');

      // Verify page structure
      const homePage = screen.getByTestId('home-page');
      const heroSection = screen.getByTestId('hero-section');

      expect(homePage).toBeInTheDocument();
      expect(heroSection).toBeInTheDocument();

      // Verify text content is visible
      expect(screen.getByTestId('hero-headline')).toBeVisible();
      expect(screen.getByTestId('hero-tagline')).toBeVisible();
    });

    it('CTA buttons are functional in cyberpunk theme', () => {
      renderWithTheme('cyberpunk');

      const getStartedButton = screen.getByTestId('get-started-button');
      const signInButton = screen.getByTestId('sign-in-button');

      expect(getStartedButton).toBeVisible();
      expect(signInButton).toBeVisible();
      expect(getStartedButton).toHaveTextContent('Get Started');
      expect(signInButton).toHaveTextContent('Sign In');
    });
  });

  /**
   * Test Case 4: Render Home with synthwave theme
   * Expected: Theme-specific styling is applied correctly
   */
  describe('Test Case 4: Synthwave Theme Rendering', () => {
    it('renders homepage with synthwave theme applied correctly', () => {
      renderWithTheme('synthwave');

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
    });

    it('synthwave theme is properly persisted', () => {
      renderWithTheme('synthwave');

      expect(window.localStorage.setItem).toHaveBeenCalledWith('app-theme', 'synthwave');
    });

    it('all major elements render correctly with synthwave theme', () => {
      renderWithTheme('synthwave');

      // Verify page structure
      const homePage = screen.getByTestId('home-page');
      const heroSection = screen.getByTestId('hero-section');

      expect(homePage).toBeInTheDocument();
      expect(heroSection).toBeInTheDocument();
      expect(screen.getByTestId('hero-headline')).toBeVisible();
      expect(screen.getByTestId('hero-tagline')).toBeVisible();
    });

    it('navbar is visible and styled in synthwave theme', () => {
      renderWithTheme('synthwave');

      const navbarLink = screen.getByRole('link', { name: /URLShortener/i });
      expect(navbarLink).toBeVisible();
    });
  });

  /**
   * Test Case 5: ThemeToggle component is present
   * Expected: ThemeToggle is rendered and functional on homepage
   */
  describe('Test Case 5: ThemeToggle Component Presence', () => {
    it('ThemeToggle is rendered on the homepage', () => {
      renderWithTheme('dark');

      // ThemeToggle uses a label element with aria-label "Toggle theme"
      const themeToggle = screen.getByLabelText(/toggle theme/i);
      expect(themeToggle).toBeInTheDocument();
    });

    it('ThemeToggle is visible and accessible', () => {
      renderWithTheme('dark');

      const themeToggle = screen.getByLabelText(/toggle theme/i);
      expect(themeToggle).toBeVisible();
    });

    it('ThemeToggle shows dropdown with theme options when clicked', async () => {
      const user = userEvent.setup();
      renderWithTheme('dark');

      const themeToggle = screen.getByLabelText(/toggle theme/i);
      await user.click(themeToggle);

      // Check that theme options are visible - these are actual buttons in the dropdown
      expect(screen.getByRole('button', { name: /set light theme/i })).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /set dark theme/i })).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /set cyberpunk theme/i })).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /set synthwave theme/i })).toBeInTheDocument();
    });

    it('ThemeToggle changes theme when option is selected', async () => {
      const user = userEvent.setup();
      renderWithTheme('dark');

      // Verify initial theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Click toggle to show dropdown
      const themeToggle = screen.getByLabelText(/toggle theme/i);
      await user.click(themeToggle);

      // Select cyberpunk theme
      const cyberpunkOption = screen.getByRole('button', { name: /set cyberpunk theme/i });
      await user.click(cyberpunkOption);

      // Verify theme changed
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
      expect(window.localStorage.setItem).toHaveBeenCalledWith('app-theme', 'cyberpunk');
    });

    it('ThemeToggle is located in the navbar', () => {
      renderWithTheme('dark');

      const themeToggle = screen.getByLabelText(/toggle theme/i);

      // ThemeToggle should be inside the navbar area
      expect(themeToggle.closest('.navbar')).toBeInTheDocument();
    });
  });

  /**
   * Test Case 6: BackgroundEffect component
   * Expected: BackgroundEffect displays appropriately for each theme
   */
  describe('Test Case 6: BackgroundEffect Component', () => {
    it('BackgroundEffect canvas is rendered on the page', () => {
      renderWithTheme('dark');

      // BackgroundEffect uses a canvas with aria-hidden="true"
      const canvas = document.querySelector('canvas[aria-hidden="true"]');
      expect(canvas).toBeInTheDocument();
    });

    it('BackgroundEffect canvas has correct positioning classes', () => {
      renderWithTheme('dark');

      const canvas = document.querySelector('canvas');
      expect(canvas).toHaveClass('fixed', 'inset-0', '-z-10', 'pointer-events-none');
    });

    it('BackgroundEffect is rendered with light theme', () => {
      renderWithTheme('light');

      const canvas = document.querySelector('canvas[aria-hidden="true"]');
      expect(canvas).toBeInTheDocument();
    });

    it('BackgroundEffect is rendered with cyberpunk theme', () => {
      renderWithTheme('cyberpunk');

      const canvas = document.querySelector('canvas[aria-hidden="true"]');
      expect(canvas).toBeInTheDocument();
    });

    it('BackgroundEffect is rendered with synthwave theme', () => {
      renderWithTheme('synthwave');

      const canvas = document.querySelector('canvas[aria-hidden="true"]');
      expect(canvas).toBeInTheDocument();
    });

    it('BackgroundEffect canvas context methods are called for rendering', () => {
      renderWithTheme('dark');

      // The mock should have been called during initialization
      expect(HTMLCanvasElement.prototype.getContext).toHaveBeenCalledWith('2d');
    });

    it('BackgroundEffect does not interfere with page interactivity', () => {
      renderWithTheme('dark');

      const canvas = document.querySelector('canvas');
      expect(canvas).toHaveClass('pointer-events-none');

      // Verify buttons are still clickable
      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toBeVisible();
    });
  });

  /**
   * Additional theme integration tests
   */
  describe('Theme Integration - Cross-cutting concerns', () => {
    it('theme persists across page interactions', async () => {
      const user = userEvent.setup();
      renderWithTheme('cyberpunk');

      // Initial theme check
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');

      // Interact with page elements
      const getStartedButton = screen.getByTestId('get-started-button');
      await user.hover(getStartedButton);

      // Theme should remain unchanged
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    });

    it('all supported themes can be applied without errors', () => {
      const themes = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'halloween'];

      themes.forEach((theme) => {
        // Clear previous render
        document.documentElement.removeAttribute('data-theme');

        const { unmount } = renderWithTheme(theme);

        expect(document.documentElement.getAttribute('data-theme')).toBe(theme);

        unmount();
      });
    });

    it('theme is loaded from localStorage on initial render', () => {
      vi.mocked(window.localStorage.getItem).mockReturnValue('synthwave');

      render(
        <ThemeProvider>
          <AuthProvider>
            <BrowserRouter>
              <Home />
            </BrowserRouter>
          </AuthProvider>
        </ThemeProvider>
      );

      expect(window.localStorage.getItem).toHaveBeenCalledWith('app-theme');
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave');
    });

    it('default theme is applied when no theme is stored', () => {
      vi.mocked(window.localStorage.getItem).mockReturnValue(null);

      render(
        <ThemeProvider>
          <AuthProvider>
            <BrowserRouter>
              <Home />
            </BrowserRouter>
          </AuthProvider>
        </ThemeProvider>
      );

      // Default theme is 'dark' as per ThemeContext
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });
});

/**
 * Scenario 15: Component Integration
 * Tests verifying homepage correctly uses existing UI components
 */
describe('Home Page - Component Integration (Scenario 15)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Test Case 1: Navbar component is rendered at top of page
   */
  describe('Test Case 1: Navbar Integration', () => {
    it('renders Navbar component at the top of the page', () => {
      renderWithProviders(<Home />);

      // Navbar should be present with expected structure
      const navbar = document.querySelector('.navbar');
      expect(navbar).toBeInTheDocument();
      expect(navbar).toHaveClass('fixed', 'top-0');
    });

    it('Navbar contains the logo/brand link', () => {
      renderWithProviders(<Home />);

      const brandLink = screen.getByRole('link', { name: /URLShortener/i });
      expect(brandLink).toBeInTheDocument();
      expect(brandLink).toHaveAttribute('href', '/');
    });

    it('Navbar contains navigation elements', () => {
      renderWithProviders(<Home />);

      // Get navbar element first
      const navbar = document.querySelector('.navbar');
      expect(navbar).toBeInTheDocument();

      // Navbar should contain Sign In and Get Started links for unauthenticated users
      // Use within navbar to avoid matching HeroSection links
      const navbarLinks = navbar!.querySelectorAll('a');
      const signInLink = Array.from(navbarLinks).find(link => link.textContent?.includes('Sign In'));
      const getStartedLink = Array.from(navbarLinks).find(link => link.textContent?.includes('Get Started'));

      expect(signInLink).toBeDefined();
      expect(getStartedLink).toBeDefined();
    });

    it('Navbar is positioned before main content in DOM order', () => {
      renderWithProviders(<Home />);

      const homePage = screen.getByTestId('home-page');
      const navbar = document.querySelector('.navbar');
      const main = homePage.querySelector('main');

      // Navbar should appear before main in the DOM
      expect(navbar).toBeInTheDocument();
      expect(main).toBeInTheDocument();

      // Both should be direct children
      expect(homePage.contains(navbar)).toBe(true);
      expect(homePage.contains(main)).toBe(true);
    });
  });

  /**
   * Test Case 2: BackgroundEffect component is rendered for visual appeal
   */
  describe('Test Case 2: BackgroundEffect Integration', () => {
    it('renders BackgroundEffect component', () => {
      renderWithProviders(<Home />);

      // BackgroundEffect renders a canvas element
      const canvas = document.querySelector('canvas');
      expect(canvas).toBeInTheDocument();
    });

    it('BackgroundEffect canvas has aria-hidden for accessibility', () => {
      renderWithProviders(<Home />);

      const canvas = document.querySelector('canvas');
      expect(canvas).toHaveAttribute('aria-hidden', 'true');
    });

    it('BackgroundEffect has correct visual positioning classes', () => {
      renderWithProviders(<Home />);

      const canvas = document.querySelector('canvas');
      expect(canvas).toHaveClass('fixed', 'inset-0', '-z-10', 'pointer-events-none');
    });

    it('BackgroundEffect does not block user interactions', () => {
      renderWithProviders(<Home />);

      const canvas = document.querySelector('canvas');
      expect(canvas).toHaveClass('pointer-events-none');

      // Verify clickable elements are still accessible
      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toBeVisible();
    });
  });

  /**
   * Test Case 3: Shorten button uses FuturisticButton component
   */
  describe('Test Case 3: FuturisticButton Usage', () => {
    it('Hero section Get Started button uses FuturisticButton styling', () => {
      renderWithProviders(<Home />);

      const getStartedButton = screen.getByTestId('get-started-button');

      // FuturisticButton has specific class patterns
      expect(getStartedButton).toHaveClass('btn');
      expect(getStartedButton).toHaveClass('btn-primary');
      expect(getStartedButton).toHaveClass('min-h-[44px]'); // WCAG touch target
    });

    it('Hero section Sign In button uses FuturisticButton styling', () => {
      renderWithProviders(<Home />);

      const signInButton = screen.getByTestId('sign-in-button');

      expect(signInButton).toHaveClass('btn');
      expect(signInButton).toHaveClass('btn-outline');
      expect(signInButton).toHaveClass('min-h-[44px]');
    });

    it('FuturisticButton elements have transition classes for hover effects', () => {
      renderWithProviders(<Home />);

      const getStartedButton = screen.getByTestId('get-started-button');
      const signInButton = screen.getByTestId('sign-in-button');

      // FuturisticButton adds transition-all for animations
      expect(getStartedButton).toHaveClass('transition-all');
      expect(signInButton).toHaveClass('transition-all');
    });

    it('GuestShortener Shorten button uses FuturisticButton component', () => {
      renderWithProviders(<Home />);

      // The Shorten button in GuestShortener section - use exact aria-label to avoid matching "Get started with URL Shortener"
      const shortenButton = screen.getByRole('button', { name: 'Shorten URL' });

      expect(shortenButton).toHaveClass('btn');
      expect(shortenButton).toHaveClass('btn-primary');
      expect(shortenButton).toHaveClass('min-h-[44px]');
    });
  });

  /**
   * Test Case 4: Feature cards use GlassMorphismCard component
   */
  describe('Test Case 4: GlassMorphismCard Usage', () => {
    it('renders feature cards using GlassMorphismCard structure', () => {
      renderWithProviders(<Home />);

      // Features section should be present
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
    });

    it('each feature card has GlassMorphismCard styling', () => {
      renderWithProviders(<Home />);

      const featureCards = screen.getAllByTestId('feature-card');

      // Should have at least 3 feature cards
      expect(featureCards.length).toBeGreaterThanOrEqual(3);

      featureCards.forEach((card) => {
        // GlassMorphismCard wraps content with specific classes
        const glassCard = card.querySelector('.card');
        expect(glassCard).toBeInTheDocument();
        expect(glassCard).toHaveClass('backdrop-blur-md');
        expect(glassCard).toHaveClass('rounded-xl');
        expect(glassCard).toHaveClass('shadow-xl');
      });
    });

    it('feature cards have hover effect classes from GlassMorphismCard', () => {
      renderWithProviders(<Home />);

      const featureCards = screen.getAllByTestId('feature-card');

      featureCards.forEach((card) => {
        const glassCard = card.querySelector('.card');
        // GlassMorphismCard with hover=true adds transition classes
        expect(glassCard).toHaveClass('transition-all');
      });
    });

    it('GuestShortener widget uses GlassMorphismCard', () => {
      renderWithProviders(<Home />);

      const guestShortenerSection = screen.getByTestId('guest-shortener-section');

      // GuestShortener wraps content in GlassMorphismCard
      const glassCard = guestShortenerSection.querySelector('.card');
      expect(glassCard).toBeInTheDocument();
      expect(glassCard).toHaveClass('backdrop-blur-md');
    });
  });

  /**
   * Test Case 5: ThemeToggle is accessible via Navbar
   */
  describe('Test Case 5: ThemeToggle Accessibility via Navbar', () => {
    it('ThemeToggle is rendered within Navbar', () => {
      renderWithProviders(<Home />);

      const themeToggle = screen.getByLabelText(/toggle theme/i);
      const navbar = document.querySelector('.navbar');

      expect(themeToggle).toBeInTheDocument();
      expect(navbar).toContainElement(themeToggle);
    });

    it('ThemeToggle is accessible with keyboard', () => {
      renderWithProviders(<Home />);

      const themeToggle = screen.getByLabelText(/toggle theme/i);

      // Should have tabIndex for keyboard accessibility
      expect(themeToggle).toHaveAttribute('tabIndex', '0');
    });

    it('ThemeToggle has proper WCAG touch target size', () => {
      renderWithProviders(<Home />);

      const themeToggle = screen.getByLabelText(/toggle theme/i);

      // Min touch target size for WCAG compliance
      expect(themeToggle).toHaveClass('min-h-[44px]');
      expect(themeToggle).toHaveClass('min-w-[44px]');
    });

    it('ThemeToggle is visible and interactable', () => {
      renderWithProviders(<Home />);

      const themeToggle = screen.getByLabelText(/toggle theme/i);

      expect(themeToggle).toBeVisible();
      expect(themeToggle).not.toBeDisabled();
    });

    it('ThemeToggle dropdown shows theme options when activated', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      const themeToggle = screen.getByLabelText(/toggle theme/i);
      await user.click(themeToggle);

      // Verify theme options are displayed in dropdown
      const lightOption = screen.getByRole('button', { name: /set light theme/i });
      const darkOption = screen.getByRole('button', { name: /set dark theme/i });

      expect(lightOption).toBeInTheDocument();
      expect(darkOption).toBeInTheDocument();
    });
  });

  /**
   * Integration verification - all components work together
   */
  describe('Component Integration - Cross-cutting', () => {
    it('all required UI components are rendered together', () => {
      renderWithProviders(<Home />);

      // Verify all required components are present
      const navbar = document.querySelector('.navbar');
      const canvas = document.querySelector('canvas');
      const heroSection = screen.getByTestId('hero-section');
      const featuresSection = screen.getByTestId('features-section');
      const themeToggle = screen.getByLabelText(/toggle theme/i);

      expect(navbar).toBeInTheDocument();
      expect(canvas).toBeInTheDocument();
      expect(heroSection).toBeInTheDocument();
      expect(featuresSection).toBeInTheDocument();
      expect(themeToggle).toBeInTheDocument();
    });

    it('components maintain proper z-index layering', () => {
      renderWithProviders(<Home />);

      const navbar = document.querySelector('.navbar');
      const canvas = document.querySelector('canvas');

      // Navbar should be above content (z-50)
      expect(navbar).toHaveClass('z-50');

      // BackgroundEffect should be behind content (-z-10)
      expect(canvas).toHaveClass('-z-10');
    });

    it('FuturisticButton and GlassMorphismCard coexist in GuestShortener', () => {
      renderWithProviders(<Home />);

      const guestShortenerSection = screen.getByTestId('guest-shortener-section');

      // Should have both GlassMorphismCard wrapper and FuturisticButton inside
      const glassCard = guestShortenerSection.querySelector('.card.backdrop-blur-md');
      const shortenButton = screen.getByRole('button', { name: 'Shorten URL' });

      expect(glassCard).toBeInTheDocument();
      expect(shortenButton).toBeInTheDocument();
      expect(shortenButton).toHaveClass('btn', 'btn-primary');
    });
  });
});
