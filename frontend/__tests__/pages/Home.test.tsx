/**
 * Home Page Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 * Extended by: Scenario 2 - Navigation to Registration
 *
 * Integration tests for the Home page component.
 */

import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom';
import { describe, it, expect, vi } from 'vitest';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { Home } from '../../src/pages/Home';

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
