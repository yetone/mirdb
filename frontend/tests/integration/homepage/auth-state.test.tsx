/**
 * Auth State Integration Tests
 * Owner: Scenario 4 - Authenticated User CTA Behavior, Scenario 25 - AuthContext Integration
 *
 * Tests that verify the homepage correctly responds to authentication state:
 * - Authenticated users see "Go to Dashboard" CTA
 * - Unauthenticated users see "Get Started" CTA
 * - Dashboard navigation works for authenticated users
 */

import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { AuthContext } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { Home } from '../../../src/pages/Home';
import { Dashboard } from '../../../src/pages/Dashboard';
import { HeroSection } from '../../../src/components/homepage/HeroSection';

/**
 * Mock auth context for testing different authentication states
 */
const createMockAuthContext = (isAuthenticated: boolean) => ({
  user: isAuthenticated
    ? { id: 1, username: 'testuser', email: 'test@example.com', is_admin: false }
    : null,
  isAuthenticated,
  isLoading: false,
  login: vi.fn(),
  logout: vi.fn(),
  register: vi.fn(),
});

/**
 * Test wrapper that provides full app context with controlled auth state
 */
function TestAppWithAuth({
  initialRoute = '/',
  isAuthenticated = false,
}: {
  initialRoute?: string;
  isAuthenticated?: boolean;
}) {
  const authValue = createMockAuthContext(isAuthenticated);

  return (
    <ThemeProvider>
      <AuthContext.Provider value={authValue}>
        <MemoryRouter initialEntries={[initialRoute]}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/dashboard" element={<Dashboard />} />
          </Routes>
        </MemoryRouter>
      </AuthContext.Provider>
    </ThemeProvider>
  );
}

/**
 * Minimal wrapper for testing HeroSection in isolation
 */
function HeroSectionTestWrapper({
  isAuthenticated = false,
}: {
  isAuthenticated?: boolean;
}) {
  const authValue = createMockAuthContext(isAuthenticated);

  return (
    <ThemeProvider>
      <AuthContext.Provider value={authValue}>
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      </AuthContext.Provider>
    </ThemeProvider>
  );
}

describe('Scenario 4: Authenticated User CTA Behavior', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Test Case 1: Render HomePage component with authenticated user context', () => {
    it('should display "Go to Dashboard" text on primary CTA when user is authenticated', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      // Find the hero CTA button
      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toBeInTheDocument();

      // Verify the CTA shows "Go to Dashboard" for authenticated users
      expect(heroCTA).toHaveTextContent(/go to dashboard/i);
    });

    it('should link to /dashboard when user is authenticated', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toHaveAttribute('href', '/dashboard');
    });

    it('should NOT display "Get Started" or login link when user is authenticated', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      // The hero CTA should not contain "Get Started"
      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).not.toHaveTextContent(/get started/i);

      // The "Already have an account? Login" link should not be visible
      expect(screen.queryByTestId('hero-login-link')).not.toBeInTheDocument();
    });
  });

  describe('Test Case 2: Click "Go to Dashboard" CTA as authenticated user', () => {
    it('should navigate to /dashboard route when clicking Dashboard CTA', async () => {
      const user = userEvent.setup();

      render(<TestAppWithAuth isAuthenticated={true} initialRoute="/" />);

      // Verify we start on the homepage
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/shorten urls/i);

      // Find and click the Dashboard CTA
      const dashboardCTA = screen.getByTestId('hero-cta');
      expect(dashboardCTA).toHaveTextContent(/go to dashboard/i);

      await user.click(dashboardCTA);

      // Verify navigation to dashboard
      await waitFor(() => {
        expect(screen.getByText(/welcome to your dashboard/i)).toBeInTheDocument();
      });

      // Verify the dashboard heading is displayed
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/dashboard/i);
    });
  });

  describe('Test Case 3: CTA text changes based on authentication state', () => {
    it('should show "Get Started" for unauthenticated users', () => {
      render(<TestAppWithAuth isAuthenticated={false} />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toHaveTextContent(/get started/i);
      expect(heroCTA).toHaveAttribute('href', '/register');
    });

    it('should show "Go to Dashboard" for authenticated users', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toHaveTextContent(/go to dashboard/i);
      expect(heroCTA).toHaveAttribute('href', '/dashboard');
    });

    it('should show login link only for unauthenticated users', () => {
      // Unauthenticated user sees login link
      const { unmount } = render(<TestAppWithAuth isAuthenticated={false} />);
      expect(screen.getByTestId('hero-login-link')).toBeInTheDocument();
      expect(screen.getByText(/already have an account/i)).toBeInTheDocument();
      unmount();

      // Authenticated user does not see login link
      render(<TestAppWithAuth isAuthenticated={true} />);
      expect(screen.queryByTestId('hero-login-link')).not.toBeInTheDocument();
      expect(screen.queryByText(/already have an account/i)).not.toBeInTheDocument();
    });
  });

  describe('HeroSection component isolation tests', () => {
    it('should use isAuthenticated prop when provided directly', () => {
      render(
        <ThemeProvider>
          <AuthContext.Provider value={createMockAuthContext(false)}>
            <MemoryRouter>
              <HeroSection isAuthenticated={true} />
            </MemoryRouter>
          </AuthContext.Provider>
        </ThemeProvider>
      );

      // Even though context says unauthenticated, prop overrides
      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toHaveTextContent(/go to dashboard/i);
    });

    it('should fall back to AuthContext when prop is not provided', () => {
      render(<HeroSectionTestWrapper isAuthenticated={true} />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toHaveTextContent(/go to dashboard/i);
    });
  });

  describe('Additional auth state edge cases', () => {
    it('should have accessible CTA button for authenticated users', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      const heroCTA = screen.getByTestId('hero-cta');

      // Verify the button is an anchor with proper attributes
      expect(heroCTA.tagName.toLowerCase()).toBe('a');
      expect(heroCTA).toHaveAttribute('href', '/dashboard');
    });

    it('should maintain consistent hero section structure regardless of auth state', () => {
      // Check authenticated state
      const { unmount } = render(<TestAppWithAuth isAuthenticated={true} />);

      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/shorten urls/i);
      expect(
        screen.getByText(/transform long, unwieldy urls/i)
      ).toBeInTheDocument();
      unmount();

      // Check unauthenticated state
      render(<TestAppWithAuth isAuthenticated={false} />);

      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/shorten urls/i);
      expect(
        screen.getByText(/transform long, unwieldy urls/i)
      ).toBeInTheDocument();
    });

    it('should render Dashboard CTA with correct styling classes', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      const heroCTA = screen.getByTestId('hero-cta');

      // FuturisticButton applies these classes for primary variant
      expect(heroCTA).toHaveClass('btn');
      expect(heroCTA).toHaveClass('btn-primary');
      expect(heroCTA).toHaveClass('btn-lg');
    });
  });
});

/**
 * Scenario 25: AuthContext Integration
 *
 * Tests that verify homepage correctly reads and responds to authentication state.
 * These tests focus on the integration between the homepage components and AuthContext,
 * ensuring that:
 * - Unauthenticated state shows appropriate CTAs and login options
 * - Authenticated state shows dashboard access and logged-in navigation
 * - Auth state transitions (like logout) update the UI appropriately
 */
describe('Scenario 25: AuthContext Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Test Case 1: Render homepage with unauthenticated AuthContext', () => {
    it('should show "Get Started" CTA when user is unauthenticated', () => {
      render(<TestAppWithAuth isAuthenticated={false} />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toBeInTheDocument();
      expect(heroCTA).toHaveTextContent(/get started/i);
    });

    it('should display login options when user is unauthenticated', () => {
      render(<TestAppWithAuth isAuthenticated={false} />);

      // Hero section login link
      const heroLoginLink = screen.getByTestId('hero-login-link');
      expect(heroLoginLink).toBeInTheDocument();
      expect(heroLoginLink).toHaveAttribute('href', '/login');

      // Navbar login button
      const navLoginButton = screen.getByTestId('nav-login');
      expect(navLoginButton).toBeInTheDocument();
      expect(navLoginButton).toHaveAttribute('href', '/login');

      // Navbar register button
      const navRegisterButton = screen.getByTestId('nav-register');
      expect(navRegisterButton).toBeInTheDocument();
      expect(navRegisterButton).toHaveAttribute('href', '/register');
    });

    it('should link Get Started CTA to registration page', () => {
      render(<TestAppWithAuth isAuthenticated={false} />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toHaveAttribute('href', '/register');
    });

    it('should display "Already have an account?" text for unauthenticated users', () => {
      render(<TestAppWithAuth isAuthenticated={false} />);

      expect(screen.getByText(/already have an account/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Render homepage with authenticated AuthContext', () => {
    it('should show "Go to Dashboard" CTA when user is authenticated', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toBeInTheDocument();
      expect(heroCTA).toHaveTextContent(/go to dashboard/i);
    });

    it('should link dashboard CTA to /dashboard when authenticated', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      const heroCTA = screen.getByTestId('hero-cta');
      expect(heroCTA).toHaveAttribute('href', '/dashboard');
    });

    it('should show logged-in navigation state when authenticated', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      // Login/Register links should NOT be visible in navbar
      expect(screen.queryByTestId('nav-login')).not.toBeInTheDocument();
      expect(screen.queryByTestId('nav-register')).not.toBeInTheDocument();

      // Dashboard links should be visible (hero CTA + navbar link)
      const dashboardLinks = screen.getAllByRole('link', { name: /dashboard/i });
      expect(dashboardLinks.length).toBeGreaterThanOrEqual(1);

      // Logout button should be visible
      expect(screen.getByRole('button', { name: /logout/i })).toBeInTheDocument();
    });

    it('should NOT show login link in hero when authenticated', () => {
      render(<TestAppWithAuth isAuthenticated={true} />);

      expect(screen.queryByTestId('hero-login-link')).not.toBeInTheDocument();
      expect(screen.queryByText(/already have an account/i)).not.toBeInTheDocument();
    });
  });

  describe('Test Case 3: Simulate logout while on homepage', () => {
    /**
     * Test wrapper that allows controlled auth state transitions
     */
    function AuthStateTransitionTestWrapper() {
      const [isAuthenticated, setIsAuthenticated] = React.useState(true);
      const [user, setUser] = React.useState({
        id: 1,
        username: 'testuser',
        email: 'test@example.com',
        is_admin: false,
      });

      const logout = () => {
        setUser(null as unknown as typeof user);
        setIsAuthenticated(false);
      };

      const authValue = {
        user: isAuthenticated ? user : null,
        isAuthenticated,
        isLoading: false,
        login: vi.fn(),
        logout,
        register: vi.fn(),
      };

      return (
        <ThemeProvider>
          <AuthContext.Provider value={authValue}>
            <MemoryRouter initialEntries={['/']}>
              <Routes>
                <Route path="/" element={<Home />} />
                <Route path="/dashboard" element={<Dashboard />} />
              </Routes>
            </MemoryRouter>
          </AuthContext.Provider>
        </ThemeProvider>
      );
    }

    it('should update CTAs to unauthenticated state after logout', async () => {
      const user = userEvent.setup();

      render(<AuthStateTransitionTestWrapper />);

      // Initially authenticated - should see Dashboard CTA
      expect(screen.getByTestId('hero-cta')).toHaveTextContent(/go to dashboard/i);

      // Should see Logout button
      const logoutButton = screen.getByRole('button', { name: /logout/i });
      expect(logoutButton).toBeInTheDocument();

      // Click logout
      await user.click(logoutButton);

      // After logout, should see Get Started CTA
      await waitFor(() => {
        expect(screen.getByTestId('hero-cta')).toHaveTextContent(/get started/i);
      });
    });

    it('should show login options after logout', async () => {
      const user = userEvent.setup();

      render(<AuthStateTransitionTestWrapper />);

      // Initially, login options should not be visible (authenticated)
      expect(screen.queryByTestId('hero-login-link')).not.toBeInTheDocument();

      // Click logout
      const logoutButton = screen.getByRole('button', { name: /logout/i });
      await user.click(logoutButton);

      // After logout, login options should be visible
      await waitFor(() => {
        expect(screen.getByTestId('hero-login-link')).toBeInTheDocument();
      });
      expect(screen.getByText(/already have an account/i)).toBeInTheDocument();
    });

    it('should update navbar to show login/register after logout', async () => {
      const user = userEvent.setup();

      render(<AuthStateTransitionTestWrapper />);

      // Initially authenticated - should NOT see nav login/register
      expect(screen.queryByTestId('nav-login')).not.toBeInTheDocument();
      expect(screen.queryByTestId('nav-register')).not.toBeInTheDocument();

      // Click logout
      const logoutButton = screen.getByRole('button', { name: /logout/i });
      await user.click(logoutButton);

      // After logout, should see nav login/register
      await waitFor(() => {
        expect(screen.getByTestId('nav-login')).toBeInTheDocument();
      });
      expect(screen.getByTestId('nav-register')).toBeInTheDocument();
    });

    it('should update CTA href from /dashboard to /register after logout', async () => {
      const user = userEvent.setup();

      render(<AuthStateTransitionTestWrapper />);

      // Initially authenticated - CTA should link to dashboard
      expect(screen.getByTestId('hero-cta')).toHaveAttribute('href', '/dashboard');

      // Click logout
      const logoutButton = screen.getByRole('button', { name: /logout/i });
      await user.click(logoutButton);

      // After logout, CTA should link to register
      await waitFor(() => {
        expect(screen.getByTestId('hero-cta')).toHaveAttribute('href', '/register');
      });
    });
  });
});
