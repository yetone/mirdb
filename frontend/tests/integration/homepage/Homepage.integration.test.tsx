/**
 * Integration Tests for Authenticated User Experience.
 * Owner: Scenario 6 - Authenticated User Experience
 *
 * Tests that the homepage correctly detects authentication state and adjusts
 * CTA to direct logged-in users to the dashboard, showing appropriate navbar state.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React, { ReactNode } from 'react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import '@testing-library/jest-dom';

import App from '../../../src/App';
import Home from '../../../src/pages/Home';
import { HeroSection } from '../../../src/components/homepage/HeroSection';
import Navbar from '../../../src/components/Navbar';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

// Mock IntersectionObserver for Framer Motion
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | null = null;
  readonly rootMargin: string = '';
  readonly thresholds: ReadonlyArray<number> = [];

  constructor(
    private callback: IntersectionObserverCallback,
    _options?: IntersectionObserverInit
  ) {}

  observe(_target: Element): void {
    this.callback(
      [
        {
          isIntersecting: true,
          boundingClientRect: {} as DOMRectReadOnly,
          intersectionRatio: 1,
          intersectionRect: {} as DOMRectReadOnly,
          rootBounds: null,
          target: {} as Element,
          time: Date.now(),
        },
      ],
      this
    );
  }

  unobserve(_target: Element): void {}
  disconnect(): void {}
  takeRecords(): IntersectionObserverEntry[] {
    return [];
  }
}

global.IntersectionObserver = MockIntersectionObserver;

// Create a mock AuthContext for testing authenticated state
const mockAuthContextModule = vi.hoisted(() => {
  let mockIsAuthenticated = false;
  let mockUsername: string | null = null;
  const mockLogin = vi.fn((username: string) => {
    mockIsAuthenticated = true;
    mockUsername = username;
  });
  const mockLogout = vi.fn(() => {
    mockIsAuthenticated = false;
    mockUsername = null;
  });

  return {
    getIsAuthenticated: () => mockIsAuthenticated,
    getUsername: () => mockUsername,
    setMockState: (auth: boolean, user: string | null) => {
      mockIsAuthenticated = auth;
      mockUsername = user;
    },
    mockLogin,
    mockLogout,
    reset: () => {
      mockIsAuthenticated = false;
      mockUsername = null;
      mockLogin.mockClear();
      mockLogout.mockClear();
    },
  };
});

// Mock the AuthContext module
vi.mock('../../../src/contexts/AuthContext', () => ({
  AuthProvider: ({ children }: { children: ReactNode }) => <>{children}</>,
  useAuth: () => ({
    isAuthenticated: mockAuthContextModule.getIsAuthenticated(),
    username: mockAuthContextModule.getUsername(),
    login: mockAuthContextModule.mockLogin,
    logout: mockAuthContextModule.mockLogout,
  }),
}));

// Mock navigate for routing tests
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Test wrapper that provides all necessary providers
function TestWrapper({ children }: { children: ReactNode }) {
  return (
    <BrowserRouter>
      <ThemeProvider>
        {children}
      </ThemeProvider>
    </BrowserRouter>
  );
}

describe('Authenticated User Experience', () => {
  beforeEach(() => {
    mockAuthContextModule.reset();
    mockNavigate.mockClear();
  });

  describe('Test Case 1: Homepage detects authentication state from AuthContext', () => {
    it('renders homepage with authenticated state when user is logged in', () => {
      mockAuthContextModule.setMockState(true, 'TestUser');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      // The homepage should render successfully
      const main = document.querySelector('main');
      expect(main).toBeInTheDocument();
    });

    it('homepage uses AuthContext to determine authentication state', () => {
      // First render unauthenticated
      mockAuthContextModule.setMockState(false, null);

      const { unmount } = render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const unauthCta = screen.getByTestId('hero-primary-cta');
      expect(unauthCta.textContent).toMatch(/get started/i);

      unmount();

      // Now render authenticated
      mockAuthContextModule.setMockState(true, 'AuthUser');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const authCta = screen.getByTestId('hero-primary-cta');
      expect(authCta.textContent).toMatch(/go to dashboard/i);
    });

    it('passes isAuthenticated prop to HeroSection from AuthContext', () => {
      mockAuthContextModule.setMockState(true, 'ContextUser');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      // When authenticated, the hero CTA should change
      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta).toHaveAttribute('aria-label', 'Go to Dashboard');
    });
  });

  describe('Test Case 2: Primary CTA displays Go to Dashboard instead of Get Started', () => {
    it('shows "Go to Dashboard" button when user is authenticated', () => {
      mockAuthContextModule.setMockState(true, 'DashboardUser');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta).toBeInTheDocument();
      expect(primaryCta.textContent).toBe('Go to Dashboard');
    });

    it('shows "Get Started Free" button when user is not authenticated', () => {
      mockAuthContextModule.setMockState(false, null);

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta).toBeInTheDocument();
      expect(primaryCta.textContent).toBe('Get Started Free');
    });

    it('CTA text dynamically changes based on auth state', () => {
      mockAuthContextModule.setMockState(true, 'DynamicUser');

      render(
        <TestWrapper>
          <HeroSection isAuthenticated={true} username="DynamicUser" />
        </TestWrapper>
      );

      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta.textContent).not.toMatch(/get started/i);
      expect(primaryCta.textContent).toMatch(/go to dashboard/i);
    });
  });

  describe('Test Case 3: Click Go to Dashboard button navigates to /dashboard route', () => {
    it('navigates to /dashboard when authenticated user clicks primary CTA', async () => {
      const user = userEvent.setup();
      mockAuthContextModule.setMockState(true, 'NavUser');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const dashboardButton = screen.getByTestId('hero-primary-cta');
      expect(dashboardButton.textContent).toBe('Go to Dashboard');

      await user.click(dashboardButton);

      expect(mockNavigate).toHaveBeenCalledWith('/dashboard');
    });

    it('does not navigate to /dashboard when unauthenticated user clicks CTA', async () => {
      const user = userEvent.setup();
      mockAuthContextModule.setMockState(false, null);

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const registerButton = screen.getByTestId('hero-primary-cta');
      await user.click(registerButton);

      // Should navigate to register, not dashboard
      expect(mockNavigate).toHaveBeenCalledWith('/register');
      expect(mockNavigate).not.toHaveBeenCalledWith('/dashboard');
    });

    it('Go to Dashboard button is clickable and functional', async () => {
      const user = userEvent.setup();
      mockAuthContextModule.setMockState(true, 'ClickUser');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const dashboardButton = screen.getByTestId('hero-primary-cta');

      // Verify button is enabled and can be clicked
      expect(dashboardButton).not.toBeDisabled();
      await user.click(dashboardButton);

      expect(mockNavigate).toHaveBeenCalledTimes(1);
      expect(mockNavigate).toHaveBeenCalledWith('/dashboard');
    });
  });

  describe('Test Case 4: Navbar shows username and logout option instead of Login/Register', () => {
    it('displays logout button when user is authenticated', () => {
      mockAuthContextModule.setMockState(true, 'NavbarUser');

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      );

      const logoutButton = screen.getByRole('button', { name: /logout/i });
      expect(logoutButton).toBeInTheDocument();
    });

    it('displays Dashboard link when user is authenticated', () => {
      mockAuthContextModule.setMockState(true, 'NavbarUser');

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      );

      const dashboardLink = screen.getByRole('link', { name: /dashboard/i });
      expect(dashboardLink).toBeInTheDocument();
      expect(dashboardLink).toHaveAttribute('href', '/dashboard');
    });

    it('hides Login link when user is authenticated', () => {
      mockAuthContextModule.setMockState(true, 'NavbarUser');

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      );

      const loginLink = screen.queryByRole('link', { name: /^login$/i });
      expect(loginLink).not.toBeInTheDocument();
    });

    it('hides Register link when user is authenticated', () => {
      mockAuthContextModule.setMockState(true, 'NavbarUser');

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      );

      const registerLink = screen.queryByRole('link', { name: /register/i });
      expect(registerLink).not.toBeInTheDocument();
    });

    it('shows Login and Register links when user is not authenticated', () => {
      mockAuthContextModule.setMockState(false, null);

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      );

      const loginLink = screen.getByRole('link', { name: /login/i });
      const registerLink = screen.getByRole('link', { name: /register/i });

      expect(loginLink).toBeInTheDocument();
      expect(registerLink).toBeInTheDocument();
    });

    it('logout button calls logout function when clicked', async () => {
      const user = userEvent.setup();
      mockAuthContextModule.setMockState(true, 'LogoutUser');

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      );

      const logoutButton = screen.getByRole('button', { name: /logout/i });
      await user.click(logoutButton);

      expect(mockAuthContextModule.mockLogout).toHaveBeenCalled();
    });
  });

  describe('Test Case 5: Hero section displays personalized greeting with username', () => {
    it('displays personalized greeting when authenticated with username', () => {
      mockAuthContextModule.setMockState(true, 'JohnDoe');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const greeting = screen.getByTestId('hero-greeting');
      expect(greeting).toBeInTheDocument();
      expect(greeting.textContent).toContain('JohnDoe');
    });

    it('greeting includes welcome message', () => {
      mockAuthContextModule.setMockState(true, 'WelcomeUser');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const greeting = screen.getByTestId('hero-greeting');
      expect(greeting.textContent).toMatch(/welcome back/i);
      expect(greeting.textContent).toContain('WelcomeUser');
    });

    it('does not display greeting when user is not authenticated', () => {
      mockAuthContextModule.setMockState(false, null);

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const greeting = screen.queryByTestId('hero-greeting');
      expect(greeting).not.toBeInTheDocument();
    });

    it('does not display greeting when authenticated but username is null', () => {
      mockAuthContextModule.setMockState(true, null);

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      const greeting = screen.queryByTestId('hero-greeting');
      expect(greeting).not.toBeInTheDocument();
    });

    it('displays different greeting for different usernames', () => {
      mockAuthContextModule.setMockState(true, 'Alice');

      const { unmount } = render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      let greeting = screen.getByTestId('hero-greeting');
      expect(greeting.textContent).toContain('Alice');

      unmount();

      mockAuthContextModule.setMockState(true, 'Bob');

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      );

      greeting = screen.getByTestId('hero-greeting');
      expect(greeting.textContent).toContain('Bob');
    });
  });

  describe('Full Integration: Authenticated User Flow', () => {
    it('complete authenticated user experience works correctly', () => {
      mockAuthContextModule.setMockState(true, 'IntegrationUser');

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      );

      // 1. Verify navbar shows authenticated state
      const logoutButton = screen.getByRole('button', { name: /logout/i });
      expect(logoutButton).toBeInTheDocument();

      // 2. Verify Login/Register are hidden
      expect(screen.queryByRole('link', { name: /^login$/i })).not.toBeInTheDocument();
      expect(screen.queryByRole('link', { name: /register/i })).not.toBeInTheDocument();

      // 3. Verify hero shows personalized greeting
      const greeting = screen.getByTestId('hero-greeting');
      expect(greeting.textContent).toContain('IntegrationUser');

      // 4. Verify CTA shows Dashboard option
      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta.textContent).toBe('Go to Dashboard');
    });

    it('complete unauthenticated user experience works correctly', () => {
      mockAuthContextModule.setMockState(false, null);

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      );

      // 1. Verify navbar shows unauthenticated state
      const loginLink = screen.getByRole('link', { name: /login/i });
      const registerLink = screen.getByRole('link', { name: /register/i });
      expect(loginLink).toBeInTheDocument();
      expect(registerLink).toBeInTheDocument();

      // 2. Verify logout is hidden
      expect(screen.queryByRole('button', { name: /logout/i })).not.toBeInTheDocument();

      // 3. Verify no personalized greeting
      expect(screen.queryByTestId('hero-greeting')).not.toBeInTheDocument();

      // 4. Verify CTA shows Get Started
      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta.textContent).toBe('Get Started Free');
    });
  });
});
