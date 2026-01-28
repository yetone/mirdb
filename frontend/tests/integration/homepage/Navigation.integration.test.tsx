/**
 * Navigation Integration Tests
 * Owner: Scenario 2 - Navigation and Routing
 *
 * Tests for verifying the navigation bar provides access to Login and Register pages
 * with proper routing (REQ-3, US-2, US-3)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from 'react-query';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { Navbar } from '../../../src/components/Navbar';
import Home from '../../../src/pages/Home';
import Login from '../../../src/pages/Login';
import Register from '../../../src/pages/Register';

// Mock the API module
vi.mock('../../../src/api', () => ({
  getCurrentUser: vi.fn().mockRejectedValue(new Error('Not authenticated')),
  login: vi.fn(),
  logout: vi.fn(),
  register: vi.fn(),
  default: {
    interceptors: {
      request: { use: vi.fn() },
      response: { use: vi.fn() },
    },
  },
}));

// Test wrapper with all providers
interface TestWrapperProps {
  children: React.ReactNode;
  initialEntries?: string[];
}

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

const TestWrapper = ({ children, initialEntries = ['/'] }: TestWrapperProps) => {
  const testQueryClient = createTestQueryClient();

  return (
    <QueryClientProvider client={testQueryClient}>
      <ThemeProvider>
        <AuthProvider>
          <MemoryRouter initialEntries={initialEntries}>
            {children}
          </MemoryRouter>
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
};

// App with routes for testing navigation
const AppWithRoutes = () => (
  <Routes>
    <Route path="/" element={<Home />} />
    <Route path="/login" element={<Login />} />
    <Route path="/register" element={<Register />} />
  </Routes>
);

describe('Navbar Navigation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Test Case 1: Navbar renders with all required navigation elements
  it('renders Navbar with all required navigation elements', async () => {
    render(
      <TestWrapper>
        <Navbar />
      </TestWrapper>
    );

    // Wait for auth state to settle
    await waitFor(() => {
      // Logo/brand element should be present
      expect(screen.getByTestId('logo-link')).toBeInTheDocument();

      // Login link should be present
      expect(screen.getByTestId('login-link')).toBeInTheDocument();

      // Register button should be present
      expect(screen.getByTestId('register-button')).toBeInTheDocument();

      // Theme toggle should be present (desktop and mobile versions)
      const themeToggles = screen.getAllByTestId('theme-toggle');
      expect(themeToggles.length).toBeGreaterThan(0);
    });
  });

  // Test Case 2: Login link exists with correct text
  it('has Login link with text "Login" or "Sign In"', async () => {
    render(
      <TestWrapper>
        <Navbar />
      </TestWrapper>
    );

    await waitFor(() => {
      const loginLink = screen.getByTestId('login-link');
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveTextContent(/login|sign in/i);
    });
  });

  // Test Case 3: Click Login link navigates to /login
  it('navigates to /login when Login link is clicked', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper initialEntries={['/']}>
        <AppWithRoutes />
      </TestWrapper>
    );

    // Wait for the page to render
    await waitFor(() => {
      expect(screen.getByTestId('login-link')).toBeInTheDocument();
    });

    // Click the login link
    await user.click(screen.getByTestId('login-link'));

    // Verify navigation to login page
    await waitFor(() => {
      expect(screen.getByText(/sign in to your account/i)).toBeInTheDocument();
    });
  });

  // Test Case 4: Register/Sign Up button exists
  it('has Register/Sign Up button with text "Register" or "Sign Up"', async () => {
    render(
      <TestWrapper>
        <Navbar />
      </TestWrapper>
    );

    await waitFor(() => {
      const registerButton = screen.getByTestId('register-button');
      expect(registerButton).toBeInTheDocument();
      expect(registerButton).toHaveTextContent(/register|sign up/i);
    });
  });

  // Test Case 5: Click Register button navigates to /register
  it('navigates to /register when Register button is clicked', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper initialEntries={['/']}>
        <AppWithRoutes />
      </TestWrapper>
    );

    // Wait for the page to render
    await waitFor(() => {
      expect(screen.getByTestId('register-button')).toBeInTheDocument();
    });

    // Click the register button
    await user.click(screen.getByTestId('register-button'));

    // Verify navigation to register page
    await waitFor(() => {
      expect(screen.getByText(/create your free account/i)).toBeInTheDocument();
    });
  });

  // Test Case 6: Logo/brand element exists and is clickable
  it('has logo/brand element that exists and is clickable', async () => {
    render(
      <TestWrapper>
        <Navbar />
      </TestWrapper>
    );

    await waitFor(() => {
      const logoLink = screen.getByTestId('logo-link');
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toHaveAttribute('href', '/');
    });
  });

  // Test Case 7: Click logo navigates to homepage
  it('navigates to / (homepage) when logo is clicked', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper initialEntries={['/login']}>
        <AppWithRoutes />
      </TestWrapper>
    );

    // Verify we're on login page first
    await waitFor(() => {
      expect(screen.getByText(/sign in to your account/i)).toBeInTheDocument();
    });

    // Click the logo link (need to find it in Login page - it's in the form header)
    const logoLink = screen.getByRole('link', { name: /url shortener/i });
    await user.click(logoLink);

    // Verify navigation to homepage
    await waitFor(() => {
      expect(screen.getByText(/shorten urls\. track every click/i)).toBeInTheDocument();
    });
  });

  // Test Case 8: ThemeToggle is present in navbar
  it('has ThemeToggle component rendered in the navbar', async () => {
    render(
      <TestWrapper>
        <Navbar />
      </TestWrapper>
    );

    await waitFor(() => {
      // There are two ThemeToggle components (desktop and mobile views)
      // Using getAllByTestId to find all instances
      const themeToggles = screen.getAllByTestId('theme-toggle');
      expect(themeToggles.length).toBeGreaterThan(0);
      expect(themeToggles[0]).toBeInTheDocument();
    });
  });
});

describe('Navigation from Homepage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  it('homepage has Login link in navbar that navigates correctly', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper initialEntries={['/']}>
        <AppWithRoutes />
      </TestWrapper>
    );

    // Wait for the page and navbar to render
    await waitFor(() => {
      expect(screen.getByTestId('login-link')).toBeInTheDocument();
    }, { timeout: 5000 });

    // Click login in navbar
    await user.click(screen.getByTestId('login-link'));

    // Verify we're on login page
    await waitFor(() => {
      expect(screen.getByText(/sign in to your account/i)).toBeInTheDocument();
    }, { timeout: 5000 });
  });

  it('homepage has Register button in navbar that navigates correctly', async () => {
    const user = userEvent.setup();

    render(
      <TestWrapper initialEntries={['/']}>
        <AppWithRoutes />
      </TestWrapper>
    );

    // Wait for the page and navbar to render
    await waitFor(() => {
      expect(screen.getByTestId('register-button')).toBeInTheDocument();
    }, { timeout: 5000 });

    // Click register in navbar
    await user.click(screen.getByTestId('register-button'));

    // Verify we're on register page
    await waitFor(() => {
      expect(screen.getByText(/create your free account/i)).toBeInTheDocument();
    }, { timeout: 5000 });
  });
});
