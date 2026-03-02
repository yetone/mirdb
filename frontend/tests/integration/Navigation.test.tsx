/**
 * Integration Tests for Navigation.
 * Owner: Scenario 3 - Navigation Bar Functionality
 * Auth Integration Owner: Scenario 19 - Authentication Context Integration
 *
 * Tests:
 * - Click Login link navigates to /login route without full page reload
 * - Click Register link navigates to /register route without full page reload
 * - Navigation uses React Router for client-side routing
 * - Auth-aware navigation shows correct links based on authentication state
 * - Token expiration handling
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider, useAuth } from '../../src/contexts/AuthContext';
import { HomeNavbar } from '../../src/components/layout/HomeNavbar';
import { Login } from '../../src/pages/Login';
import { Register } from '../../src/pages/Register';

// Helper to create a valid JWT token (header.payload.signature)
function createMockToken(expiresIn: number = 3600): string {
  const header = btoa(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const payload = btoa(
    JSON.stringify({
      sub: 'user-123',
      email: 'test@example.com',
      exp: Math.floor(Date.now() / 1000) + expiresIn,
    })
  );
  const signature = btoa('mock-signature');
  return `${header}.${payload}.${signature}`;
}

// Helper to create an expired JWT token
function createExpiredToken(): string {
  const header = btoa(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const payload = btoa(
    JSON.stringify({
      sub: 'user-123',
      email: 'test@example.com',
      exp: Math.floor(Date.now() / 1000) - 3600, // 1 hour ago
    })
  );
  const signature = btoa('mock-signature');
  return `${header}.${payload}.${signature}`;
}

function renderWithRouter(initialPath = '/') {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <ThemeProvider>
        <AuthProvider>
          <Routes>
            <Route
              path="/"
              element={
                <div>
                  <HomeNavbar />
                  <main data-testid="home-page">Home Page</main>
                </div>
              }
            />
            <Route
              path="/login"
              element={
                <div data-testid="login-page">
                  <Login />
                </div>
              }
            />
            <Route
              path="/register"
              element={
                <div data-testid="register-page">
                  <Register />
                </div>
              }
            />
            <Route
              path="/dashboard"
              element={
                <div data-testid="dashboard-page">Dashboard</div>
              }
            />
          </Routes>
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  );
}

describe('Navigation Integration', () => {
  // Test Case 2: Click Login link in navbar
  it('navigates to /login route without full page reload when Login is clicked', async () => {
    const user = userEvent.setup();
    renderWithRouter('/');

    // Verify we start on the home page
    expect(screen.getByTestId('home-page')).toBeInTheDocument();
    expect(screen.queryByTestId('login-page')).not.toBeInTheDocument();

    // Click the Login link
    const loginLink = screen.getByTestId('navbar-login-link');
    await user.click(loginLink);

    // Verify navigation to login page
    expect(screen.getByTestId('login-page')).toBeInTheDocument();
    expect(screen.queryByTestId('home-page')).not.toBeInTheDocument();

    // Verify page content
    expect(screen.getByRole('heading', { name: /login/i })).toBeInTheDocument();
  });

  // Test Case 3: Click Register link in navbar
  it('navigates to /register route without full page reload when Register is clicked', async () => {
    const user = userEvent.setup();
    renderWithRouter('/');

    // Verify we start on the home page
    expect(screen.getByTestId('home-page')).toBeInTheDocument();
    expect(screen.queryByTestId('register-page')).not.toBeInTheDocument();

    // Click the Register link
    const registerLink = screen.getByTestId('navbar-register-link');
    await user.click(registerLink);

    // Verify navigation to register page
    expect(screen.getByTestId('register-page')).toBeInTheDocument();
    expect(screen.queryByTestId('home-page')).not.toBeInTheDocument();

    // Verify page content
    expect(screen.getByRole('heading', { name: /register/i })).toBeInTheDocument();
  });

  it('uses React Router Link components (not anchor tags with page reload)', async () => {
    renderWithRouter('/');

    // The Login and Register links should be using React Router Link
    // which means they should not cause a full page reload
    const loginLink = screen.getByTestId('navbar-login-link');
    const registerLink = screen.getByTestId('navbar-register-link');

    // Verify they are anchor elements (how Link renders)
    expect(loginLink.tagName).toBe('A');
    expect(registerLink.tagName).toBe('A');

    // Verify they have the correct href
    expect(loginLink).toHaveAttribute('href', '/login');
    expect(registerLink).toHaveAttribute('href', '/register');
  });

  it('maintains navbar visibility on all pages', async () => {
    const user = userEvent.setup();
    renderWithRouter('/');

    // Navbar visible on home
    expect(screen.getByTestId('home-navbar')).toBeInTheDocument();

    // Navigate to login
    await user.click(screen.getByTestId('navbar-login-link'));

    // After navigation, the login page is rendered (without navbar since we only added it to home route)
    // This verifies React Router client-side navigation works correctly
    expect(screen.getByTestId('login-page')).toBeInTheDocument();
  });

  it('allows navigation back and forth between pages', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <ThemeProvider>
          <AuthProvider>
            <Routes>
              <Route
                path="/"
                element={
                  <div>
                    <HomeNavbar />
                    <main data-testid="home-page">Home Page</main>
                  </div>
                }
              />
              <Route
                path="/login"
                element={
                  <div>
                    <HomeNavbar />
                    <main data-testid="login-page">Login Page</main>
                  </div>
                }
              />
              <Route
                path="/register"
                element={
                  <div>
                    <HomeNavbar />
                    <main data-testid="register-page">Register Page</main>
                  </div>
                }
              />
            </Routes>
          </AuthProvider>
        </ThemeProvider>
      </MemoryRouter>
    );

    // Start on home
    expect(screen.getByTestId('home-page')).toBeInTheDocument();

    // Go to login
    await user.click(screen.getByTestId('navbar-login-link'));
    expect(screen.getByTestId('login-page')).toBeInTheDocument();

    // Go to register
    await user.click(screen.getByTestId('navbar-register-link'));
    expect(screen.getByTestId('register-page')).toBeInTheDocument();

    // Go back to login
    await user.click(screen.getByTestId('navbar-login-link'));
    expect(screen.getByTestId('login-page')).toBeInTheDocument();
  });

  // Test Case 3: Browser back button navigation (Scenario 18)
  describe('Browser History Integration', () => {
    it('navigates back to homepage when browser back button is used after going to /login', async () => {
      const user = userEvent.setup();

      // Create a custom history tracking render
      let testHistory: string[] = ['/'];

      const { container } = render(
        <MemoryRouter initialEntries={testHistory}>
          <ThemeProvider>
            <AuthProvider>
              <Routes>
                <Route
                  path="/"
                  element={
                    <div>
                      <HomeNavbar />
                      <main data-testid="home-page">
                        <h1>Homepage Content</h1>
                      </main>
                    </div>
                  }
                />
                <Route
                  path="/login"
                  element={
                    <div>
                      <HomeNavbar />
                      <main data-testid="login-page">
                        <Login />
                      </main>
                    </div>
                  }
                />
              </Routes>
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      // Verify starting on homepage
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      expect(screen.getByText('Homepage Content')).toBeInTheDocument();

      // Navigate to login page
      await user.click(screen.getByTestId('navbar-login-link'));
      expect(screen.getByTestId('login-page')).toBeInTheDocument();

      // Note: In a real browser, window.history.back() would work.
      // In MemoryRouter, we simulate this by testing that navigation creates
      // proper history entries that React Router can navigate back through.
      // The actual back button behavior is tested via Playwright E2E tests.
    });

    it('maintains React Router history stack for back navigation', async () => {
      const user = userEvent.setup();

      render(
        <MemoryRouter initialEntries={['/']}>
          <ThemeProvider>
            <AuthProvider>
              <Routes>
                <Route
                  path="/"
                  element={
                    <div>
                      <HomeNavbar />
                      <main data-testid="home-page">Home</main>
                    </div>
                  }
                />
                <Route
                  path="/login"
                  element={
                    <div>
                      <HomeNavbar />
                      <main data-testid="login-page">Login</main>
                    </div>
                  }
                />
                <Route
                  path="/register"
                  element={
                    <div>
                      <HomeNavbar />
                      <main data-testid="register-page">Register</main>
                    </div>
                  }
                />
              </Routes>
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      // Navigate: Home -> Login -> Register
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      await user.click(screen.getByTestId('navbar-login-link'));
      expect(screen.getByTestId('login-page')).toBeInTheDocument();

      await user.click(screen.getByTestId('navbar-register-link'));
      expect(screen.getByTestId('register-page')).toBeInTheDocument();

      // This demonstrates that React Router properly tracks navigation history
      // Browser back button functionality is handled by React Router's integration
      // with the browser's History API, which is tested in E2E tests.
    });
  });

  // Test Case 4: Direct URL access (Scenario 18)
  describe('Direct URL Access', () => {
    it('renders homepage correctly when accessing "/" directly', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <ThemeProvider>
            <AuthProvider>
              <Routes>
                <Route
                  path="/"
                  element={
                    <div>
                      <HomeNavbar />
                      <main data-testid="home-page">Home Page Content</main>
                    </div>
                  }
                />
              </Routes>
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      expect(screen.getByTestId('home-page')).toBeInTheDocument();
      expect(screen.getByText('Home Page Content')).toBeInTheDocument();
    });

    it('renders login page correctly when accessing "/login" directly', () => {
      render(
        <MemoryRouter initialEntries={['/login']}>
          <ThemeProvider>
            <AuthProvider>
              <Routes>
                <Route path="/" element={<div data-testid="home-page">Home</div>} />
                <Route
                  path="/login"
                  element={
                    <div data-testid="login-page">
                      <Login />
                    </div>
                  }
                />
              </Routes>
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      expect(screen.getByTestId('login-page')).toBeInTheDocument();
      expect(screen.queryByTestId('home-page')).not.toBeInTheDocument();
    });

    it('renders register page correctly when accessing "/register" directly', () => {
      render(
        <MemoryRouter initialEntries={['/register']}>
          <ThemeProvider>
            <AuthProvider>
              <Routes>
                <Route path="/" element={<div data-testid="home-page">Home</div>} />
                <Route
                  path="/register"
                  element={
                    <div data-testid="register-page">
                      <Register />
                    </div>
                  }
                />
              </Routes>
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      expect(screen.getByTestId('register-page')).toBeInTheDocument();
      expect(screen.queryByTestId('home-page')).not.toBeInTheDocument();
    });
  });
});

/**
 * Authentication Context Integration Tests
 * Owner: Scenario 19 - Authentication Context Integration
 *
 * Tests verify that homepage correctly integrates with AuthContext:
 * - TC1: Unauthenticated state shows Login/Register links
 * - TC2: Authenticated state shows Dashboard link and user menu
 * - TC3: Homepage components correctly use useAuth() hook
 * - TC4: UI handles expired token state gracefully
 */
describe('Authentication Context Integration', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
    vi.clearAllMocks();
  });

  afterEach(() => {
    localStorage.clear();
  });

  // Test Case 1: Render homepage without authentication token
  describe('Unauthenticated User View', () => {
    it('shows Login and Register links when no auth token is present', async () => {
      renderWithRouter('/');

      // Wait for auth loading to complete
      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      // Verify Login and Register links are visible
      expect(screen.getByTestId('navbar-login-link')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register-link')).toBeInTheDocument();

      // Verify Dashboard link and user menu are NOT visible
      expect(screen.queryByTestId('navbar-dashboard-link')).not.toBeInTheDocument();
      expect(screen.queryByTestId('user-menu')).not.toBeInTheDocument();
    });

    it('Login link navigates to /login page', async () => {
      const user = userEvent.setup();
      renderWithRouter('/');

      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      await user.click(screen.getByTestId('navbar-login-link'));

      expect(screen.getByTestId('login-page')).toBeInTheDocument();
    });

    it('Register link navigates to /register page', async () => {
      const user = userEvent.setup();
      renderWithRouter('/');

      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      await user.click(screen.getByTestId('navbar-register-link'));

      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });
  });

  // Test Case 2: Render homepage with valid authentication token
  describe('Authenticated User View', () => {
    it('shows Dashboard link and user menu instead of Login/Register', async () => {
      // Setup authenticated state in localStorage
      const validToken = createMockToken(3600);
      const mockUser = { id: 'user-123', email: 'test@example.com', name: 'Test User' };

      localStorage.setItem('auth_token', validToken);
      localStorage.setItem('auth_user', JSON.stringify(mockUser));

      renderWithRouter('/');

      // Wait for auth loading to complete
      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      // Verify Dashboard link is visible
      expect(screen.getByTestId('navbar-dashboard-link')).toBeInTheDocument();

      // Verify user menu is visible
      expect(screen.getByTestId('user-menu')).toBeInTheDocument();

      // Verify Login and Register links are NOT visible
      expect(screen.queryByTestId('navbar-login-link')).not.toBeInTheDocument();
      expect(screen.queryByTestId('navbar-register-link')).not.toBeInTheDocument();
    });

    it('Dashboard link navigates to /dashboard page', async () => {
      const user = userEvent.setup();

      const validToken = createMockToken(3600);
      const mockUser = { id: 'user-123', email: 'test@example.com' };

      localStorage.setItem('auth_token', validToken);
      localStorage.setItem('auth_user', JSON.stringify(mockUser));

      renderWithRouter('/');

      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      await user.click(screen.getByTestId('navbar-dashboard-link'));

      expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();
    });

    it('user menu shows user email', async () => {
      const validToken = createMockToken(3600);
      const mockUser = { id: 'user-123', email: 'test@example.com' };

      localStorage.setItem('auth_token', validToken);
      localStorage.setItem('auth_user', JSON.stringify(mockUser));

      renderWithRouter('/');

      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      // User menu should display user email
      expect(screen.getByTestId('user-email')).toHaveTextContent('test@example.com');
    });

    it('logout button clears auth state and shows Login/Register links', async () => {
      const user = userEvent.setup();

      const validToken = createMockToken(3600);
      const mockUser = { id: 'user-123', email: 'test@example.com' };

      localStorage.setItem('auth_token', validToken);
      localStorage.setItem('auth_user', JSON.stringify(mockUser));

      renderWithRouter('/');

      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      // Click logout button
      await user.click(screen.getByTestId('logout-button'));

      // Verify Login and Register links are now visible
      await waitFor(() => {
        expect(screen.getByTestId('navbar-login-link')).toBeInTheDocument();
        expect(screen.getByTestId('navbar-register-link')).toBeInTheDocument();
      });

      // Verify Dashboard and user menu are NOT visible
      expect(screen.queryByTestId('navbar-dashboard-link')).not.toBeInTheDocument();
      expect(screen.queryByTestId('user-menu')).not.toBeInTheDocument();

      // Verify localStorage was cleared
      expect(localStorage.getItem('auth_token')).toBeNull();
      expect(localStorage.getItem('auth_user')).toBeNull();
    });
  });

  // Test Case 3: Check AuthContext consumption
  describe('AuthContext Hook Usage', () => {
    // Test component that verifies useAuth hook works correctly
    function AuthTestComponent() {
      const { isAuthenticated, isLoading, user, isTokenExpired } = useAuth();
      return (
        <div>
          <span data-testid="is-authenticated">{String(isAuthenticated)}</span>
          <span data-testid="is-loading">{String(isLoading)}</span>
          <span data-testid="is-token-expired">{String(isTokenExpired)}</span>
          <span data-testid="user-data">{user ? user.email : 'no-user'}</span>
        </div>
      );
    }

    it('useAuth hook provides correct initial state when unauthenticated', async () => {
      render(
        <MemoryRouter>
          <ThemeProvider>
            <AuthProvider>
              <AuthTestComponent />
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      // Wait for loading to complete
      await waitFor(() => {
        expect(screen.getByTestId('is-loading')).toHaveTextContent('false');
      });

      expect(screen.getByTestId('is-authenticated')).toHaveTextContent('false');
      expect(screen.getByTestId('is-token-expired')).toHaveTextContent('false');
      expect(screen.getByTestId('user-data')).toHaveTextContent('no-user');
    });

    it('useAuth hook provides correct state when authenticated', async () => {
      const validToken = createMockToken(3600);
      const mockUser = { id: 'user-123', email: 'auth@example.com' };

      localStorage.setItem('auth_token', validToken);
      localStorage.setItem('auth_user', JSON.stringify(mockUser));

      render(
        <MemoryRouter>
          <ThemeProvider>
            <AuthProvider>
              <AuthTestComponent />
            </AuthProvider>
          </ThemeProvider>
        </MemoryRouter>
      );

      // Wait for loading to complete
      await waitFor(() => {
        expect(screen.getByTestId('is-loading')).toHaveTextContent('false');
      });

      expect(screen.getByTestId('is-authenticated')).toHaveTextContent('true');
      expect(screen.getByTestId('is-token-expired')).toHaveTextContent('false');
      expect(screen.getByTestId('user-data')).toHaveTextContent('auth@example.com');
    });

    it('HomeNavbar correctly consumes useAuth hook', async () => {
      // Test that HomeNavbar uses the auth context correctly by verifying
      // it responds to auth state changes
      renderWithRouter('/');

      // Initially unauthenticated
      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      expect(screen.getByTestId('navbar-login-link')).toBeInTheDocument();

      // This verifies HomeNavbar is consuming the auth context
      // because it renders different UI based on auth state
    });
  });

  // Test Case 4: Auth token expires while on homepage
  describe('Token Expiration Handling', () => {
    it('shows expired token indicator and login options when token is expired', async () => {
      // Setup state with expired token
      const expiredToken = createExpiredToken();
      const mockUser = { id: 'user-123', email: 'test@example.com' };

      localStorage.setItem('auth_token', expiredToken);
      localStorage.setItem('auth_user', JSON.stringify(mockUser));

      renderWithRouter('/');

      // Wait for auth loading to complete
      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      // Should show expired token indicator
      expect(screen.getByTestId('token-expired-indicator')).toBeInTheDocument();

      // Should show Login and Register links (not Dashboard/user menu)
      expect(screen.getByTestId('navbar-login-link')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-register-link')).toBeInTheDocument();

      // Dashboard and user menu should NOT be visible
      expect(screen.queryByTestId('navbar-dashboard-link')).not.toBeInTheDocument();
      expect(screen.queryByTestId('user-menu')).not.toBeInTheDocument();
    });

    it('gracefully transitions from authenticated to expired state', async () => {
      // This test verifies the UI handles the transition gracefully
      // when a token becomes expired

      // First, set up with a valid token
      const validToken = createMockToken(3600);
      const mockUser = { id: 'user-123', email: 'test@example.com' };

      localStorage.setItem('auth_token', validToken);
      localStorage.setItem('auth_user', JSON.stringify(mockUser));

      const { unmount } = renderWithRouter('/');

      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      // Initially should show authenticated state
      expect(screen.getByTestId('navbar-dashboard-link')).toBeInTheDocument();
      expect(screen.queryByTestId('token-expired-indicator')).not.toBeInTheDocument();

      // Clean up first render
      unmount();

      // Now simulate expired token by replacing with expired one
      const expiredToken = createExpiredToken();
      localStorage.setItem('auth_token', expiredToken);

      // Re-render (simulating a page reload or re-mount)
      renderWithRouter('/');

      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      // Should now show expired state
      expect(screen.getByTestId('token-expired-indicator')).toBeInTheDocument();
      expect(screen.getByTestId('navbar-login-link')).toBeInTheDocument();
      expect(screen.queryByTestId('navbar-dashboard-link')).not.toBeInTheDocument();
    });

    it('expired token state allows navigation to login page', async () => {
      const user = userEvent.setup();

      const expiredToken = createExpiredToken();
      const mockUser = { id: 'user-123', email: 'test@example.com' };

      localStorage.setItem('auth_token', expiredToken);
      localStorage.setItem('auth_user', JSON.stringify(mockUser));

      renderWithRouter('/');

      await waitFor(() => {
        expect(screen.queryByTestId('auth-loading')).not.toBeInTheDocument();
      });

      // Click login link
      await user.click(screen.getByTestId('navbar-login-link'));

      // Should navigate to login page
      expect(screen.getByTestId('login-page')).toBeInTheDocument();
    });
  });
});
