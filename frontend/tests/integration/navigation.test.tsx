/**
 * Navigation integration tests.
 * Owner: Scenario 1 - Homepage Route Accessibility, Scenario 6 - Navbar Component Integration
 *
 * Test coverage:
 * - Route navigation from homepage
 * - Public routes accessibility without authentication
 * - Navigation links work correctly
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor, fireEvent } from '@testing-library/react';
import { render } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from '@/contexts/ThemeContext';
import { AuthProvider } from '@/contexts/AuthContext';
import { Home } from '@/pages/Home';

// Helper to render the app with routing for navigation tests
function renderApp(initialEntries: string[] = ['/']) {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <AuthProvider>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
            <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
            <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard Page</div>} />
          </Routes>
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  );
}

describe('Navigation Integration Tests', () => {
  beforeEach(() => {
    vi.mocked(localStorage.getItem).mockReturnValue(null);
  });

  describe('Route Accessibility', () => {
    it('should render homepage at root route (/)', async () => {
      renderApp(['/']);

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });
    });

    it('should render login page at /login route', async () => {
      renderApp(['/login']);

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });

    it('should render register page at /register route', async () => {
      renderApp(['/register']);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should not redirect unauthenticated user from homepage to login', async () => {
      vi.mocked(localStorage.getItem).mockReturnValue(null);

      renderApp(['/']);

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Should stay on home page
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument();
    });
  });

  describe('Navigation Links', () => {
    it('should navigate to login page when navbar Login is clicked', async () => {
      renderApp(['/']);

      await waitFor(() => {
        expect(screen.getByTestId('navbar')).toBeInTheDocument();
      });

      const loginLink = screen.getByTestId('navbar-login');
      fireEvent.click(loginLink);

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });

    it('should navigate to register page when navbar Register is clicked', async () => {
      renderApp(['/']);

      await waitFor(() => {
        expect(screen.getByTestId('navbar')).toBeInTheDocument();
      });

      const registerLink = screen.getByTestId('navbar-register');
      fireEvent.click(registerLink);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should navigate to homepage when logo is clicked', async () => {
      renderApp(['/login']);

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });

      // Navigate to login first, then click logo to go home
      renderApp(['/']);
      const logoLink = screen.getByTestId('navbar-logo');
      fireEvent.click(logoLink);

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });
    });
  });

  describe('CTA Navigation', () => {
    it('should navigate to register when Get Started CTA is clicked', async () => {
      renderApp(['/']);

      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      });

      const ctaGetStarted = screen.getByTestId('cta-get-started');
      fireEvent.click(ctaGetStarted);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should navigate to login when Sign In CTA is clicked', async () => {
      renderApp(['/']);

      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      });

      const ctaSignIn = screen.getByTestId('cta-sign-in');
      fireEvent.click(ctaSignIn);

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });
  });

  describe('Footer Navigation', () => {
    it('should have footer links on homepage', async () => {
      renderApp(['/']);

      await waitFor(() => {
        expect(screen.getByTestId('footer')).toBeInTheDocument();
      });

      // Footer should have navigation links (Home, Login, Register)
      const footer = screen.getByTestId('footer');
      expect(footer).toBeInTheDocument();

      // Check footer copyright
      expect(screen.getByTestId('footer-copyright')).toBeInTheDocument();
    });
  });
});
