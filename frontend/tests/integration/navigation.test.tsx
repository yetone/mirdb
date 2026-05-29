import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import App from '../../src/App';

// Mock fetch for auth-related API calls
const mockFetch = vi.fn();

describe('Navigation and Routing Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    global.fetch = mockFetch;
  });

  const renderApp = (initialEntries = ['/']) => {
    return render(
      <MemoryRouter initialEntries={initialEntries}>
        <App />
      </MemoryRouter>
    );
  };

  it('TC1: Click Login link from homepage navigates to /login with login form', async () => {
    const user = userEvent.setup();
    renderApp();

    expect(screen.getByTestId('home-page')).toBeInTheDocument();

    const loginLink = screen.getByTestId('nav-login');
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveAttribute('href', '/login');

    await user.click(loginLink);

    await waitFor(() => {
      expect(screen.getByTestId('login-page')).toBeInTheDocument();
    });

    expect(screen.getByTestId('login-form')).toBeInTheDocument();
    expect(screen.getByTestId('login-username')).toBeInTheDocument();
    expect(screen.getByTestId('login-password')).toBeInTheDocument();
  });

  it('TC2: Click Register link from homepage navigates to /register with registration form', async () => {
    const user = userEvent.setup();
    renderApp();

    const registerLink = screen.getByTestId('nav-register');
    expect(registerLink).toBeInTheDocument();
    expect(registerLink).toHaveAttribute('href', '/register');

    await user.click(registerLink);

    await waitFor(() => {
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });

    expect(screen.getByTestId('register-form')).toBeInTheDocument();
    expect(screen.getByTestId('register-username')).toBeInTheDocument();
    expect(screen.getByTestId('register-email')).toBeInTheDocument();
    expect(screen.getByTestId('register-password')).toBeInTheDocument();
  });

  it('TC3: Click Get Started CTA button in hero navigates to /register', async () => {
    const user = userEvent.setup();
    renderApp();

    const ctaButton = screen.getByTestId('hero-cta-button');
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toHaveAttribute('href', '/register');

    await user.click(ctaButton);

    await waitFor(() => {
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });
  });

  it('TC6: Click Home/Logo link from another page navigates back to homepage', async () => {
    const user = userEvent.setup();
    renderApp(['/login']);

    expect(screen.getByTestId('login-page')).toBeInTheDocument();

    const homeLink = screen.getByTestId('nav-home-link');
    expect(homeLink).toBeInTheDocument();
    expect(homeLink).toHaveAttribute('href', '/');

    await user.click(homeLink);

    await waitFor(() => {
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });
  });

  it('TC4: Authenticated user sees Dashboard link and can navigate to /dashboard', async () => {
    const user = userEvent.setup();

    // Mock token endpoint
    mockFetch.mockImplementation((url: string) => {
      if (url === '/api/token') {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({ access_token: 'mock-token', token_type: 'bearer' }),
        } as Response);
      }
      if (url === '/api/users/me') {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({ id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 }),
        } as Response);
      }
      return Promise.resolve({ ok: false, json: () => Promise.resolve({}) } as Response);
    });

    renderApp(['/login']);

    // Login
    await user.type(screen.getByTestId('login-username'), 'testuser');
    await user.type(screen.getByTestId('login-password'), 'password123');
    await user.click(screen.getByTestId('login-submit'));

    await waitFor(() => {
      expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();
    });

    // Go back to homepage
    const homeLink = screen.getByTestId('nav-home-link');
    await user.click(homeLink);

    await waitFor(() => {
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });

    // Dashboard link should be visible
    const dashboardLink = screen.getByTestId('nav-dashboard');
    expect(dashboardLink).toBeInTheDocument();
    expect(dashboardLink).toHaveAttribute('href', '/dashboard');

    // Click Dashboard
    await user.click(dashboardLink);

    await waitFor(() => {
      expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();
    });
  });
});
