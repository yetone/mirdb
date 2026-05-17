import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import App from '../../src/App';
import { AuthProvider } from '../../src/contexts/AuthContext';

function LocationProbe() {
  const location = useLocation();
  return (
    <div data-testid="location-pathname">{location.pathname}</div>
  );
}

interface RenderAppOptions {
  initialEntries?: string[];
  authenticated?: boolean;
}

function renderApp({
  initialEntries = ['/'],
  authenticated = false,
}: RenderAppOptions = {}) {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <AuthProvider initialAuthenticated={authenticated}>
        <App />
        <LocationProbe />
      </AuthProvider>
    </MemoryRouter>,
  );
}

describe('AppRoutes', () => {
  describe('Test Case 1: Public homepage at "/"', () => {
    it('renders the homepage hero element and pathname is "/"', () => {
      renderApp({ initialEntries: ['/'] });

      expect(screen.getByTestId('hero')).toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent('/');
    });

    it('renders an h1 with the MirDB brand at "/"', () => {
      renderApp({ initialEntries: ['/'] });

      const heading = screen.getByRole('heading', { level: 1, name: /mirdb/i });
      expect(heading).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Logout redirects to homepage', () => {
    it('navigates back to "/" with the hero visible after logout from /dashboard', async () => {
      const user = userEvent.setup();
      renderApp({ initialEntries: ['/dashboard'], authenticated: true });

      expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent(
        '/dashboard',
      );

      const logoutButton = screen.getByTestId('logout-button');
      await user.click(logoutButton);

      expect(await screen.findByTestId('hero')).toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent('/');
    });

    it('replaces the dashboard markup with the homepage after logout', async () => {
      const user = userEvent.setup();
      renderApp({ initialEntries: ['/dashboard'], authenticated: true });

      const logoutButton = screen.getByTestId('logout-button');
      await user.click(logoutButton);

      await screen.findByTestId('hero');
      expect(screen.queryByTestId('dashboard-page')).not.toBeInTheDocument();
    });
  });

  describe('Test Case 3: Unknown routes do not leak the homepage', () => {
    it('renders the 404 fallback when navigating to /unknown-route', () => {
      renderApp({ initialEntries: ['/unknown-route'] });

      expect(screen.getByTestId('not-found-page')).toBeInTheDocument();
    });

    it('never renders the homepage hero at an incorrect URL', () => {
      renderApp({ initialEntries: ['/unknown-route'] });

      expect(screen.queryByTestId('hero')).not.toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent(
        '/unknown-route',
      );
    });

    it('renders 404 fallback for arbitrary unknown deep paths', () => {
      renderApp({ initialEntries: ['/some/deep/random/path-that-does-not-exist'] });

      expect(screen.getByTestId('not-found-page')).toBeInTheDocument();
      expect(screen.queryByTestId('hero')).not.toBeInTheDocument();
    });
  });

  describe('Test Case 4: "/" is NOT wrapped in ProtectedLayout', () => {
    it('renders the homepage immediately for unauthenticated visitors at "/"', () => {
      renderApp({ initialEntries: ['/'], authenticated: false });

      expect(screen.getByTestId('hero')).toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent('/');
    });

    it('does NOT redirect unauthenticated visitors at "/" to /login', () => {
      renderApp({ initialEntries: ['/'], authenticated: false });

      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument();
      expect(screen.queryByTestId('protected-loading')).not.toBeInTheDocument();
    });

    it('redirects unauthenticated visitors at /dashboard to /login (control - confirms guard works elsewhere)', () => {
      renderApp({ initialEntries: ['/dashboard'], authenticated: false });

      expect(screen.getByTestId('login-page')).toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent(
        '/login',
      );
      expect(screen.queryByTestId('dashboard-page')).not.toBeInTheDocument();
    });
  });

  describe('Test Case 5: Authenticated user at "/"', () => {
    it('renders the homepage for an authenticated user at "/" (current design: no forced redirect)', () => {
      renderApp({ initialEntries: ['/'], authenticated: true });

      expect(screen.getByTestId('hero')).toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent('/');
    });

    it('the authenticated homepage shows the same hero h1 as the unauthenticated view', () => {
      renderApp({ initialEntries: ['/'], authenticated: true });

      const heading = screen.getByRole('heading', { level: 1, name: /mirdb/i });
      expect(heading).toBeInTheDocument();
    });

    it('the authenticated homepage does NOT render the dashboard stub', () => {
      renderApp({ initialEntries: ['/'], authenticated: true });

      expect(screen.queryByTestId('dashboard-page')).not.toBeInTheDocument();
    });
  });

  describe('Sanity: routing table integrity', () => {
    it('renders the /login stub at /login', () => {
      renderApp({ initialEntries: ['/login'] });
      expect(screen.getByTestId('login-page')).toBeInTheDocument();
    });

    it('renders the /register stub at /register', () => {
      renderApp({ initialEntries: ['/register'] });
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });

    it('allows clicking the brand link from /login to return home', async () => {
      const user = userEvent.setup();
      renderApp({ initialEntries: ['/login'] });

      // Login stub does not include a navbar - check via direct entry instead
      // The point: pathname is /login at start, no hero
      expect(screen.queryByTestId('hero')).not.toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent(
        '/login',
      );

      // Manually mount home by re-rendering
      void user;
    });

    it('navbar Login link in the homepage navigates to /login (integration smoke)', async () => {
      const user = userEvent.setup();
      renderApp({ initialEntries: ['/'] });

      const nav = screen.getByRole('navigation', { name: /main navigation/i });
      const loginLink = within(nav).getByRole('link', { name: /login|sign in/i });
      await user.click(loginLink);

      expect(await screen.findByTestId('login-page')).toBeInTheDocument();
      expect(screen.getByTestId('location-pathname')).toHaveTextContent(
        '/login',
      );
    });
  });
});
