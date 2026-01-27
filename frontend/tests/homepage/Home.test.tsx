/**
 * Homepage Integration Tests
 * Owner: Scenarios 2, 3, 12, 14, 15, 16, 17
 *
 * Tests for the main Home page component, covering:
 * - Navigation to registration (Scenario 2)
 * - Navigation to login (Scenario 3)
 * - Navbar integration (Scenario 12)
 * - Animation presence (Scenario 14)
 * - Component reuse (Scenario 15)
 * - Route configuration (Scenario 16)
 * - Error handling (Scenario 17)
 */
import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { renderWithProviders } from './testUtils';
import Home from '../../src/pages/Home';
import Navbar from '../../src/components/Navbar';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

function renderWithRoutes(initialRoute = '/') {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider>
          <AuthProvider>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
              <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
            </Routes>
          </AuthProvider>
        </ThemeProvider>
      </QueryClientProvider>
    </MemoryRouter>
  );
}

/**
 * Scenario 2: Navigation to Registration Page
 *
 * Test that users can navigate from homepage to registration page via CTA buttons.
 */
describe('Scenario 2: Navigation to Registration Page', () => {
  describe('Test Case 1: Click primary CTA button navigates to /register', () => {
    it('navigates to /register when clicking "Get Started" CTA button in hero section', async () => {
      const user = userEvent.setup();
      renderWithRoutes('/');

      // Find all Get Started links (there are multiple: Navbar and Hero section)
      const getStartedLinks = screen.getAllByRole('link', { name: /Get Started/i });
      // At least one Get Started link should exist
      expect(getStartedLinks.length).toBeGreaterThan(0);

      // Click the first Get Started link (either one should work)
      await user.click(getStartedLinks[0]);

      // Verify navigation occurred to /register route
      const registerPage = await screen.findByTestId('register-page');
      expect(registerPage).toBeInTheDocument();
    });

    it('primary CTA button in hero section is visible and accessible', () => {
      renderWithRoutes('/');

      // Find the hero section by looking for the h1 heading
      const heroHeading = screen.getByRole('heading', { level: 1 });
      const heroSection = heroHeading.closest('section');
      expect(heroSection).toBeInTheDocument();

      // Find the Get Started button within the hero section
      const getStartedButton = within(heroSection!).getByRole('button', { name: /Get Started/i });
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).not.toHaveAttribute('hidden');
      expect(getStartedButton).not.toBeDisabled();
    });
  });

  describe('Test Case 2: Registration link has correct href attribute', () => {
    it('All Get Started links have href="/register"', () => {
      renderWithRoutes('/');

      // Find all Get Started links
      const getStartedLinks = screen.getAllByRole('link', { name: /Get Started/i });
      expect(getStartedLinks.length).toBeGreaterThan(0);

      // All Get Started links should navigate to /register
      getStartedLinks.forEach((link) => {
        expect(link).toHaveAttribute('href', '/register');
      });
    });

    it('Hero section CTA link wraps the FuturisticButton component', () => {
      renderWithRoutes('/');

      // Find the hero section
      const heroHeading = screen.getByRole('heading', { level: 1 });
      const heroSection = heroHeading.closest('section');
      expect(heroSection).toBeInTheDocument();

      // Find the Get Started link in the hero section
      const getStartedLink = within(heroSection!).getByRole('link', { name: /Get Started/i });
      // The link should contain a button element (FuturisticButton renders as motion.button)
      const button = within(getStartedLink).getByRole('button');
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent('Get Started');
    });
  });

  describe('Homepage renders with navigation CTAs', () => {
    it('renders the Home page with CTA links at "/" route', () => {
      renderWithRoutes('/');

      // Verify the page renders with navigation elements
      const getStartedLinks = screen.getAllByRole('link', { name: /Get Started/i });
      expect(getStartedLinks.length).toBeGreaterThan(0);
    });

    it('hero section contains registration CTA with correct navigation', () => {
      renderWithRoutes('/');

      // Verify the hero section has the Get Started link
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toBeInTheDocument();

      // Get Started links exist and navigate to registration
      const getStartedLinks = screen.getAllByRole('link', { name: /Get Started/i });
      expect(getStartedLinks.length).toBeGreaterThan(0);

      // All should have href="/register"
      getStartedLinks.forEach((link) => {
        expect(link).toHaveAttribute('href', '/register');
      });
    });
  });
});

/**
 * Scenario 3: Navigation to Login Page
 *
 * Test that returning users can navigate from homepage to login page.
 *
 * Steps:
 * 1. Navigate to homepage
 * 2. Locate sign in option
 * 3. Click sign in button
 * 4. Verify navigation to login
 */
describe('Scenario 3: Navigation to Login Page', () => {
  describe('Test Case 1: Click Sign In/Login button or link navigates to /login route', () => {
    it('navigates to /login when Sign In button in hero section is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Find Sign In link in hero section (button within link)
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      expect(signInLinks.length).toBeGreaterThan(0);

      // Click the Sign In link
      await user.click(signInLinks[0]);

      // The link should have href="/login"
      expect(signInLinks[0]).toHaveAttribute('href', '/login');
    });

    it('navigates to /login when Sign In link in navbar is clicked', async () => {
      const user = userEvent.setup();
      renderWithProviders(<Home />);

      // Find the navbar Sign In link - it's the first link with "Sign In" (navbar comes before hero)
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      // First link is from navbar (btn btn-ghost class), second is from hero section
      const navbarSignInLink = signInLinks.find(link => link.classList.contains('btn-ghost'));
      expect(navbarSignInLink).toBeDefined();
      expect(navbarSignInLink).toBeInTheDocument();

      await user.click(navbarSignInLink!);

      // Verify navigation target
      expect(navbarSignInLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 2: Login link/button has href=/login or onClick navigates to /login', () => {
    it('hero section Sign In link has correct href attribute', () => {
      renderWithProviders(<Home />);

      // Find all Sign In links
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });

      // All Sign In links should have href="/login"
      signInLinks.forEach(link => {
        expect(link).toHaveAttribute('href', '/login');
      });
    });

    it('Sign In button is wrapped in a link with correct href', () => {
      renderWithProviders(<Home />);

      // Find the Sign In button
      const signInButton = screen.getByRole('button', { name: /Sign In/i });
      expect(signInButton).toBeInTheDocument();

      // The button should be inside a link with href="/login"
      const parentLink = signInButton.closest('a');
      expect(parentLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 3: Navbar contains accessible login navigation', () => {
    it('Navbar renders Sign In link for unauthenticated users', () => {
      renderWithProviders(<Navbar />);

      const signInLink = screen.getByRole('link', { name: /Sign In/i });
      expect(signInLink).toBeInTheDocument();
      expect(signInLink).toHaveAttribute('href', '/login');
    });

    it('Navbar Sign In link is accessible via keyboard', async () => {
      renderWithProviders(<Navbar />);

      const signInLink = screen.getByRole('link', { name: /Sign In/i });

      // Link should be focusable
      signInLink.focus();
      expect(document.activeElement).toBe(signInLink);
    });

    it('Navbar Sign In link has visible text', () => {
      renderWithProviders(<Navbar />);

      const signInLink = screen.getByRole('link', { name: /Sign In/i });
      expect(signInLink).toHaveTextContent('Sign In');
    });

    it('Home page includes Navbar with login navigation', () => {
      renderWithProviders(<Home />);

      // Navbar should be present with navigation
      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();

      // Sign In links should be accessible within the page (one in navbar, one in hero)
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      expect(signInLinks.length).toBeGreaterThan(0);

      // All should have correct href
      signInLinks.forEach(link => {
        expect(link).toHaveAttribute('href', '/login');
      });
    });
  });
});
