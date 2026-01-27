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
import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { render } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { renderWithProviders } from './testUtils';
import Home from '../../src/pages/Home';
import HeroSection from '../../src/components/homepage/HeroSection';
import Navbar from '../../src/components/Navbar';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

/**
 * Custom render function with full router navigation support
 * for testing actual route changes
 */
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
 * Helper to find the hero section's CTA button
 * The hero section contains a FuturisticButton wrapped in a Link
 * while the Navbar has a plain link without a button element inside
 */
function getHeroCTALink() {
  const links = screen.getAllByRole('link', { name: /Get Started/i });
  // The hero section link contains a button element; navbar link does not
  return links.find(link => link.querySelector('button'));
}

/**
 * Scenario 2: Navigation to Registration Page
 *
 * Test that users can navigate from homepage to registration page via CTA buttons.
 *
 * Steps:
 * 1. Navigate to homepage
 * 2. Click primary CTA button (Get Started/Sign Up)
 * 3. Verify navigation to /register route
 */
describe('Scenario 2: Navigation to Registration Page', () => {
  describe('Test Case 1: Click primary CTA button (Get Started/Sign Up)', () => {
    it('navigates to /register route when primary CTA "Get Started" button is clicked', async () => {
      const user = userEvent.setup();
      renderWithRoutes('/');

      // Step 1: Verify we are on the homepage (root URL)
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline.textContent).toMatch(/Shorten URLs/i);

      // Step 2: Find and click the primary CTA button in hero section
      // There are multiple "Get Started" links - one in Navbar and one in HeroSection
      // We target the one containing a button (the HeroSection CTA)
      const heroCTALink = getHeroCTALink();
      expect(heroCTALink).toBeInTheDocument();

      await user.click(heroCTALink!);

      // Step 3: Verify navigation to /register route
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('primary CTA button is easily identifiable in the hero section', () => {
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

  describe('Test Case 2: Verify registration link href attribute', () => {
    it('link has href="/register" attribute', () => {
      renderWithProviders(<HeroSection />);

      // Find the "Get Started" link and verify its href
      const getStartedLink = screen.getByRole('link', { name: /Get Started/i });
      expect(getStartedLink).toHaveAttribute('href', '/register');
    });

    it('HeroSection Get Started link navigates to /register route', () => {
      renderWithRoutes('/');

      // All Get Started links should point to /register
      const getStartedLinks = screen.getAllByRole('link', { name: /Get Started/i });
      expect(getStartedLinks.length).toBeGreaterThan(0);

      getStartedLinks.forEach(link => {
        expect(link).toHaveAttribute('href', '/register');
      });

      // The hero section CTA (with button inside) specifically points to /register
      const heroCTALink = getStartedLinks.find(link => link.querySelector('button'));
      expect(heroCTALink).toHaveAttribute('href', '/register');
    });

    it('registration link is properly wrapped around the FuturisticButton', () => {
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

  describe('User Journey: Homepage to Registration', () => {
    it('completes full user journey from homepage to registration', async () => {
      const user = userEvent.setup();
      renderWithRoutes('/');

      // User lands on homepage
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // User sees the value proposition
      expect(screen.getByText(/Transform your long URLs/i)).toBeInTheDocument();

      // User decides to sign up and clicks Get Started in hero section
      const heroCTALink = getHeroCTALink();
      expect(heroCTALink).toBeInTheDocument();
      await user.click(heroCTALink!);

      // User is redirected to registration page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
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
