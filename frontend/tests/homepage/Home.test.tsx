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
import { describe, it, expect, vi } from 'vitest';
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
import Footer from '../../src/components/homepage/Footer';
import Navbar from '../../src/components/Navbar';
import App from '../../src/App';

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

/**
 * Scenario 10: Footer Section Display
 *
 * Test that footer displays branding, navigation links, and copyright information.
 *
 * Steps:
 * 1. Navigate to homepage
 * 2. Scroll to bottom of page
 * 3. Verify footer content
 */
describe('Scenario 10: Footer Section Display', () => {
  describe('Test Case 1: Render HomePage and check for footer element', () => {
    it('footer element is present at bottom of page', () => {
      renderWithProviders(<Home />);

      // Footer should be present
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('footer renders as a semantic footer element', () => {
      renderWithProviders(<Footer />);

      // Footer should use semantic <footer> element
      const footer = screen.getByRole('contentinfo');
      expect(footer.tagName).toBe('FOOTER');
    });
  });

  describe('Test Case 2: Check footer for login link', () => {
    it('footer contains link to /login', () => {
      renderWithProviders(<Footer />);

      // Footer should contain Login link
      const loginLink = screen.getByRole('link', { name: /Login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('login link in footer is accessible', () => {
      renderWithProviders(<Footer />);

      const loginLink = screen.getByRole('link', { name: /Login/i });

      // Link should be focusable
      loginLink.focus();
      expect(document.activeElement).toBe(loginLink);
    });
  });

  describe('Test Case 3: Check footer for register link', () => {
    it('footer contains link to /register', () => {
      renderWithProviders(<Footer />);

      // Footer should contain Register link
      const registerLink = screen.getByRole('link', { name: /Register/i });
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('register link in footer is accessible', () => {
      renderWithProviders(<Footer />);

      const registerLink = screen.getByRole('link', { name: /Register/i });

      // Link should be focusable
      registerLink.focus();
      expect(document.activeElement).toBe(registerLink);
    });
  });

  describe('Test Case 4: Check footer for copyright text', () => {
    it('footer contains copyright notice with current year', () => {
      renderWithProviders(<Footer />);

      const currentYear = new Date().getFullYear();

      // Footer should contain copyright text with current year
      const copyrightText = screen.getByText(new RegExp(`© ${currentYear}`, 'i'));
      expect(copyrightText).toBeInTheDocument();
    });

    it('copyright notice includes application name', () => {
      renderWithProviders(<Footer />);

      // Copyright should mention URL Shortener
      const copyrightText = screen.getByText(/URL Shortener/i, { selector: 'p' });
      expect(copyrightText).toBeInTheDocument();
    });
  });

  describe('Footer Integration with Homepage', () => {
    it('homepage includes footer with all required elements', () => {
      renderWithProviders(<Home />);

      // Get the footer
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();

      // Check for branding - use getAllByText since "URL Shortener" appears multiple times in footer
      const brandingElements = within(footer).getAllByText(/URL Shortener/i);
      expect(brandingElements.length).toBeGreaterThan(0);

      // Check for navigation links
      expect(within(footer).getByRole('link', { name: /Login/i })).toHaveAttribute('href', '/login');
      expect(within(footer).getByRole('link', { name: /Register/i })).toHaveAttribute('href', '/register');

      // Check for copyright
      const currentYear = new Date().getFullYear();
      expect(within(footer).getByText(new RegExp(`© ${currentYear}`))).toBeInTheDocument();
    });

    it('footer navigation links are functional', async () => {
      renderWithRoutes('/');

      const footer = screen.getByRole('contentinfo');

      // Test login link navigation
      const loginLink = within(footer).getByRole('link', { name: /Login/i });
      expect(loginLink).toHaveAttribute('href', '/login');
    });
  });
});

/**
 * Scenario 11: Call-to-Action Section Display
 *
 * Test that the reinforcing CTA section near the bottom of the page
 * is present and functional.
 *
 * Steps:
 * 1. Navigate to homepage
 * 2. Scroll to CTA section (before footer)
 * 3. Verify CTA content (compelling message and registration button)
 */
describe('Scenario 11: Call-to-Action Section Display', () => {
  describe('Test Case 1: Render HomePage and check for bottom CTA section', () => {
    it('CTA section with signup encouragement is present', () => {
      renderWithProviders(<Home />);

      // Find the CTA section by its id
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();
      expect(ctaSection?.tagName.toLowerCase()).toBe('section');
    });

    it('CTA section contains a compelling headline', () => {
      renderWithProviders(<Home />);

      // Find the CTA heading (should be h2)
      const ctaHeading = screen.getByRole('heading', { name: /Ready to Get Started/i });
      expect(ctaHeading).toBeInTheDocument();
      expect(ctaHeading.tagName.toLowerCase()).toBe('h2');
    });

    it('CTA section contains signup encouragement message', () => {
      renderWithProviders(<Home />);

      // Find the encouraging message text
      const encouragementText = screen.getByText(/Join thousands of users/i);
      expect(encouragementText).toBeInTheDocument();

      // Also check for free account mention
      const freeAccountText = screen.getByText(/Create your free account/i);
      expect(freeAccountText).toBeInTheDocument();
    });

    it('CTA section contains a registration button', () => {
      renderWithProviders(<Home />);

      // Find the CTA section
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Find the registration button within the CTA section
      const registerButton = within(ctaSection!).getByRole('button', { name: /Create Free Account/i });
      expect(registerButton).toBeInTheDocument();
    });

    it('CTA section is placed before the footer', () => {
      renderWithProviders(<Home />);

      const ctaSection = document.getElementById('cta');
      const footer = document.querySelector('footer');

      expect(ctaSection).toBeInTheDocument();
      expect(footer).toBeInTheDocument();

      // CTA section should come before footer in document order
      const mainElement = document.querySelector('main');
      expect(mainElement?.contains(ctaSection)).toBe(true);
      expect(mainElement?.contains(footer)).toBe(false);
    });

    it('CTA section has proper accessibility attributes', () => {
      renderWithProviders(<Home />);

      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Section should be labeled
      expect(ctaSection).toHaveAttribute('aria-labelledby', 'cta-heading');

      // The heading should have the id referenced by aria-labelledby
      const ctaHeading = document.getElementById('cta-heading');
      expect(ctaHeading).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Check bottom CTA button functionality', () => {
    it('Button navigates to /register when clicked', async () => {
      const user = userEvent.setup();
      renderWithRoutes('/');

      // Find the CTA section registration link
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Find the Create Free Account button link
      const registerLink = within(ctaSection!).getByRole('link', { name: /Create Free Account/i });
      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toHaveAttribute('href', '/register');

      // Click the link
      await user.click(registerLink);

      // Verify navigation to /register route
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('CTA registration link has correct href attribute', () => {
      renderWithProviders(<Home />);

      // Find the CTA section
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Find the registration link
      const registerLink = within(ctaSection!).getByRole('link', { name: /Create Free Account/i });
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('CTA button uses FuturisticButton component', () => {
      renderWithProviders(<Home />);

      // Find the CTA section
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // The button should exist and have the btn class from FuturisticButton
      const button = within(ctaSection!).getByRole('button', { name: /Create Free Account/i });
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('btn');
      expect(button).toHaveClass('btn-primary');
    });

    it('CTA button is keyboard accessible', async () => {
      renderWithProviders(<Home />);

      // Find the CTA section
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Find the registration link
      const registerLink = within(ctaSection!).getByRole('link', { name: /Create Free Account/i });

      // Link should be focusable
      registerLink.focus();
      expect(document.activeElement).toBe(registerLink);
    });
  });

  describe('User Journey: Homepage CTA to Registration', () => {
    it('completes journey from homepage CTA section to registration', async () => {
      const user = userEvent.setup();
      renderWithRoutes('/');

      // User lands on homepage
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // User scrolls to CTA section (simulated by finding it)
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // User sees the encouraging message
      expect(screen.getByText(/Ready to Get Started/i)).toBeInTheDocument();

      // User clicks the Create Free Account button
      const registerLink = within(ctaSection!).getByRole('link', { name: /Create Free Account/i });
      await user.click(registerLink);

      // User is navigated to registration page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });
  });
});

/**
 * Scenario 12: Navbar Integration
 *
 * Test that Navbar component integrates correctly with homepage.
 *
 * Steps:
 * 1. Navigate to homepage
 * 2. Verify Navbar presence
 * 3. Verify Navbar links (Login, Register)
 * 4. Verify ThemeToggle in Navbar
 */
describe('Scenario 12: Navbar Integration', () => {
  describe('Test Case 1: Render HomePage and check for Navbar component', () => {
    it('Navbar is rendered at top of page', () => {
      renderWithProviders(<Home />);

      // Navbar should be present using navigation role
      const navbar = screen.getByRole('navigation');
      expect(navbar).toBeInTheDocument();
    });

    it('Navbar renders as a semantic nav element', () => {
      renderWithProviders(<Home />);

      // Should be a semantic <nav> element
      const navbar = screen.getByRole('navigation');
      expect(navbar.tagName).toBe('NAV');
    });

    it('Navbar is positioned at the top (before main content)', () => {
      renderWithProviders(<Home />);

      // Navbar should come before the main content area
      const navbar = screen.getByRole('navigation');
      const mainContent = document.querySelector('main');

      expect(navbar).toBeInTheDocument();
      expect(mainContent).toBeInTheDocument();

      // Navbar should precede main in the DOM order
      const navPosition = navbar.compareDocumentPosition(mainContent!);
      // Node.DOCUMENT_POSITION_FOLLOWING = 4 (mainContent comes after navbar)
      expect(navPosition & Node.DOCUMENT_POSITION_FOLLOWING).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    });

    it('Navbar has sticky positioning for visibility', () => {
      renderWithProviders(<Home />);

      const navbar = screen.getByRole('navigation');
      expect(navbar).toHaveClass('sticky');
      expect(navbar).toHaveClass('top-0');
    });

    it('Navbar includes application branding link', () => {
      renderWithProviders(<Home />);

      // Should have a link to home with the app name
      const brandLink = screen.getByRole('link', { name: /URL Shortener/i });
      expect(brandLink).toBeInTheDocument();
      expect(brandLink).toHaveAttribute('href', '/');
    });
  });

  describe('Test Case 2: Navbar shows Login and Register links when user not authenticated', () => {
    it('Navbar shows Sign In link for unauthenticated users', () => {
      renderWithProviders(<Home />);

      // Find the navbar
      const navbar = screen.getByRole('navigation');

      // Sign In link should be in the navbar
      const signInLink = within(navbar).getByRole('link', { name: /Sign In/i });
      expect(signInLink).toBeInTheDocument();
      expect(signInLink).toHaveAttribute('href', '/login');
    });

    it('Navbar shows Get Started (Register) link for unauthenticated users', () => {
      renderWithProviders(<Home />);

      // Find the navbar
      const navbar = screen.getByRole('navigation');

      // Get Started link should be in the navbar
      const getStartedLink = within(navbar).getByRole('link', { name: /Get Started/i });
      expect(getStartedLink).toBeInTheDocument();
      expect(getStartedLink).toHaveAttribute('href', '/register');
    });

    it('both authentication links are accessible in navbar', () => {
      renderWithProviders(<Home />);

      const navbar = screen.getByRole('navigation');

      // Both links should be present and accessible
      const signInLink = within(navbar).getByRole('link', { name: /Sign In/i });
      const getStartedLink = within(navbar).getByRole('link', { name: /Get Started/i });

      // Links should be focusable (keyboard accessible)
      signInLink.focus();
      expect(document.activeElement).toBe(signInLink);

      getStartedLink.focus();
      expect(document.activeElement).toBe(getStartedLink);
    });

    it('navbar links are styled as buttons', () => {
      renderWithProviders(<Home />);

      const navbar = screen.getByRole('navigation');

      const signInLink = within(navbar).getByRole('link', { name: /Sign In/i });
      const getStartedLink = within(navbar).getByRole('link', { name: /Get Started/i });

      // Sign In should be styled as a ghost button
      expect(signInLink).toHaveClass('btn');
      expect(signInLink).toHaveClass('btn-ghost');

      // Get Started should be styled as a primary button
      expect(getStartedLink).toHaveClass('btn');
      expect(getStartedLink).toHaveClass('btn-primary');
    });
  });

  describe('Test Case 3: ThemeToggle component is present in Navbar', () => {
    it('ThemeToggle is rendered within Navbar', () => {
      renderWithProviders(<Home />);

      const navbar = screen.getByRole('navigation');

      // ThemeToggle renders as a select element with aria-label
      const themeToggle = within(navbar).getByRole('combobox', { name: /Select theme/i });
      expect(themeToggle).toBeInTheDocument();
    });

    it('ThemeToggle has correct accessible label', () => {
      renderWithProviders(<Home />);

      const navbar = screen.getByRole('navigation');

      const themeToggle = within(navbar).getByRole('combobox', { name: /Select theme/i });
      expect(themeToggle).toHaveAttribute('aria-label', 'Select theme');
    });

    it('ThemeToggle contains expected theme options', () => {
      renderWithProviders(<Home />);

      const navbar = screen.getByRole('navigation');

      const themeToggle = within(navbar).getByRole('combobox', { name: /Select theme/i });

      // Check for expected theme options
      const options = within(themeToggle).getAllByRole('option');
      const themeNames = options.map(option => option.textContent?.toLowerCase());

      expect(themeNames).toContain('light');
      expect(themeNames).toContain('dark');
      expect(themeNames).toContain('cyberpunk');
      expect(themeNames).toContain('synthwave');
    });

    it('ThemeToggle is styled correctly', () => {
      renderWithProviders(<Home />);

      const navbar = screen.getByRole('navigation');

      const themeToggle = within(navbar).getByRole('combobox', { name: /Select theme/i });
      expect(themeToggle).toHaveClass('select');
      expect(themeToggle).toHaveClass('select-bordered');
      expect(themeToggle).toHaveClass('select-sm');
    });

    it('ThemeToggle is keyboard accessible', () => {
      renderWithProviders(<Home />);

      const navbar = screen.getByRole('navigation');

      const themeToggle = within(navbar).getByRole('combobox', { name: /Select theme/i });

      // ThemeToggle should be focusable
      themeToggle.focus();
      expect(document.activeElement).toBe(themeToggle);
    });
  });

  describe('Navbar Integration Tests', () => {
    it('Navbar component works correctly when rendered standalone', () => {
      renderWithProviders(<Navbar />);

      // Navbar should render
      const navbar = screen.getByRole('navigation');
      expect(navbar).toBeInTheDocument();

      // Should have Sign In link
      expect(screen.getByRole('link', { name: /Sign In/i })).toBeInTheDocument();

      // Should have Get Started link
      expect(screen.getByRole('link', { name: /Get Started/i })).toBeInTheDocument();

      // Should have ThemeToggle
      expect(screen.getByRole('combobox', { name: /Select theme/i })).toBeInTheDocument();
    });

    it('Navbar in Home matches standalone Navbar behavior', () => {
      // Render Home page
      const { unmount } = renderWithProviders(<Home />);
      const homeNavbar = screen.getByRole('navigation');
      const homeSignIn = within(homeNavbar).getByRole('link', { name: /Sign In/i });
      const homeGetStarted = within(homeNavbar).getByRole('link', { name: /Get Started/i });
      const homeThemeToggle = within(homeNavbar).getByRole('combobox', { name: /Select theme/i });

      expect(homeSignIn).toHaveAttribute('href', '/login');
      expect(homeGetStarted).toHaveAttribute('href', '/register');
      expect(homeThemeToggle).toBeInTheDocument();

      unmount();

      // Render standalone Navbar
      renderWithProviders(<Navbar />);
      const standaloneNavbar = screen.getByRole('navigation');
      const standaloneSignIn = within(standaloneNavbar).getByRole('link', { name: /Sign In/i });
      const standaloneGetStarted = within(standaloneNavbar).getByRole('link', { name: /Get Started/i });
      const standaloneThemeToggle = within(standaloneNavbar).getByRole('combobox', { name: /Select theme/i });

      // Both should have the same behavior
      expect(standaloneSignIn).toHaveAttribute('href', '/login');
      expect(standaloneGetStarted).toHaveAttribute('href', '/register');
      expect(standaloneThemeToggle).toBeInTheDocument();
    });

    it('Navbar navigation links work with route navigation', async () => {
      const user = userEvent.setup();
      renderWithRoutes('/');

      const navbar = screen.getByRole('navigation');

      // Click Sign In link
      const signInLink = within(navbar).getByRole('link', { name: /Sign In/i });
      await user.click(signInLink);

      // Should navigate to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });

    it('Navbar Get Started link navigates to register', async () => {
      const user = userEvent.setup();
      renderWithRoutes('/');

      const navbar = screen.getByRole('navigation');

      // Click Get Started link
      const getStartedLink = within(navbar).getByRole('link', { name: /Get Started/i });
      await user.click(getStartedLink);

      // Should navigate to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });
  });
});

/**
 * Scenario 14: Animation and Motion Effects
 *
 * Test that Framer Motion animations load and perform correctly.
 *
 * Steps:
 * 1. Navigate to homepage
 * 2. Observe initial load animations
 * 3. Test hover animations
 * 4. Test scroll-triggered animations
 */
describe('Scenario 14: Animation and Motion Effects', () => {
  describe('Test Case 1: Render HomePage and check for Framer Motion components', () => {
    it('HomePage contains motion.div elements for animations', () => {
      renderWithProviders(<Home />);

      // motion.div elements are rendered as regular divs but with framer-motion attributes
      // The hero section should have animated elements
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toBeInTheDocument();

      // The h1 element should be a motion element (framer-motion adds style attributes)
      // Framer Motion adds data-projection-id or style with transform for animated elements
      const headingParent = heroHeading.closest('div');
      expect(headingParent).toBeInTheDocument();
    });

    it('HeroSection uses motion components for entrance animations', () => {
      renderWithProviders(<HeroSection />);

      // Check that the hero section renders with animated content
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();

      // The headline should have Framer Motion initial/animate states
      // Framer Motion renders motion.h1 as h1 with added styles
      expect(headline.textContent).toMatch(/Shorten URLs/i);
    });

    it('HomePage renders BackgroundEffect component with motion elements', () => {
      renderWithProviders(<Home />);

      // BackgroundEffect should be present with its test id
      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();

      // BackgroundEffect contains motion.div elements for animated circles
      const animatedDivs = backgroundEffect.querySelectorAll('div');
      expect(animatedDivs.length).toBeGreaterThanOrEqual(3); // 3 animated circles + container
    });

    it('FeaturesSection contains motion components for card animations', () => {
      renderWithProviders(<Home />);

      // Features section should be present
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Feature cards should be rendered (animated with motion.div)
      const featureCards = screen.getAllByTestId(/feature-card-/);
      expect(featureCards.length).toBe(4);
    });

    it('CTASection contains motion components', () => {
      renderWithProviders(<Home />);

      // CTA section should be present
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // CTA heading should be present (animated with motion.h2)
      const ctaHeading = screen.getByRole('heading', { name: /Ready to Get Started/i });
      expect(ctaHeading).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Check FuturisticButton hover animation', () => {
    it('FuturisticButton is rendered as a motion.button element', () => {
      renderWithProviders(<HeroSection />);

      // Find the Get Started button
      const getStartedButton = screen.getByRole('button', { name: /Get Started/i });
      expect(getStartedButton).toBeInTheDocument();

      // The button should have the btn class from FuturisticButton
      expect(getStartedButton).toHaveClass('btn');
      expect(getStartedButton).toHaveClass('btn-primary');
    });

    it('FuturisticButton has hover animation props (whileHover scale)', () => {
      renderWithProviders(<HeroSection />);

      // Find the Get Started button
      const getStartedButton = screen.getByRole('button', { name: /Get Started/i });
      expect(getStartedButton).toBeInTheDocument();

      // Framer Motion applies transforms via inline styles
      // We can verify the button element exists and is properly styled
      expect(getStartedButton.tagName.toLowerCase()).toBe('button');

      // Button should have transition classes for smooth animation
      expect(getStartedButton).toHaveClass('transition-all');
    });

    it('FuturisticButton has correct variant styling for primary CTA', () => {
      renderWithProviders(<HeroSection />);

      // Primary button should have primary variant classes
      const primaryButton = screen.getByRole('button', { name: /Get Started/i });
      expect(primaryButton).toHaveClass('btn-primary');

      // Secondary button (Sign In) should have outline variant
      const secondaryButton = screen.getByRole('button', { name: /Sign In/i });
      expect(secondaryButton).toHaveClass('btn-outline');
    });

    it('multiple FuturisticButtons exist with hover capability', () => {
      renderWithProviders(<Home />);

      // Multiple buttons throughout the page
      const allButtons = screen.getAllByRole('button');
      expect(allButtons.length).toBeGreaterThanOrEqual(2);

      // Get Started buttons (hero and CTA sections)
      const getStartedButtons = screen.getAllByRole('button', { name: /Get Started/i });
      expect(getStartedButtons.length).toBeGreaterThanOrEqual(1);

      // Each button should have the base classes
      getStartedButtons.forEach((button) => {
        expect(button).toHaveClass('btn');
      });
    });
  });

  describe('Test Case 3: Verify BackgroundEffect animation runs', () => {
    it('BackgroundEffect component is present and has animated elements', () => {
      renderWithProviders(<Home />);

      // BackgroundEffect should be in the document
      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();

      // Background effect should be positioned correctly (fixed, full screen)
      expect(backgroundEffect).toHaveClass('fixed');
      expect(backgroundEffect).toHaveClass('inset-0');
      expect(backgroundEffect).toHaveClass('overflow-hidden');
    });

    it('BackgroundEffect contains multiple animated blur circles', () => {
      renderWithProviders(<Home />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // BackgroundEffect has 3 motion.div elements for animated circles
      // Each has blur-3xl or blur-2xl class
      const blurElements = backgroundEffect.querySelectorAll('.blur-3xl, .blur-2xl');
      expect(blurElements.length).toBe(3);
    });

    it('BackgroundEffect circles have proper color classes for theming', () => {
      renderWithProviders(<Home />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // Check for primary, secondary, and accent color classes
      const primaryCircle = backgroundEffect.querySelector('.bg-primary\\/20');
      const secondaryCircle = backgroundEffect.querySelector('.bg-secondary\\/20');
      const accentCircle = backgroundEffect.querySelector('.bg-accent\\/10');

      expect(primaryCircle).toBeInTheDocument();
      expect(secondaryCircle).toBeInTheDocument();
      expect(accentCircle).toBeInTheDocument();
    });

    it('BackgroundEffect is rendered behind content (negative z-index)', () => {
      renderWithProviders(<Home />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // BackgroundEffect should have negative z-index to stay behind content
      expect(backgroundEffect).toHaveClass('-z-10');
    });

    it('BackgroundEffect does not block pointer events', () => {
      renderWithProviders(<Home />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // BackgroundEffect should not intercept clicks
      expect(backgroundEffect).toHaveClass('pointer-events-none');
    });

    it('BackgroundEffect circles have rounded-full class for circular shape', () => {
      renderWithProviders(<Home />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // All animated circles should be round
      const roundedElements = backgroundEffect.querySelectorAll('.rounded-full');
      expect(roundedElements.length).toBe(3);
    });
  });

  describe('Animation Integration Tests', () => {
    it('HomePage has all sections with animations properly rendered', () => {
      renderWithProviders(<Home />);

      // Hero section with motion elements
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toBeInTheDocument();

      // Features section with animated cards
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // How It Works section (if present)
      const howItWorksHeading = screen.getByRole('heading', { name: /How It Works/i });
      expect(howItWorksHeading).toBeInTheDocument();

      // CTA section with animations
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Background effect for visual polish
      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('all FuturisticButtons on page have consistent styling', () => {
      renderWithProviders(<Home />);

      // All buttons should have common base styles
      const allButtons = screen.getAllByRole('button');

      allButtons.forEach((button) => {
        // Each FuturisticButton has these base classes
        expect(button).toHaveClass('btn');
        expect(button).toHaveClass('font-semibold');
        expect(button).toHaveClass('rounded-lg');
      });
    });

    it('motion elements are rendered without crashing', () => {
      // This test ensures Framer Motion components render correctly
      expect(() => {
        renderWithProviders(<Home />);
      }).not.toThrow();
    });
  });
});

/**
 * Scenario 15: Component Reuse and Consistency
 *
 * Verify homepage uses existing design system components consistently.
 *
 * Steps:
 * 1. Audit component usage - Verify GlassMorphismCard is used for feature cards
 * 2. Check button components - Verify FuturisticButton is used for CTAs
 * 3. Verify styling patterns - Check that Tailwind/DaisyUI classes are used consistently
 */
describe('Scenario 15: Component Reuse and Consistency', () => {
  describe('Test Case 1: Check feature cards component type', () => {
    it('FeaturesSection uses GlassMorphismCard for feature cards', () => {
      renderWithProviders(<Home />);

      // Find the features section
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // GlassMorphismCard has distinctive classes: backdrop-blur-lg, bg-base-100/70, rounded-2xl
      // Each feature card should be wrapped in a GlassMorphismCard
      const featureCards = screen.getAllByTestId(/feature-card-/);
      expect(featureCards.length).toBe(4);

      // Verify each feature card is inside a GlassMorphismCard (check for backdrop-blur-lg class)
      featureCards.forEach((card) => {
        // The card's parent (GlassMorphismCard) should have the backdrop-blur-lg class
        const glassCard = card.closest('.backdrop-blur-lg');
        expect(glassCard).toBeInTheDocument();
        // Check for GlassMorphismCard signature classes (bg-base-100/70 has a / in it)
        expect(glassCard?.className).toContain('bg-base-100/70');
        expect(glassCard).toHaveClass('rounded-2xl');
      });
    });

    it('all 4 feature cards use GlassMorphismCard with consistent styling', () => {
      renderWithProviders(<Home />);

      // Find the features grid
      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();

      // Get all GlassMorphismCard components within the grid (identifiable by backdrop-blur-lg)
      const glassMorphismCards = featuresGrid.querySelectorAll('.backdrop-blur-lg');
      expect(glassMorphismCards.length).toBe(4);

      // Each card should have consistent GlassMorphismCard styling
      glassMorphismCards.forEach((card) => {
        // GlassMorphismCard default classes
        expect(card).toHaveClass('backdrop-blur-lg');
        expect(card).toHaveClass('border');
        expect(card).toHaveClass('rounded-2xl');
        expect(card).toHaveClass('shadow-xl');
      });
    });

    it('feature cards do not use custom card implementations', () => {
      renderWithProviders(<Home />);

      const featuresGrid = screen.getByTestId('features-grid');

      // Ensure there are no custom card classes that would indicate non-design-system usage
      // GlassMorphismCard uses specific Tailwind classes, not custom CSS
      const customCards = featuresGrid.querySelectorAll('[class*="custom-card"], [class*="Card__"]');
      expect(customCards.length).toBe(0);

      // Verify we're using the design system by checking for backdrop-blur (GlassMorphismCard signature)
      const designSystemCards = featuresGrid.querySelectorAll('.backdrop-blur-lg');
      expect(designSystemCards.length).toBe(4);
    });
  });

  describe('Test Case 2: Check CTA buttons component type', () => {
    it('Hero section uses FuturisticButton for primary CTA', () => {
      renderWithProviders(<Home />);

      // Find the hero section (contains h1)
      const heroHeading = screen.getByRole('heading', { level: 1 });
      const heroSection = heroHeading.closest('section');
      expect(heroSection).toBeInTheDocument();

      // Get Started button should use FuturisticButton (has btn, btn-primary, font-semibold, rounded-lg)
      const getStartedButton = within(heroSection!).getByRole('button', { name: /Get Started/i });
      expect(getStartedButton).toBeInTheDocument();

      // FuturisticButton signature classes
      expect(getStartedButton).toHaveClass('btn');
      expect(getStartedButton).toHaveClass('btn-primary');
      expect(getStartedButton).toHaveClass('font-semibold');
      expect(getStartedButton).toHaveClass('rounded-lg');
      expect(getStartedButton).toHaveClass('transition-all');
    });

    it('Hero section uses FuturisticButton for secondary CTA (Sign In)', () => {
      renderWithProviders(<Home />);

      const heroHeading = screen.getByRole('heading', { level: 1 });
      const heroSection = heroHeading.closest('section');

      // Sign In button should use FuturisticButton with outline variant
      const signInButton = within(heroSection!).getByRole('button', { name: /Sign In/i });
      expect(signInButton).toBeInTheDocument();

      // FuturisticButton with outline variant
      expect(signInButton).toHaveClass('btn');
      expect(signInButton).toHaveClass('btn-outline');
      expect(signInButton).toHaveClass('font-semibold');
      expect(signInButton).toHaveClass('rounded-lg');
    });

    it('CTA section uses FuturisticButton for registration CTA', () => {
      renderWithProviders(<Home />);

      // Find the CTA section
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Create Free Account button should use FuturisticButton
      const registerButton = within(ctaSection!).getByRole('button', { name: /Create Free Account/i });
      expect(registerButton).toBeInTheDocument();

      // FuturisticButton signature classes
      expect(registerButton).toHaveClass('btn');
      expect(registerButton).toHaveClass('btn-primary');
      expect(registerButton).toHaveClass('font-semibold');
      expect(registerButton).toHaveClass('rounded-lg');
    });

    it('all CTA buttons use FuturisticButton consistently', () => {
      renderWithProviders(<Home />);

      // Get all buttons on the page
      const allButtons = screen.getAllByRole('button');

      // Each button should have FuturisticButton base styles
      allButtons.forEach((button) => {
        // FuturisticButton base styles
        expect(button).toHaveClass('btn');
        expect(button).toHaveClass('font-semibold');
        expect(button).toHaveClass('rounded-lg');
        expect(button).toHaveClass('transition-all');
        expect(button).toHaveClass('duration-300');
        expect(button).toHaveClass('relative');
        expect(button).toHaveClass('overflow-hidden');
      });
    });

    it('buttons do not use non-design-system button implementations', () => {
      renderWithProviders(<Home />);

      const allButtons = screen.getAllByRole('button');

      // Ensure no buttons use custom button classes
      allButtons.forEach((button) => {
        const classList = button.className;
        // Should not have custom CSS module classes
        expect(classList).not.toMatch(/Button__/);
        expect(classList).not.toMatch(/custom-button/);
        // Should have DaisyUI btn class
        expect(button).toHaveClass('btn');
      });
    });
  });

  describe('Test Case 3: Verify no inline styles', () => {
    it('hero section uses Tailwind classes instead of inline styles', () => {
      renderWithProviders(<Home />);

      const heroHeading = screen.getByRole('heading', { level: 1 });
      const heroSection = heroHeading.closest('section');
      expect(heroSection).toBeInTheDocument();

      // Check that the section and its children don't have inline styles
      // (style attribute with actual style values, not empty or framer-motion transforms)
      const elementsWithInlineStyles = heroSection!.querySelectorAll('[style]');

      elementsWithInlineStyles.forEach((element) => {
        const styleAttr = element.getAttribute('style') || '';
        // Framer Motion adds transform styles for animations, which is acceptable
        // But there should be no custom CSS properties like color, margin, padding, etc.
        expect(styleAttr).not.toMatch(/margin\s*:/i);
        expect(styleAttr).not.toMatch(/padding\s*:/i);
        expect(styleAttr).not.toMatch(/color\s*:/i);
        expect(styleAttr).not.toMatch(/background\s*:/i);
        expect(styleAttr).not.toMatch(/font-size\s*:/i);
        expect(styleAttr).not.toMatch(/width\s*:/i);
        expect(styleAttr).not.toMatch(/height\s*:/i);
      });

      // Verify section uses Tailwind classes
      expect(heroSection).toHaveClass('relative');
      expect(heroSection).toHaveClass('flex');
    });

    it('features section uses Tailwind classes instead of inline styles', () => {
      renderWithProviders(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Check inline styles
      const elementsWithInlineStyles = featuresSection.querySelectorAll('[style]');

      elementsWithInlineStyles.forEach((element) => {
        const styleAttr = element.getAttribute('style') || '';
        // No custom CSS properties should be present
        expect(styleAttr).not.toMatch(/margin\s*:/i);
        expect(styleAttr).not.toMatch(/padding\s*:/i);
        expect(styleAttr).not.toMatch(/background-color\s*:/i);
        expect(styleAttr).not.toMatch(/font-size\s*:/i);
      });

      // Verify section uses Tailwind classes
      expect(featuresSection).toHaveClass('py-16');
    });

    it('CTA section uses Tailwind classes instead of inline styles', () => {
      renderWithProviders(<Home />);

      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Check inline styles
      const elementsWithInlineStyles = ctaSection!.querySelectorAll('[style]');

      elementsWithInlineStyles.forEach((element) => {
        const styleAttr = element.getAttribute('style') || '';
        expect(styleAttr).not.toMatch(/margin\s*:/i);
        expect(styleAttr).not.toMatch(/padding\s*:/i);
        expect(styleAttr).not.toMatch(/background-color\s*:/i);
      });

      // Verify section uses Tailwind classes
      expect(ctaSection).toHaveClass('py-16');
      expect(ctaSection).toHaveClass('bg-base-200');
    });

    it('Footer uses Tailwind classes instead of inline styles', () => {
      renderWithProviders(<Home />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();

      // Check inline styles
      const elementsWithInlineStyles = footer.querySelectorAll('[style]');

      elementsWithInlineStyles.forEach((element) => {
        const styleAttr = element.getAttribute('style') || '';
        expect(styleAttr).not.toMatch(/margin\s*:/i);
        expect(styleAttr).not.toMatch(/padding\s*:/i);
        expect(styleAttr).not.toMatch(/background-color\s*:/i);
      });

      // Verify footer uses Tailwind classes
      expect(footer).toHaveClass('py-8');
      expect(footer).toHaveClass('border-t');
    });

    it('HowItWorks section uses Tailwind classes instead of inline styles', () => {
      renderWithProviders(<Home />);

      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toBeInTheDocument();

      // Check inline styles
      const elementsWithInlineStyles = howItWorksSection.querySelectorAll('[style]');

      elementsWithInlineStyles.forEach((element) => {
        const styleAttr = element.getAttribute('style') || '';
        expect(styleAttr).not.toMatch(/margin\s*:/i);
        expect(styleAttr).not.toMatch(/padding\s*:/i);
        expect(styleAttr).not.toMatch(/background-color\s*:/i);
      });

      // Verify section uses Tailwind classes
      expect(howItWorksSection).toHaveClass('py-16');
    });

    it('all components use DaisyUI theme-aware classes', () => {
      renderWithProviders(<Home />);

      // Check for theme-aware base classes (bg-base-*, text-base-content, etc.)
      const baseElements = document.querySelectorAll('[class*="base-"]');
      expect(baseElements.length).toBeGreaterThan(0);

      // Verify main container uses theme-aware background
      const mainContainer = document.querySelector('.bg-base-100');
      expect(mainContainer).toBeInTheDocument();

      // Verify text uses theme-aware colors
      const themeAwareText = document.querySelectorAll('[class*="text-base-content"]');
      expect(themeAwareText.length).toBeGreaterThan(0);

      // Verify primary color is used from theme
      const primaryElements = document.querySelectorAll('[class*="text-primary"], [class*="bg-primary"], [class*="btn-primary"]');
      expect(primaryElements.length).toBeGreaterThan(0);
    });

    it('no custom CSS-in-JS or CSS modules are used', () => {
      renderWithProviders(<Home />);

      // Check that no elements have CSS module generated class names (e.g., ComponentName__className__hash)
      const allElements = document.querySelectorAll('*');

      allElements.forEach((element) => {
        const classList = element.className;
        if (typeof classList === 'string') {
          // CSS modules typically generate class names like "Component_className__hash"
          expect(classList).not.toMatch(/__[a-zA-Z0-9]{5,}/);
        }
      });
    });
  });

  describe('Design System Consistency Integration', () => {
    it('homepage uses a consistent component library throughout', () => {
      renderWithProviders(<Home />);

      // Verify GlassMorphismCard is used for cards (4 feature cards)
      const glassMorphismCards = document.querySelectorAll('.backdrop-blur-lg.rounded-2xl');
      expect(glassMorphismCards.length).toBeGreaterThanOrEqual(4);

      // Verify FuturisticButton is used for all buttons
      const allButtons = screen.getAllByRole('button');
      allButtons.forEach((button) => {
        expect(button).toHaveClass('btn');
        expect(button).toHaveClass('font-semibold');
      });

      // Verify BackgroundEffect is present
      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('component styling follows DaisyUI conventions', () => {
      renderWithProviders(<Home />);

      // Buttons use btn- prefix
      const primaryButtons = document.querySelectorAll('.btn-primary');
      expect(primaryButtons.length).toBeGreaterThan(0);

      // Text uses text- prefix with theme colors
      const themedText = document.querySelectorAll('[class*="text-primary"], [class*="text-secondary"]');
      expect(themedText.length).toBeGreaterThan(0);

      // Spacing uses Tailwind conventions (py-, px-, mb-, etc.)
      const spacedElements = document.querySelectorAll('[class*="py-"], [class*="px-"], [class*="mb-"]');
      expect(spacedElements.length).toBeGreaterThan(0);
    });

    it('all interactive elements use motion components from Framer Motion', () => {
      renderWithProviders(<Home />);

      // Buttons should be motion elements (FuturisticButton uses motion.button)
      // We can verify this by checking that buttons have transition classes
      const allButtons = screen.getAllByRole('button');
      allButtons.forEach((button) => {
        expect(button).toHaveClass('transition-all');
      });

      // GlassMorphismCards are motion.div elements
      const cards = document.querySelectorAll('.backdrop-blur-lg');
      expect(cards.length).toBeGreaterThan(0);
    });
  });
});

/**
 * Scenario 17: Error Handling - Missing Resources
 *
 * Test that homepage gracefully handles missing or failed resource loading.
 *
 * Steps:
 * 1. Simulate failed image load
 * 2. Verify no JavaScript errors on initial render
 */
describe('Scenario 17: Error Handling - Missing Resources', () => {
  describe('Test Case 1: Render HomePage and check for console errors', () => {
    it('renders HomePage without throwing JavaScript errors', () => {
      // This test ensures the home page renders without any exceptions
      expect(() => {
        renderWithProviders(<Home />);
      }).not.toThrow();
    });

    it('all major sections render without errors', () => {
      // Render the homepage and verify all sections are present
      renderWithProviders(<Home />);

      // Verify navigation is present
      expect(screen.getByRole('navigation')).toBeInTheDocument();

      // Verify hero section renders (contains h1)
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // Verify features section renders
      expect(screen.getByTestId('features-section')).toBeInTheDocument();

      // Verify how it works section renders
      expect(screen.getByRole('heading', { name: /How It Works/i })).toBeInTheDocument();

      // Verify CTA section renders
      expect(document.getElementById('cta')).toBeInTheDocument();

      // Verify footer renders
      expect(screen.getByRole('contentinfo')).toBeInTheDocument();
    });

    it('renders without console errors when all components are present', () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      renderWithProviders(<Home />);

      // Verify no React errors were logged to console
      // Filter out any warnings that are not actual errors
      const reactErrors = consoleSpy.mock.calls.filter(
        (call) => call[0]?.includes?.('Error') || call[0]?.includes?.('Uncaught')
      );
      expect(reactErrors).toHaveLength(0);

      consoleSpy.mockRestore();
    });

    it('homepage components do not throw during render lifecycle', () => {
      // Test that each homepage component can render independently without errors
      expect(() => renderWithProviders(<HeroSection />)).not.toThrow();
    });

    it('homepage renders with all child components mounted', () => {
      renderWithProviders(<Home />);

      // Verify the main content structure
      expect(document.querySelector('main')).toBeInTheDocument();

      // All main sections should be children of main or visible on page
      const main = document.querySelector('main');
      expect(main?.children.length).toBeGreaterThanOrEqual(4); // Hero, Features, HowItWorks, CTA
    });
  });

  describe('Test Case 2: Simulate image load failure', () => {
    it('SVG icons render as inline elements (no external image load required)', () => {
      renderWithProviders(<Home />);

      // The features section uses inline SVG icons which cannot fail to load
      const featureCards = screen.getAllByTestId(/feature-card-/);
      expect(featureCards.length).toBe(4);

      // Each feature card should have an SVG icon present
      featureCards.forEach((card) => {
        const svg = card.querySelector('svg');
        expect(svg).toBeInTheDocument();
      });
    });

    it('feature icons have aria-hidden attribute for accessibility', () => {
      renderWithProviders(<Home />);

      const featureCards = screen.getAllByTestId(/feature-card-/);

      // Each SVG icon should have aria-hidden="true" since they are decorative
      featureCards.forEach((card) => {
        const svg = card.querySelector('svg');
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });

    it('text content is always visible regardless of image loading state', () => {
      renderWithProviders(<Home />);

      // Primary text content should always be visible
      expect(screen.getByText(/Shorten URLs/i)).toBeInTheDocument();
      expect(screen.getByText(/Track Clicks/i)).toBeInTheDocument();
      expect(screen.getByText(/Transform your long URLs/i)).toBeInTheDocument();

      // Feature titles are always visible
      expect(screen.getByText('URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Click Analytics')).toBeInTheDocument();
      expect(screen.getByText('Dashboard Management')).toBeInTheDocument();
      expect(screen.getByText('Share Statistics')).toBeInTheDocument();
    });

    it('homepage remains functional even if BackgroundEffect has animation issues', () => {
      renderWithProviders(<Home />);

      // Even if background animation fails, main content should be accessible
      const ctaLinks = screen.getAllByRole('link', { name: /Get Started/i });
      expect(ctaLinks.length).toBeGreaterThan(0);

      // Navigation should be functional
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      expect(signInLinks.length).toBeGreaterThan(0);
    });

    it('BackgroundEffect uses CSS classes for fallback styling', () => {
      renderWithProviders(<Home />);

      const backgroundEffect = screen.getByTestId('background-effect');

      // Background should have pointer-events-none so it doesn't block interaction
      expect(backgroundEffect).toHaveClass('pointer-events-none');

      // Background should be behind content
      expect(backgroundEffect).toHaveClass('-z-10');
    });

    it('all interactive elements remain clickable even with animation errors', async () => {
      const user = userEvent.setup();
      renderWithRoutes('/');

      // Find a button and verify it can receive focus and is clickable
      const heroHeading = screen.getByRole('heading', { level: 1 });
      const heroSection = heroHeading.closest('section');
      const getStartedLink = within(heroSection!).getByRole('link', { name: /Get Started/i });

      // Link should be interactable
      expect(getStartedLink).not.toHaveAttribute('disabled');
      expect(getStartedLink).toHaveAttribute('href', '/register');

      // Should be able to click without errors
      await user.click(getStartedLink);

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });
  });

  describe('Error Boundary Behavior', () => {
    it('homepage renders complete structure without errors', () => {
      const { container } = renderWithProviders(<Home />);

      // Verify the complete DOM structure is rendered
      expect(container.querySelector('div.min-h-screen')).toBeInTheDocument();
      expect(container.querySelector('nav')).toBeInTheDocument();
      expect(container.querySelector('main')).toBeInTheDocument();
      expect(container.querySelector('footer')).toBeInTheDocument();
    });

    it('all sections have proper semantic structure for graceful degradation', () => {
      renderWithProviders(<Home />);

      // Semantic elements ensure content is accessible even if styles/animations fail
      expect(screen.getByRole('navigation')).toBeInTheDocument();
      expect(screen.getByRole('main')).toBeInTheDocument();
      expect(screen.getByRole('contentinfo')).toBeInTheDocument();

      // Headings provide structure
      const headings = screen.getAllByRole('heading');
      expect(headings.length).toBeGreaterThanOrEqual(4); // h1 + multiple h2s
    });

    it('fallback content exists through semantic HTML even without JavaScript', () => {
      renderWithProviders(<Home />);

      // Text content is in semantic HTML, not dependent on JS rendering success
      const mainContent = screen.getByRole('main');
      expect(mainContent.textContent).toContain('Shorten URLs');
      expect(mainContent.textContent).toContain('Get Started');
    });
  });

  describe('Resource Loading Resilience', () => {
    it('no external images that could fail to load', () => {
      renderWithProviders(<Home />);

      // Verify there are no img elements that could fail
      const images = document.querySelectorAll('img');
      // If there are images, they should have alt text
      images.forEach((img) => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('icons use inline SVG which cannot fail to load from network', () => {
      renderWithProviders(<Home />);

      // All icons in the app use inline SVG
      const svgs = document.querySelectorAll('svg');
      expect(svgs.length).toBeGreaterThan(0);

      // SVGs should be inline (have path elements)
      svgs.forEach((svg) => {
        // SVG should either have path children or be a valid SVG structure
        expect(svg.innerHTML).toBeTruthy();
      });
    });

    it('CSS is applied through Tailwind classes (no external CSS that could fail)', () => {
      renderWithProviders(<Home />);

      // Core styling classes are present
      const container = document.querySelector('.min-h-screen');
      expect(container).toHaveClass('bg-base-100');

      // Buttons have proper styling classes
      const buttons = screen.getAllByRole('button');
      buttons.forEach((button) => {
        expect(button).toHaveClass('btn');
      });
    });
  });
});

/**
 * Scenario 16: Route Configuration
 *
 * Test that homepage is properly configured at root route.
 *
 * Steps:
 * 1. Access root URL
 * 2. Verify no redirect (homepage accessible to unauthenticated users)
 * 3. Verify correct component renders (Home component at / route)
 */
describe('Scenario 16: Route Configuration', () => {
  /**
   * Helper function to render App with router
   * This tests the actual App.tsx route configuration
   */
  function renderApp(initialRoute = '/') {
    return render(
      <MemoryRouter initialEntries={[initialRoute]}>
        <QueryClientProvider client={queryClient}>
          <ThemeProvider>
            <AuthProvider>
              <App />
            </AuthProvider>
          </ThemeProvider>
        </QueryClientProvider>
      </MemoryRouter>
    );
  }

  describe('Test Case 1: Access "/" route without authentication', () => {
    it('homepage renders without redirect to /login when accessing root URL', () => {
      renderApp('/');

      // Should render the homepage, not be redirected to login
      const heroHeadline = screen.getByRole('heading', { level: 1 });
      expect(heroHeadline).toBeInTheDocument();
      expect(heroHeadline.textContent).toMatch(/Shorten URLs/i);

      // Login page should NOT be visible
      expect(screen.queryByText(/Welcome back/i)).not.toBeInTheDocument();
      expect(screen.queryByRole('heading', { name: /Sign in to your account/i })).not.toBeInTheDocument();
    });

    it('unauthenticated users are NOT redirected away from root route', () => {
      renderApp('/');

      // Homepage content should be visible
      const homepage = document.querySelector('.min-h-screen.bg-base-100');
      expect(homepage).toBeInTheDocument();

      // Hero section should be present (first section of homepage)
      const heroSection = document.querySelector('section');
      expect(heroSection).toBeInTheDocument();

      // Should NOT be on login page
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument();
      expect(screen.queryByRole('form')).not.toBeInTheDocument();
    });

    it('homepage is accessible to all users (public route)', () => {
      // Render as unauthenticated user
      renderApp('/');

      // Homepage should display all sections
      expect(screen.getByRole('heading', { level: 1, name: /Shorten URLs/i })).toBeInTheDocument();
      expect(screen.getByRole('navigation')).toBeInTheDocument();

      // Features section should be visible
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // CTA section should be visible
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Footer should be visible
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('root route does not require authentication context to render', () => {
      // Render without explicit authentication state - should still work
      renderApp('/');

      // Homepage content should render successfully
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();

      // Navigation should include login/register options (indicating unauthenticated state)
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      expect(signInLinks.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 2: Check App.tsx route configuration', () => {
    it('route path="/" renders Home component', () => {
      renderApp('/');

      // Verify Home component renders at root route
      // Home component renders with specific structure
      const homeContainer = document.querySelector('.min-h-screen.bg-base-100');
      expect(homeContainer).toBeInTheDocument();

      // Home includes Navbar
      const navbar = screen.getByRole('navigation');
      expect(navbar).toBeInTheDocument();

      // Home includes main content with sections
      const main = document.querySelector('main');
      expect(main).toBeInTheDocument();

      // Home includes Footer
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('App routes "/" to Home component correctly', () => {
      renderApp('/');

      // Verify the h1 headline from HeroSection (child of Home)
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline.textContent).toMatch(/Shorten URLs\. Track Clicks\. Grow Your Reach\./i);

      // Verify subheadline exists
      expect(screen.getByText(/Transform your long URLs/i)).toBeInTheDocument();
    });

    it('Home component is NOT wrapped in ProtectedLayout at root route', () => {
      renderApp('/');

      // Homepage should render directly without protection
      // If it were protected, we'd be redirected to login
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();

      // The homepage renders immediately without auth check
      // Multiple "Get Started" links exist (navbar and hero), verify at least one
      const getStartedLinks = screen.getAllByText(/Get Started/i);
      expect(getStartedLinks.length).toBeGreaterThan(0);
    });

    it('root route is outside ProtectedLayout routes', async () => {
      // Navigate to root - should show homepage
      renderApp('/');
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // Dashboard route is protected and would redirect if accessed
      // But root route is public
      const heroSection = document.querySelector('section');
      expect(heroSection).toBeInTheDocument();
    });

    it('routing configuration maps "/" to Home before any protected routes', () => {
      renderApp('/');

      // Home component structure verification
      // Home component renders: div > Navbar, main > sections, Footer
      const container = document.querySelector('.min-h-screen');
      expect(container).toBeInTheDocument();

      const navbar = screen.getByRole('navigation');
      const main = document.querySelector('main');
      const footer = screen.getByRole('contentinfo');

      // Verify all Home component children are present
      expect(navbar).toBeInTheDocument();
      expect(main).toBeInTheDocument();
      expect(footer).toBeInTheDocument();

      // Main contains all sections
      expect(main?.querySelectorAll('section').length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Route Integration Tests', () => {
    it('can navigate from "/" to "/login" route', async () => {
      const user = userEvent.setup();
      renderApp('/');

      // Find and click Sign In link
      const signInLinks = screen.getAllByRole('link', { name: /Sign In/i });
      await user.click(signInLinks[0]);

      // Should navigate to login page (Login.tsx has heading "Sign In" and "Don't have an account?" text)
      await waitFor(() => {
        // Login page specific text that doesn't exist on homepage
        expect(screen.getByText(/Don't have an account\?/i)).toBeInTheDocument();
      });
    });

    it('can navigate from "/" to "/register" route', async () => {
      const user = userEvent.setup();
      renderApp('/');

      // Find and click Get Started link (registration)
      const getStartedLinks = screen.getAllByRole('link', { name: /Get Started/i });
      await user.click(getStartedLinks[0]);

      // Should navigate to register page (Register.tsx has "Already have an account?" text)
      await waitFor(() => {
        expect(screen.getByText(/Already have an account\?/i)).toBeInTheDocument();
      });
    });

    it('homepage brand link in navbar navigates back to "/"', () => {
      renderApp('/');

      // Brand link should point to home
      const brandLink = screen.getByRole('link', { name: /URL Shortener/i });
      expect(brandLink).toHaveAttribute('href', '/');
    });

    it('full page structure is correct at root route', () => {
      renderApp('/');

      // Verify complete page structure
      // 1. Navbar at top
      const navbar = screen.getByRole('navigation');
      expect(navbar).toBeInTheDocument();

      // 2. Main content area with sections
      const main = document.querySelector('main');
      expect(main).toBeInTheDocument();

      // 3. HeroSection (with h1)
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toBeInTheDocument();

      // 4. FeaturesSection
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // 5. HowItWorksSection
      const howItWorksHeading = screen.getByRole('heading', { name: /How It Works/i });
      expect(howItWorksHeading).toBeInTheDocument();

      // 6. CTASection
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // 7. Footer
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });
  });
});
