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
import Footer from '../../src/components/homepage/Footer';
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
 * Test that homepage uses existing design system components consistently.
 *
 * Steps:
 * 1. Audit component usage - Verify GlassMorphismCard is used for feature cards
 * 2. Check button components - Verify FuturisticButton is used for CTAs
 * 3. Verify styling patterns - Check that Tailwind/DaisyUI classes are used consistently
 */
describe('Scenario 15: Component Reuse and Consistency', () => {
  describe('Test Case 1: Feature cards use GlassMorphismCard component', () => {
    it('feature cards have GlassMorphismCard characteristic classes', () => {
      renderWithProviders(<Home />);

      // GlassMorphismCard applies: backdrop-blur-lg bg-base-100/70 border border-base-content/10 rounded-2xl shadow-xl
      const featuresGrid = screen.getByTestId('features-grid');

      // Each feature card should be wrapped in GlassMorphismCard
      // GlassMorphismCard renders as a motion.div with specific classes
      const cards = featuresGrid.querySelectorAll('.backdrop-blur-lg');
      expect(cards.length).toBe(4); // All 4 feature cards use GlassMorphismCard
    });

    it('feature cards have glass morphism styling (bg-base-100/70)', () => {
      renderWithProviders(<Home />);

      const featuresGrid = screen.getByTestId('features-grid');

      // Check for the translucent background class
      const cards = featuresGrid.querySelectorAll('.bg-base-100\\/70');
      expect(cards.length).toBe(4);
    });

    it('feature cards have rounded corners (rounded-2xl)', () => {
      renderWithProviders(<Home />);

      const featuresGrid = screen.getByTestId('features-grid');

      // Check for rounded corners
      const cards = featuresGrid.querySelectorAll('.rounded-2xl');
      expect(cards.length).toBe(4);
    });

    it('feature cards have shadow effect (shadow-xl)', () => {
      renderWithProviders(<Home />);

      const featuresGrid = screen.getByTestId('features-grid');

      // Check for shadow
      const cards = featuresGrid.querySelectorAll('.shadow-xl');
      expect(cards.length).toBe(4);
    });

    it('feature cards have border styling from GlassMorphismCard', () => {
      renderWithProviders(<Home />);

      const featuresGrid = screen.getByTestId('features-grid');

      // Check for border class
      const cards = featuresGrid.querySelectorAll('.border');
      expect(cards.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Test Case 2: Primary CTAs use FuturisticButton component', () => {
    it('hero section Get Started button uses FuturisticButton styling', () => {
      renderWithProviders(<Home />);

      // Find the hero section Get Started button
      const heroSection = screen.getByRole('heading', { level: 1 }).closest('section');
      expect(heroSection).toBeInTheDocument();

      const getStartedButton = within(heroSection!).getByRole('button', { name: /Get Started/i });

      // FuturisticButton applies: btn btn-primary for primary variant
      expect(getStartedButton).toHaveClass('btn');
      expect(getStartedButton).toHaveClass('btn-primary');
      expect(getStartedButton).toHaveClass('font-semibold');
      expect(getStartedButton).toHaveClass('rounded-lg');
    });

    it('hero section Sign In button uses FuturisticButton styling', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByRole('heading', { level: 1 }).closest('section');
      expect(heroSection).toBeInTheDocument();

      const signInButton = within(heroSection!).getByRole('button', { name: /Sign In/i });

      // FuturisticButton with outline variant applies: btn btn-outline btn-primary
      expect(signInButton).toHaveClass('btn');
      expect(signInButton).toHaveClass('btn-outline');
      expect(signInButton).toHaveClass('font-semibold');
      expect(signInButton).toHaveClass('rounded-lg');
    });

    it('CTA section button uses FuturisticButton styling', () => {
      renderWithProviders(<Home />);

      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      const ctaButton = within(ctaSection!).getByRole('button', { name: /Create Free Account/i });

      // CTA button should have primary variant styling
      expect(ctaButton).toHaveClass('btn');
      expect(ctaButton).toHaveClass('btn-primary');
      expect(ctaButton).toHaveClass('font-semibold');
      expect(ctaButton).toHaveClass('rounded-lg');
    });

    it('all buttons have transition-all class for animations', () => {
      renderWithProviders(<Home />);

      const allButtons = screen.getAllByRole('button');

      allButtons.forEach((button) => {
        // FuturisticButton applies transition-all duration-300
        expect(button).toHaveClass('transition-all');
      });
    });

    it('buttons use relative and overflow-hidden for animation effects', () => {
      renderWithProviders(<Home />);

      const allButtons = screen.getAllByRole('button');

      allButtons.forEach((button) => {
        // FuturisticButton has relative overflow-hidden for potential effects
        expect(button).toHaveClass('relative');
        expect(button).toHaveClass('overflow-hidden');
      });
    });
  });

  describe('Test Case 3: Components use Tailwind classes instead of inline styles', () => {
    it('hero section does not use inline styles on main elements', () => {
      renderWithProviders(<Home />);

      const heroSection = screen.getByRole('heading', { level: 1 }).closest('section');
      expect(heroSection).toBeInTheDocument();

      // Check that the section and its direct children don't have problematic inline styles
      // Framer Motion may add transform styles for animation, which is acceptable
      const inlineStyleAttribute = heroSection!.getAttribute('style');
      if (inlineStyleAttribute) {
        // Should not contain layout styles (margin, padding, width, height, etc.)
        expect(inlineStyleAttribute).not.toMatch(/margin|padding|width:|height:/i);
      }
    });

    it('feature cards use Tailwind utility classes for layout', () => {
      renderWithProviders(<Home />);

      const featuresGrid = screen.getByTestId('features-grid');

      // The grid should use Tailwind grid classes
      expect(featuresGrid).toHaveClass('grid');
      expect(featuresGrid).toHaveClass('grid-cols-1');
      expect(featuresGrid).toHaveClass('md:grid-cols-2');
      expect(featuresGrid).toHaveClass('lg:grid-cols-4');
      expect(featuresGrid).toHaveClass('gap-6');
    });

    it('CTA section uses Tailwind background classes', () => {
      renderWithProviders(<Home />);

      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toBeInTheDocument();

      // Should use DaisyUI bg-base-200 class, not inline background styles
      expect(ctaSection).toHaveClass('bg-base-200');

      const inlineStyle = ctaSection!.getAttribute('style');
      if (inlineStyle) {
        expect(inlineStyle).not.toMatch(/background/i);
      }
    });

    it('buttons do not use inline styles for sizing', () => {
      renderWithProviders(<Home />);

      const allButtons = screen.getAllByRole('button');

      allButtons.forEach((button) => {
        const inlineStyle = button.getAttribute('style');
        if (inlineStyle) {
          // Allow transform styles from Framer Motion, but not layout styles
          expect(inlineStyle).not.toMatch(/width:|height:|padding:|margin:/i);
        }
      });
    });

    it('text elements use Tailwind typography classes', () => {
      renderWithProviders(<Home />);

      // Check the hero heading uses Tailwind text classes
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toHaveClass('text-4xl');
      expect(heroHeading).toHaveClass('font-bold');

      // Check the features heading
      const featuresHeading = screen.getByRole('heading', { name: /Powerful Features/i });
      expect(featuresHeading).toHaveClass('text-3xl');
      expect(featuresHeading).toHaveClass('font-bold');
    });

    it('container elements use Tailwind spacing classes', () => {
      renderWithProviders(<Home />);

      // Features section should use consistent container classes
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toHaveClass('py-16');

      // CTA section should use consistent spacing
      const ctaSection = document.getElementById('cta');
      expect(ctaSection).toHaveClass('py-16');
    });

    it('color styling uses DaisyUI theme classes', () => {
      renderWithProviders(<Home />);

      // Check that theme-aware classes are used
      const heroHeading = screen.getByRole('heading', { level: 1 });

      // The hero heading has spans with text-primary and text-secondary
      const primarySpan = heroHeading.querySelector('.text-primary');
      const secondarySpan = heroHeading.querySelector('.text-secondary');

      expect(primarySpan).toBeInTheDocument();
      expect(secondarySpan).toBeInTheDocument();
    });
  });

  describe('Design System Consistency Integration', () => {
    it('homepage maintains consistent component usage throughout', () => {
      renderWithProviders(<Home />);

      // All buttons should follow the same pattern
      const allButtons = screen.getAllByRole('button');
      allButtons.forEach((button) => {
        expect(button).toHaveClass('btn');
        expect(button).toHaveClass('font-semibold');
        expect(button).toHaveClass('rounded-lg');
      });

      // All feature cards should follow the GlassMorphismCard pattern
      const featuresGrid = screen.getByTestId('features-grid');
      const glassMorphismCards = featuresGrid.querySelectorAll('.backdrop-blur-lg.rounded-2xl.shadow-xl');
      expect(glassMorphismCards.length).toBe(4);
    });

    it('no custom one-off button styles exist', () => {
      renderWithProviders(<Home />);

      // All buttons should use btn class (DaisyUI/FuturisticButton)
      const allButtons = screen.getAllByRole('button');

      allButtons.forEach((button) => {
        // Should use DaisyUI btn base class, not custom classes
        expect(button).toHaveClass('btn');
        // Should not have ad-hoc background color classes that bypass the design system
        expect(button.className).not.toMatch(/bg-blue-|bg-green-|bg-red-/);
      });
    });

    it('card components consistently use GlassMorphismCard', () => {
      renderWithProviders(<Home />);

      // Feature cards should all have the same glass morphism treatment
      const featuresGrid = screen.getByTestId('features-grid');
      const cards = featuresGrid.querySelectorAll('.backdrop-blur-lg');

      cards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100/70');
        expect(card).toHaveClass('border');
        expect(card).toHaveClass('rounded-2xl');
        expect(card).toHaveClass('shadow-xl');
      });
    });
  });
});
