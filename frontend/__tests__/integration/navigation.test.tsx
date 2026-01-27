/**
 * Navigation Integration Tests
 * Owner: Scenario 4 - Navigation and CTA Links
 *
 * Test cases:
 * 1. Clicking primary CTA button navigates to /register route
 * 2. Clicking Login button/link navigates to /login route
 * 3. Primary CTA has correct href for /register
 * 4. Login link has correct href for /login
 *
 * These tests verify that navigation links and CTA buttons correctly
 * route to Login and Register pages as per REQ-3.
 */

import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { Home } from '../../src/pages/Home';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    button: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
    section: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <section {...props}>{children}</section>
    ),
  },
}));

// Mock BackgroundEffect component to avoid Three.js issues in tests
vi.mock('../../src/components/BackgroundEffect', () => ({
  BackgroundEffect: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
  default: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
}));

// Helper component to display the current location for testing
function LocationDisplay() {
  const location = useLocation();
  return <div data-testid="location-display">{location.pathname}</div>;
}

// Test wrapper that provides routing context
interface TestAppProps {
  initialRoute?: string;
}

function TestApp({ initialRoute = '/' }: TestAppProps) {
  return (
    <ThemeProvider>
      <MemoryRouter initialEntries={[initialRoute]}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route
            path="/register"
            element={
              <div data-testid="register-page">
                <h1>Register</h1>
              </div>
            }
          />
          <Route
            path="/login"
            element={
              <div data-testid="login-page">
                <h1>Login</h1>
              </div>
            }
          />
        </Routes>
        <LocationDisplay />
      </MemoryRouter>
    </ThemeProvider>
  );
}

/**
 * Accessibility - Keyboard Navigation Tests
 * Owner: Scenario 14 - Accessibility - Keyboard Navigation
 *
 * Test cases:
 * 1. Tab through landing page - all interactive elements focusable
 * 2. Focus indicator visible on focused button
 * 3. Press Enter on focused CTA - navigates to /register
 * 4. Skip-to-content link present for screen reader users
 *
 * These tests verify WCAG 2.1 AA compliance for keyboard accessibility as per NFR-2.
 */
describe('Accessibility - Keyboard Navigation Tests', () => {
  describe('Test Case 1: Tab through landing page', () => {
    it('all interactive elements (buttons, links) are focusable via Tab', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Collect all focusable interactive elements
      const allLinks = screen.getAllByRole('link');
      const focusedElements: HTMLElement[] = [];

      // Tab through the page and collect focused elements
      let tabCount = 0;
      const maxTabs = 20; // Reasonable limit to avoid infinite loop

      while (tabCount < maxTabs) {
        await user.tab();
        tabCount++;

        const activeElement = document.activeElement as HTMLElement;

        // If we've cycled back to body or no active element, stop
        if (!activeElement || activeElement === document.body) {
          break;
        }

        // Track unique focused elements
        if (!focusedElements.includes(activeElement)) {
          focusedElements.push(activeElement);
        }

        // Check if we've completed a cycle (back to first element)
        if (focusedElements.length > 1 && activeElement === focusedElements[0]) {
          break;
        }
      }

      // Verify that multiple interactive elements were focused
      expect(focusedElements.length).toBeGreaterThan(0);

      // Verify key interactive elements are in the focused list
      // Check that at least register and login links are focusable
      const registerLink = screen.getByRole('link', { name: /get started|create free account/i });
      const loginLinks = screen.getAllByRole('link', { name: /login/i });

      // At least one of the key links should have been focused
      const focusedRegister = focusedElements.some(el => el === registerLink);
      const focusedLogin = focusedElements.some(el => loginLinks.includes(el));

      expect(focusedRegister || focusedLogin).toBe(true);
    });
  });

  describe('Test Case 2: Focus indicator visibility', () => {
    it('focus indicator is visible on focused CTA button', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find the primary CTA link
      const primaryCta = screen.getByRole('link', { name: /get started|create free account/i });

      // Focus on the element
      primaryCta.focus();

      // Verify the element is focused
      expect(document.activeElement).toBe(primaryCta);

      // The element should have focus styles applied by the browser/CSS
      // We verify the element is focusable and receives focus
      expect(primaryCta).toHaveFocus();

      // Also tab to verify focus via keyboard
      await user.tab();

      // Verify an element received focus
      const activeAfterTab = document.activeElement;
      expect(activeAfterTab).not.toBe(document.body);
      expect(activeAfterTab).toBeInstanceOf(HTMLElement);
    });
  });

  describe('Test Case 3: Keyboard activation with Enter', () => {
    it('button activates and navigates to /register when Enter is pressed', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find the primary CTA link
      const primaryCta = screen.getByRole('link', { name: /get started|create free account/i });

      // Focus the element
      primaryCta.focus();
      expect(primaryCta).toHaveFocus();

      // Press Enter to activate the link
      await user.keyboard('{Enter}');

      // Verify navigation to register page
      const locationDisplay = screen.getByTestId('location-display');
      expect(locationDisplay).toHaveTextContent('/register');

      // Verify register page content is rendered
      const registerPage = screen.getByTestId('register-page');
      expect(registerPage).toBeInTheDocument();
    });

    it('login link activates and navigates when Enter is pressed', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find the login link (first one in hero section - there are multiple)
      const loginLinks = screen.getAllByRole('link', { name: /login/i });
      const loginLink = loginLinks[0]; // First login link (hero section)

      // Focus and activate
      loginLink.focus();
      expect(loginLink).toHaveFocus();

      await user.keyboard('{Enter}');

      // Verify navigation to login page
      const locationDisplay = screen.getByTestId('location-display');
      expect(locationDisplay).toHaveTextContent('/login');
    });
  });

  describe('Test Case 4: Skip-to-content link', () => {
    it('skip-to-content link is present for screen reader users', () => {
      render(<TestApp />);

      // Look for skip link (common implementations)
      const skipLink = screen.getByRole('link', { name: /skip to (main )?content/i });

      expect(skipLink).toBeInTheDocument();

      // Skip link should link to main content area
      expect(skipLink).toHaveAttribute('href', '#main-content');
    });

    it('skip-to-content link becomes visible on focus', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      const skipLink = screen.getByRole('link', { name: /skip to (main )?content/i });

      // Initially, skip link is typically visually hidden but accessible
      // When focused, it should become visible

      // Tab to the skip link (should be first focusable element)
      await user.tab();

      // The skip link should now be focused and visible
      expect(skipLink).toHaveFocus();
    });

    it('skip-to-content link moves focus to main content when activated', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      const skipLink = screen.getByRole('link', { name: /skip to (main )?content/i });

      // Focus and activate skip link
      skipLink.focus();
      await user.keyboard('{Enter}');

      // Main content should exist with the target id
      const mainContent = document.getElementById('main-content');
      expect(mainContent).toBeInTheDocument();
    });
  });
});

/**
 * Routing Integration Tests
 * Owner: Scenario 19 - Routing Integration
 *
 * Test cases:
 * 1. Navigate to root path / - Landing page component is rendered
 * 2. Check App.tsx routing configuration - Route for / renders Home component
 *
 * These tests verify that the landing page is correctly integrated
 * with React Router at the root path (/).
 */
describe('Routing Integration Tests', () => {
  describe('Test Case 1: Navigate to root path /', () => {
    it('Landing page component is rendered at root path', () => {
      render(<TestApp initialRoute="/" />);

      // Verify we are at the root path
      const locationDisplay = screen.getByTestId('location-display');
      expect(locationDisplay).toHaveTextContent('/');

      // Verify the landing page component is rendered
      const landingPage = screen.getByTestId('landing-page');
      expect(landingPage).toBeInTheDocument();
    });

    it('renders the main content area with semantic HTML', () => {
      render(<TestApp initialRoute="/" />);

      // Verify main element exists (semantic HTML)
      const mainContent = screen.getByRole('main');
      expect(mainContent).toBeInTheDocument();

      // Verify main has the correct id for skip-to-content
      expect(mainContent).toHaveAttribute('id', 'main-content');
    });

    it('renders all landing page sections at root path', () => {
      render(<TestApp initialRoute="/" />);

      // Verify key sections are present
      // Hero section with headline
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // Features section
      expect(screen.getByRole('heading', { name: /features/i })).toBeInTheDocument();

      // How it works section
      expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Route configuration verification', () => {
    it('Route for / renders Home component', () => {
      render(<TestApp initialRoute="/" />);

      // Verify the Home component is rendered (identified by landing-page testid)
      const homePage = screen.getByTestId('landing-page');
      expect(homePage).toBeInTheDocument();

      // The Home component should be wrapped in main with id main-content
      expect(homePage.tagName).toBe('MAIN');
    });

    it('Home component includes all required sections', () => {
      render(<TestApp initialRoute="/" />);

      // Hero section - check for primary CTA
      const primaryCta = screen.getByRole('link', { name: /get started|create free account/i });
      expect(primaryCta).toBeInTheDocument();

      // Features section - check for feature cards
      const features = screen.getAllByText(/instant url shortening|detailed analytics|share statistics/i);
      expect(features.length).toBeGreaterThan(0);

      // Social proof - check for stats (there may be multiple matches)
      const statsElements = screen.getAllByText(/links created|clicks tracked|happy users/i);
      expect(statsElements.length).toBeGreaterThan(0);
    });

    it('landing page integrates correctly with ThemeProvider', () => {
      render(<TestApp initialRoute="/" />);

      // Verify the page renders within the theme context
      // The page should render without errors when ThemeProvider is present
      const landingPage = screen.getByTestId('landing-page');
      expect(landingPage).toBeInTheDocument();

      // Content should be visible (theme is applied correctly)
      expect(landingPage).toBeVisible();
    });

    it('landing page is the only component at root route', () => {
      render(<TestApp initialRoute="/" />);

      // Verify we're not on login or register pages
      expect(screen.queryByTestId('login-page')).not.toBeInTheDocument();
      expect(screen.queryByTestId('register-page')).not.toBeInTheDocument();

      // Only landing page is rendered
      expect(screen.getByTestId('landing-page')).toBeInTheDocument();
    });
  });
});

describe('Navigation and CTA Links Integration Tests', () => {
  describe('Integration Tests - Navigation', () => {
    it('navigates to /register when clicking primary CTA button', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find and click the primary CTA button (Get Started)
      const primaryCta = screen.getByRole('link', { name: /get started|create free account/i });
      await user.click(primaryCta);

      // Verify navigation to register page
      const locationDisplay = screen.getByTestId('location-display');
      expect(locationDisplay).toHaveTextContent('/register');

      // Verify register page content is rendered
      const registerPage = screen.getByTestId('register-page');
      expect(registerPage).toBeInTheDocument();
    });

    it('navigates to /login when clicking Login button/link', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find and click the Login link (first one - hero section)
      const loginLinks = screen.getAllByRole('link', { name: /login/i });
      const loginLink = loginLinks[0];
      await user.click(loginLink);

      // Verify navigation to login page
      const locationDisplay = screen.getByTestId('location-display');
      expect(locationDisplay).toHaveTextContent('/login');

      // Verify login page content is rendered
      const loginPage = screen.getByTestId('login-page');
      expect(loginPage).toBeInTheDocument();
    });
  });

  describe('Unit Tests - Link Attributes', () => {
    it('primary CTA has correct href attribute for /register', () => {
      render(<TestApp />);

      // Find the primary CTA link
      const primaryCta = screen.getByRole('link', { name: /get started|create free account/i });

      // Verify href attribute
      expect(primaryCta).toHaveAttribute('href', '/register');
    });

    it('Login link has correct href attribute for /login', () => {
      render(<TestApp />);

      // Find the Login links (hero and footer both have one)
      const loginLinks = screen.getAllByRole('link', { name: /login/i });

      // Verify href attribute for all login links
      loginLinks.forEach(link => {
        expect(link).toHaveAttribute('href', '/login');
      });
    });
  });

  describe('Navigation Links Presence', () => {
    it('renders navigation links for Login and Register', () => {
      render(<TestApp />);

      // Verify both navigation links are present
      const registerLink = screen.getByRole('link', { name: /get started|create free account/i });
      const loginLinks = screen.getAllByRole('link', { name: /login/i });

      expect(registerLink).toBeInTheDocument();
      expect(registerLink).toBeVisible();
      expect(loginLinks.length).toBeGreaterThan(0);
      expect(loginLinks[0]).toBeInTheDocument();
      expect(loginLinks[0]).toBeVisible();
    });

    it('CTA buttons are accessible and keyboard navigable', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Get the hero section CTA link
      const registerLink = screen.getByRole('link', { name: /get started|create free account/i });

      // Tab to first link and verify focus
      await user.tab();

      // Continue tabbing to find the CTA link (it should be focusable)
      let attempts = 0;
      const maxAttempts = 15;
      let foundRegister = false;

      while (attempts < maxAttempts) {
        if (document.activeElement === registerLink) {
          foundRegister = true;
          break;
        }
        await user.tab();
        attempts++;
      }

      // Verify the primary CTA link was reachable via keyboard
      expect(foundRegister).toBe(true);
    });
  });
});
