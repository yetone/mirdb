/**
 * Navigation Integration Tests
 * Owner: Scenario 3 - Navigation and Routing
 *
 * Tests for navigation links and routing functionality across the application.
 * Validates that all navigation elements work correctly and route to appropriate pages.
 */

import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import App from '../../src/App';

// Test wrapper with router
const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <App />
    </MemoryRouter>
  );
};

describe('Navigation and Routing', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Test Case 1: Homepage is accessible at route '/' and Navbar is present
   */
  describe('Test Case 1: Homepage rendering with Navbar', () => {
    it('should render homepage at root path with Navbar present', () => {
      renderWithRouter(['/']);

      // Verify homepage is rendered
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      // Verify Navbar is present
      expect(screen.getByTestId('navbar')).toBeInTheDocument();

      // Verify brand/logo is present
      expect(screen.getByTestId('brand-logo')).toBeInTheDocument();
      expect(screen.getByTestId('brand-logo')).toHaveTextContent('LinkSnip');
    });

    it('should display navigation links in Navbar', () => {
      renderWithRouter(['/']);

      // Verify Login link is present
      expect(screen.getByTestId('login-link')).toBeInTheDocument();
      expect(screen.getByTestId('login-link')).toHaveTextContent('Login');

      // Verify Register link is present
      expect(screen.getByTestId('register-link')).toBeInTheDocument();
      expect(screen.getByTestId('register-link')).toHaveTextContent('Register');
    });
  });

  /**
   * Test Case 2: Click Login link in Navbar navigates to /login
   */
  describe('Test Case 2: Login navigation', () => {
    it('should navigate to /login when clicking Login link in Navbar', async () => {
      renderWithRouter(['/']);

      // Click Login link
      const loginLink = screen.getByTestId('login-link');
      fireEvent.click(loginLink);

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });

      // Verify Login page content renders correctly
      expect(screen.getByTestId('login-title')).toBeInTheDocument();
      expect(screen.getByTestId('login-title')).toHaveTextContent('Login');
    });

    it('should render Login page form elements after navigation', async () => {
      renderWithRouter(['/']);

      fireEvent.click(screen.getByTestId('login-link'));

      await waitFor(() => {
        expect(screen.getByTestId('email-input')).toBeInTheDocument();
        expect(screen.getByTestId('password-input')).toBeInTheDocument();
        expect(screen.getByTestId('login-button')).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 3: Click Register link in Navbar navigates to /register
   */
  describe('Test Case 3: Register navigation', () => {
    it('should navigate to /register when clicking Register link in Navbar', async () => {
      renderWithRouter(['/']);

      // Click Register link
      const registerLink = screen.getByTestId('register-link');
      fireEvent.click(registerLink);

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });

      // Verify Register page content renders correctly
      expect(screen.getByTestId('register-title')).toBeInTheDocument();
      expect(screen.getByTestId('register-title')).toHaveTextContent('Create Account');
    });

    it('should render Register page form elements after navigation', async () => {
      renderWithRouter(['/']);

      fireEvent.click(screen.getByTestId('register-link'));

      await waitFor(() => {
        expect(screen.getByTestId('name-input')).toBeInTheDocument();
        expect(screen.getByTestId('email-input')).toBeInTheDocument();
        expect(screen.getByTestId('password-input')).toBeInTheDocument();
        expect(screen.getByTestId('confirm-password-input')).toBeInTheDocument();
        expect(screen.getByTestId('register-button')).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 4: Click 'Get Started Free' button navigates to /register
   */
  describe('Test Case 4: Get Started Free CTA navigation', () => {
    it('should navigate to /register when clicking Get Started Free button in HeroSection', async () => {
      renderWithRouter(['/']);

      // Verify hero section is present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();

      // Click Get Started Free button
      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveTextContent('Get Started Free');
      fireEvent.click(getStartedButton);

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 5: Click 'Learn More' button scrolls to features section
   */
  describe('Test Case 5: Learn More CTA scroll behavior', () => {
    it('should call scrollIntoView when clicking Learn More button', async () => {
      // Mock scrollIntoView
      const scrollIntoViewMock = vi.fn();
      Element.prototype.scrollIntoView = scrollIntoViewMock;

      renderWithRouter(['/']);

      // Verify features section exists
      expect(screen.getByTestId('features-section')).toBeInTheDocument();

      // Click Learn More button
      const learnMoreButton = screen.getByTestId('learn-more-button');
      expect(learnMoreButton).toBeInTheDocument();
      expect(learnMoreButton).toHaveTextContent('Learn More');
      fireEvent.click(learnMoreButton);

      // Verify scrollIntoView was called (smooth scroll to features section)
      await waitFor(() => {
        expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
      });
    });

    it('should have Learn More button in hero section', () => {
      renderWithRouter(['/']);

      const heroSection = screen.getByTestId('hero-section');
      const learnMoreButton = screen.getByTestId('learn-more-button');

      expect(heroSection).toContainElement(learnMoreButton);
    });
  });

  /**
   * Test Case 6: Click Privacy Policy link navigates to privacy page
   */
  describe('Test Case 6: Privacy Policy navigation', () => {
    it('should navigate to /privacy when clicking Privacy Policy link in Footer', async () => {
      renderWithRouter(['/']);

      // Verify footer is present
      expect(screen.getByTestId('footer')).toBeInTheDocument();

      // Click Privacy Policy link
      const privacyLink = screen.getByTestId('privacy-policy-link');
      expect(privacyLink).toBeInTheDocument();
      expect(privacyLink).toHaveTextContent('Privacy Policy');
      fireEvent.click(privacyLink);

      // Verify navigation to privacy policy page
      await waitFor(() => {
        expect(screen.getByTestId('privacy-policy-page')).toBeInTheDocument();
      });

      // Verify Privacy Policy page content
      expect(screen.getByTestId('privacy-title')).toBeInTheDocument();
      expect(screen.getByTestId('privacy-title')).toHaveTextContent('Privacy Policy');
    });

    it('should be able to navigate back to home from Privacy Policy page', async () => {
      renderWithRouter(['/privacy']);

      // Verify we're on the privacy page
      expect(screen.getByTestId('privacy-policy-page')).toBeInTheDocument();

      // Click back to home button
      const backButton = screen.getByTestId('back-to-home');
      fireEvent.click(backButton);

      // Verify navigation back to home
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });
    });
  });

  /**
   * Test Case 7: Click Terms of Service link navigates to terms page
   */
  describe('Test Case 7: Terms of Service navigation', () => {
    it('should navigate to /terms when clicking Terms of Service link in Footer', async () => {
      renderWithRouter(['/']);

      // Verify footer is present
      expect(screen.getByTestId('footer')).toBeInTheDocument();

      // Click Terms of Service link
      const termsLink = screen.getByTestId('terms-of-service-link');
      expect(termsLink).toBeInTheDocument();
      expect(termsLink).toHaveTextContent('Terms of Service');
      fireEvent.click(termsLink);

      // Verify navigation to terms of service page
      await waitFor(() => {
        expect(screen.getByTestId('terms-of-service-page')).toBeInTheDocument();
      });

      // Verify Terms of Service page content
      expect(screen.getByTestId('terms-title')).toBeInTheDocument();
      expect(screen.getByTestId('terms-title')).toHaveTextContent('Terms of Service');
    });

    it('should be able to navigate back to home from Terms of Service page', async () => {
      renderWithRouter(['/terms']);

      // Verify we're on the terms page
      expect(screen.getByTestId('terms-of-service-page')).toBeInTheDocument();

      // Click back to home button
      const backButton = screen.getByTestId('back-to-home');
      fireEvent.click(backButton);

      // Verify navigation back to home
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });
    });
  });

  /**
   * Additional navigation tests for comprehensive coverage
   */
  describe('Additional navigation scenarios', () => {
    it('should navigate from homepage to login and back to homepage', async () => {
      renderWithRouter(['/']);

      // Start at homepage
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      // Navigate to login
      fireEvent.click(screen.getByTestId('login-link'));
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });

      // Navigate back to homepage via brand logo
      fireEvent.click(screen.getByTestId('brand-logo'));
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });
    });

    it('should navigate from login to register via link', async () => {
      renderWithRouter(['/login']);

      // Verify we're on login page
      expect(screen.getByTestId('login-page')).toBeInTheDocument();

      // Click register link in login page
      const registerLink = screen.getByTestId('page-register-link');
      fireEvent.click(registerLink);

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument();
      });
    });

    it('should navigate from register to login via link', async () => {
      renderWithRouter(['/register']);

      // Verify we're on register page
      expect(screen.getByTestId('register-page')).toBeInTheDocument();

      // Click login link in register page
      const loginLink = screen.getByTestId('page-login-link');
      fireEvent.click(loginLink);

      // Verify navigation to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });
    });

    it('should maintain Navbar presence across all pages', async () => {
      // Test homepage
      renderWithRouter(['/']);
      expect(screen.getByTestId('navbar')).toBeInTheDocument();
    });

    it('should have correct hero section elements', () => {
      renderWithRouter(['/']);

      // Verify hero section structure
      expect(screen.getByTestId('hero-title')).toBeInTheDocument();
      expect(screen.getByTestId('hero-tagline')).toBeInTheDocument();
      expect(screen.getByTestId('get-started-button')).toBeInTheDocument();
      expect(screen.getByTestId('learn-more-button')).toBeInTheDocument();
    });

    it('should have correct footer links', () => {
      renderWithRouter(['/']);

      // Verify footer structure
      expect(screen.getByTestId('footer')).toBeInTheDocument();
      expect(screen.getByTestId('privacy-policy-link')).toBeInTheDocument();
      expect(screen.getByTestId('terms-of-service-link')).toBeInTheDocument();
      expect(screen.getByTestId('copyright')).toBeInTheDocument();
    });
  });
});
