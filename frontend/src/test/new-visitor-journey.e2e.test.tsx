/**
 * New Visitor User Journey E2E Tests
 *
 * Scenario: Verify complete new visitor flow from landing to registration
 *
 * Test Flow:
 * 1. Land on homepage (/)
 * 2. View hero and value proposition
 * 3. Scroll through features
 * 4. Click "Get Started Free" CTA
 * 5. Navigate to /register page
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import Home from '../pages/Home';
import Register from '../pages/Register';
import { ThemeProvider } from '../contexts/ThemeContext';

// Test app wrapper with all routes for E2E testing
function TestApp({ initialEntries = ['/'] }: { initialEntries?: string[] }) {
  return (
    <ThemeProvider>
      <MemoryRouter initialEntries={initialEntries}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/register" element={<Register />} />
        </Routes>
      </MemoryRouter>
    </ThemeProvider>
  );
}

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

describe('New Visitor User Journey E2E Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * Test Case 1: Complete new visitor journey E2E test
   * Input: Complete new visitor journey E2E test
   * Expected: User successfully navigates from homepage to registration
   */
  describe('Test Case 1: Complete new visitor journey E2E test', () => {
    it('completes full journey from homepage to registration', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Step 1: Verify landing on homepage (/)
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Step 2: View hero and value proposition
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toHaveTextContent(/shorten, share, track/i);

      const subheadline = screen.getByText(/create short, memorable links/i);
      expect(subheadline).toBeInTheDocument();

      // Step 3: Verify features section is accessible
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards.length).toBeGreaterThanOrEqual(3);

      // Step 4: Click Get Started CTA
      const getStartedButton = screen.getByRole('button', { name: /get started free/i });
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toBeEnabled();

      await user.click(getStartedButton);

      // Step 5: Verify navigation to /register is triggered
      await waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/register');
      });
    });

    it('user can view all homepage sections before navigating to register', async () => {
      render(<TestApp />);

      // Verify all major sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('demo-section')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument();
      expect(screen.getByTestId('social-proof-section')).toBeInTheDocument();
      expect(screen.getByTestId('homepage-footer')).toBeInTheDocument();
    });

    it('displays compelling value proposition above the fold', async () => {
      render(<TestApp />);

      // Hero section should be present
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Check headline
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toHaveTextContent('Shorten, Share, Track');

      // Check subheadline
      const subheadline = screen.getByText(/create short, memorable links in seconds/i);
      expect(subheadline).toBeInTheDocument();

      // Check CTA buttons are present and accessible
      const getStartedButton = screen.getByRole('button', { name: /get started free/i });
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i });
      expect(getStartedButton).toBeInTheDocument();
      expect(learnMoreButton).toBeInTheDocument();
      expect(getStartedButton).toBeEnabled();
      expect(learnMoreButton).toBeEnabled();
    });
  });

  /**
   * Test Case 2: Click 'Get Started Free' button integration test
   * Input: Click 'Get Started Free' button
   * Expected: User is navigated to /register without errors
   */
  describe('Test Case 2: Click Get Started Free button integration', () => {
    it('Get Started Free button navigates to /register without errors', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Find the Get Started Free button
      const getStartedButton = screen.getByRole('button', { name: /get started free/i });
      expect(getStartedButton).toBeInTheDocument();

      // Click the button
      await user.click(getStartedButton);

      // Verify navigation was called correctly
      await waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/register');
        expect(mockNavigate).toHaveBeenCalledTimes(1);
      });
    });

    it('Get Started Free button is clickable and responsive', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      const getStartedButton = screen.getByRole('button', { name: /get started free/i });

      // Button should be enabled
      expect(getStartedButton).toBeEnabled();

      // Button should not be disabled
      expect(getStartedButton).not.toBeDisabled();

      // Click should work without errors
      await expect(user.click(getStartedButton)).resolves.not.toThrow();
    });

    it('Sign Up link in navbar also navigates to /register', async () => {
      render(<TestApp />);

      // Find Sign Up link in navbar
      const signUpLink = screen.getByLabelText(/go to registration page/i);
      expect(signUpLink).toBeInTheDocument();
      expect(signUpLink).toHaveAttribute('href', '/register');
    });

    it('CTA button in CTA section navigates to /register', async () => {
      render(<TestApp />);

      // Find Sign Up Free link in the CTA section
      const ctaLink = screen.getByRole('link', { name: /sign up free/i });
      expect(ctaLink).toBeInTheDocument();
      expect(ctaLink).toHaveAttribute('href', '/register');
    });
  });

  /**
   * Test Case 3: Homepage to register journey performance test
   * Input: Homepage to register journey
   * Expected: Journey completes in under 5 seconds
   */
  describe('Test Case 3: Homepage to register journey performance', () => {
    it('journey completes in under 5 seconds', async () => {
      const user = userEvent.setup();
      const startTime = performance.now();

      render(<TestApp />);

      // Verify homepage is rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();

      // Click Get Started Free
      const getStartedButton = screen.getByRole('button', { name: /get started free/i });
      await user.click(getStartedButton);

      // Wait for navigation
      await waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/register');
      });

      const endTime = performance.now();
      const journeyDuration = endTime - startTime;

      // Journey should complete in under 5 seconds (5000ms)
      expect(journeyDuration).toBeLessThan(5000);
    });

    it('homepage renders quickly without blocking elements', async () => {
      const startTime = performance.now();

      render(<TestApp />);

      // Verify critical elements are rendered
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      const endTime = performance.now();
      const renderTime = endTime - startTime;

      // Initial render should be fast (under 2 seconds)
      expect(renderTime).toBeLessThan(2000);
    });

    it('all user journey steps are accessible immediately', async () => {
      render(<TestApp />);

      // All key elements should be present immediately after render
      // Step 1: Homepage
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();

      // Step 2: Value proposition
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

      // Step 3: Features section
      expect(screen.getByTestId('features-section')).toBeInTheDocument();

      // Step 4: CTA button
      expect(screen.getByRole('button', { name: /get started free/i })).toBeInTheDocument();
    });
  });

  /**
   * Additional journey tests for comprehensive coverage
   */
  describe('Additional journey coverage', () => {
    it('visitor can see features section content', async () => {
      render(<TestApp />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // Check feature cards
      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard');
      const geoCard = screen.getByTestId('feature-card-geographic-insights');
      const shareCard = screen.getByTestId('feature-card-share-collaborate');

      expect(urlShorteningCard).toBeInTheDocument();
      expect(analyticsCard).toBeInTheDocument();
      expect(geoCard).toBeInTheDocument();
      expect(shareCard).toBeInTheDocument();
    });

    it('visitor can understand the service from How It Works section', async () => {
      render(<TestApp />);

      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection).toBeInTheDocument();

      // Check all three steps
      expect(screen.getByTestId('how-it-works-step-1')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-step-2')).toBeInTheDocument();
      expect(screen.getByTestId('how-it-works-step-3')).toBeInTheDocument();

      // Verify step titles
      expect(screen.getByTestId('how-it-works-step-1-title')).toHaveTextContent(/paste your long url/i);
      expect(screen.getByTestId('how-it-works-step-2-title')).toHaveTextContent(/get your short link/i);
      expect(screen.getByTestId('how-it-works-step-3-title')).toHaveTextContent(/track performance/i);
    });

    it('visitor sees social proof before deciding to register', async () => {
      render(<TestApp />);

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection).toBeInTheDocument();

      // Check statistics
      const statisticsSection = screen.getByTestId('statistics-section');
      expect(statisticsSection).toBeInTheDocument();

      // Verify at least one statistic is displayed
      const urlsStat = screen.getByTestId('statistic-urls-shortened');
      expect(urlsStat).toBeInTheDocument();

      // Check testimonials
      const testimonialsSection = screen.getByTestId('testimonials-section');
      expect(testimonialsSection).toBeInTheDocument();
    });

    it('Learn More button scrolls to features section', async () => {
      const user = userEvent.setup();
      const scrollIntoViewMock = vi.fn();
      Element.prototype.scrollIntoView = scrollIntoViewMock;

      render(<TestApp />);

      const learnMoreButton = screen.getByRole('button', { name: /learn more/i });
      await user.click(learnMoreButton);

      await waitFor(() => {
        expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
      });
    });

    it('multiple paths lead to registration page', async () => {
      render(<TestApp />);

      // Path 1: Get Started Free in hero
      const getStartedButton = screen.getByRole('button', { name: /get started free/i });
      expect(getStartedButton).toBeInTheDocument();

      // Path 2: Sign Up in navbar
      const navSignUp = screen.getByLabelText(/go to registration page/i);
      expect(navSignUp).toHaveAttribute('href', '/register');

      // Path 3: Sign Up Free in CTA section
      const ctaSignUp = screen.getByRole('link', { name: /sign up free/i });
      expect(ctaSignUp).toHaveAttribute('href', '/register');
    });

    it('visitor journey is accessible via keyboard navigation', async () => {
      const user = userEvent.setup();
      render(<TestApp />);

      // Tab through the page
      await user.tab();

      // Verify skip link exists
      const skipLink = screen.getByTestId('skip-link');
      expect(skipLink).toBeInTheDocument();

      // Verify main content can be focused
      const mainContent = document.getElementById('main-content');
      expect(mainContent).toBeInTheDocument();
    });
  });

  describe('Register page arrival verification', () => {
    it('register page displays correct heading', async () => {
      render(<TestApp initialEntries={['/register']} />);

      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toHaveTextContent(/create account/i);
    });

    it('register page has registration form', async () => {
      render(<TestApp initialEntries={['/register']} />);

      // Check form fields
      expect(screen.getByPlaceholderText(/john doe/i)).toBeInTheDocument();
      expect(screen.getByPlaceholderText(/your@email.com/i)).toBeInTheDocument();
      expect(screen.getByPlaceholderText(/create a password/i)).toBeInTheDocument();
    });

    it('register page has Sign Up button', async () => {
      render(<TestApp initialEntries={['/register']} />);

      const signUpButton = screen.getByRole('button', { name: /sign up/i });
      expect(signUpButton).toBeInTheDocument();
    });
  });
});
