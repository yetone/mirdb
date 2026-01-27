/**
 * CTASection Component Tests
 * Owner: Scenario 5 - Call-to-Action and Navigation
 *
 * Tests the final CTA section with reinforcing call-to-action
 * and proper navigation links to registration and login pages.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { CTASection } from '../../src/components/homepage/CTASection';

// Mock framer-motion to avoid IntersectionObserver issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    button: ({
      children,
      ...props
    }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
      <button {...props}>{children}</button>
    ),
  },
  HTMLMotionProps: {},
}));

function renderWithRouter(
  component: React.ReactElement,
  { route = '/' } = {}
) {
  return {
    ...render(
      <MemoryRouter initialEntries={[route]}>
        <Routes>
          <Route path="/" element={component} />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
          <Route
            path="/login"
            element={<div data-testid="login-page">Login Page</div>}
          />
        </Routes>
      </MemoryRouter>
    ),
  };
}

describe('CTASection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Section Container', () => {
    // Test Case 1: CTA section exists near bottom of page
    it('renders the CTA section container', () => {
      renderWithRouter(<CTASection />);

      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection).toBeInTheDocument();
    });

    it('has proper section landmark', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      expect(section.tagName.toLowerCase()).toBe('section');
    });
  });

  describe('CTA Headline', () => {
    // Test Case 2: Headline with encouraging text
    it('displays an encouraging CTA headline', () => {
      renderWithRouter(<CTASection />);

      const headline = screen.getByRole('heading', { level: 2 });
      expect(headline).toBeInTheDocument();
      // Should contain encouraging text
      expect(headline.textContent).toBeTruthy();
    });

    it('headline contains encouraging call-to-action text', () => {
      renderWithRouter(<CTASection />);

      const headline = screen.getByRole('heading', { level: 2 });
      const text = headline.textContent?.toLowerCase() || '';
      // Check for common CTA phrases
      const hasEncouragingText =
        text.includes('ready') ||
        text.includes('start') ||
        text.includes('join') ||
        text.includes('try') ||
        text.includes('get started');
      expect(hasEncouragingText).toBe(true);
    });
  });

  describe('Primary CTA Button', () => {
    // Test Case 3: Get Started or Sign Up button exists
    it('renders a primary CTA button', () => {
      renderWithRouter(<CTASection />);

      const ctaButton = screen.getByTestId('cta-get-started-button');
      expect(ctaButton).toBeInTheDocument();
    });

    it('primary button contains Get Started or Sign Up text', () => {
      renderWithRouter(<CTASection />);

      const ctaButton = screen.getByTestId('cta-get-started-button');
      const text = ctaButton.textContent?.toLowerCase() || '';
      const hasCtaText =
        text.includes('get started') ||
        text.includes('sign up') ||
        text.includes('start');
      expect(hasCtaText).toBe(true);
    });

    // Test Case 4: Navigation to /register route
    it('navigates to /register when primary CTA button is clicked', async () => {
      renderWithRouter(<CTASection />);

      const ctaButton = screen.getByTestId('cta-get-started-button');
      fireEvent.click(ctaButton);

      expect(await screen.findByTestId('register-page')).toBeInTheDocument();
    });

    it('calls onGetStarted callback when provided', () => {
      const onGetStarted = vi.fn();
      renderWithRouter(<CTASection onGetStarted={onGetStarted} />);

      const ctaButton = screen.getByTestId('cta-get-started-button');
      fireEvent.click(ctaButton);

      expect(onGetStarted).toHaveBeenCalledTimes(1);
    });
  });

  describe('Sign In Link', () => {
    // Test Case 5: Sign In link exists as secondary option
    it('renders a Sign In link', () => {
      renderWithRouter(<CTASection />);

      const signInLink = screen.getByTestId('cta-sign-in-link');
      expect(signInLink).toBeInTheDocument();
    });

    it('Sign In link contains appropriate text', () => {
      renderWithRouter(<CTASection />);

      const signInLink = screen.getByTestId('cta-sign-in-link');
      const text = signInLink.textContent?.toLowerCase() || '';
      const hasSignInText =
        text.includes('sign in') ||
        text.includes('login') ||
        text.includes('log in');
      expect(hasSignInText).toBe(true);
    });

    // Test Case 6: Navigation to /login route
    it('navigates to /login when Sign In link is clicked', async () => {
      renderWithRouter(<CTASection />);

      const signInLink = screen.getByTestId('cta-sign-in-link');
      fireEvent.click(signInLink);

      expect(await screen.findByTestId('login-page')).toBeInTheDocument();
    });

    it('calls onSignIn callback when provided', () => {
      const onSignIn = vi.fn();
      renderWithRouter(<CTASection onSignIn={onSignIn} />);

      const signInLink = screen.getByTestId('cta-sign-in-link');
      fireEvent.click(signInLink);

      expect(onSignIn).toHaveBeenCalledTimes(1);
    });
  });

  describe('FuturisticButton Styling', () => {
    // Test Case 7: Primary button uses FuturisticButton or consistent styling
    it('primary button has consistent button styling', () => {
      renderWithRouter(<CTASection />);

      const ctaButton = screen.getByTestId('cta-get-started-button');
      // Check for btn class that FuturisticButton uses
      expect(ctaButton.className).toMatch(/btn/);
    });

    it('primary button has primary variant styling', () => {
      renderWithRouter(<CTASection />);

      const ctaButton = screen.getByTestId('cta-get-started-button');
      // Check for btn-primary class that indicates primary variant
      expect(ctaButton.className).toMatch(/btn-primary/);
    });
  });

  describe('Accessibility', () => {
    it('section has accessible name or description', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      // Should have aria-labelledby or aria-label
      const hasLabel =
        section.hasAttribute('aria-labelledby') ||
        section.hasAttribute('aria-label');
      expect(hasLabel).toBe(true);
    });

    it('buttons are focusable', () => {
      renderWithRouter(<CTASection />);

      const ctaButton = screen.getByTestId('cta-get-started-button');
      const signInLink = screen.getByTestId('cta-sign-in-link');

      expect(ctaButton).not.toHaveAttribute('tabindex', '-1');
      expect(signInLink).not.toHaveAttribute('tabindex', '-1');
    });
  });

  describe('Supporting Text', () => {
    it('renders supporting text for users who already have an account', () => {
      renderWithRouter(<CTASection />);

      // Should have text like "Already have an account?" or similar
      const text = screen.getByTestId('cta-section').textContent?.toLowerCase() || '';
      const hasSupportingText =
        text.includes('already') ||
        text.includes('account') ||
        text.includes('member');
      expect(hasSupportingText).toBe(true);
    });
  });
});
