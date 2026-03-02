/**
 * HeroSection Component Unit Tests.
 *
 * Tests for Scenario 1 - Hero Section Display and Content:
 * - Headline text validation
 * - Subheadline text validation
 * - Primary CTA button ("Sign Up Free")
 * - Secondary CTA button ("Try as Guest")
 * - Minimum height requirement (600px)
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { HeroSection } from '../../../../src/components/home/HeroSection';

// Helper to render with Router context
function renderWithRouter(ui: React.ReactElement) {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
}

describe('HeroSection', () => {
  describe('Test Case 2: Headline Text', () => {
    it('renders with correct headline text "Shorten Links. Track Clicks. Grow Your Impact."', () => {
      renderWithRouter(<HeroSection />);

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent(
        'Shorten Links. Track Clicks. Grow Your Impact.'
      );
    });

    it('headline has proper ARIA attributes', () => {
      renderWithRouter(<HeroSection />);

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toHaveAttribute('id', 'hero-headline');
    });
  });

  describe('Test Case 3: Sign Up Free Button', () => {
    it('contains "Sign Up Free" button with role="button"', () => {
      renderWithRouter(<HeroSection />);

      const signUpButton = screen.getByRole('button', { name: /sign up free/i });
      expect(signUpButton).toBeInTheDocument();
    });

    it('Sign Up Free button links to /register', () => {
      renderWithRouter(<HeroSection />);

      const signUpLink = screen.getByRole('button', { name: /sign up free/i });
      expect(signUpLink).toHaveAttribute('href', '/register');
    });

    it('Sign Up Free button has primary styling', () => {
      renderWithRouter(<HeroSection />);

      const signUpButton = screen.getByRole('button', { name: /sign up free/i });
      expect(signUpButton).toHaveClass('btn-primary');
    });
  });

  describe('Test Case 4: Try as Guest Button', () => {
    it('contains "Try as Guest" secondary button', () => {
      renderWithRouter(<HeroSection />);

      const guestButton = screen.getByRole('button', { name: /try as guest/i });
      expect(guestButton).toBeInTheDocument();
    });

    it('Try as Guest button has outline/secondary styling', () => {
      renderWithRouter(<HeroSection />);

      const guestButton = screen.getByRole('button', { name: /try as guest/i });
      expect(guestButton).toHaveClass('btn-outline');
    });
  });

  describe('Test Case 5: Minimum Height', () => {
    it('hero section has min-height of 600px on desktop viewport', () => {
      renderWithRouter(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // Check inline style for min-height
      expect(heroSection).toHaveStyle({ minHeight: '600px' });
    });

    it('hero section has hero-section class for CSS styling', () => {
      renderWithRouter(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('hero-section');
    });
  });

  describe('Subheadline Content', () => {
    it('renders subheadline with correct text', () => {
      renderWithRouter(<HeroSection />);

      const subheadline = screen.getByText(
        /free url shortener with powerful analytics\. no account required for basic use\./i
      );
      expect(subheadline).toBeInTheDocument();
    });
  });

  describe('URL Input Form', () => {
    it('contains URL input field', () => {
      renderWithRouter(<HeroSection />);

      const urlInput = screen.getByPlaceholderText(/paste your long url here/i);
      expect(urlInput).toBeInTheDocument();
      expect(urlInput).toHaveAttribute('type', 'url');
    });

    it('URL input has accessible label', () => {
      renderWithRouter(<HeroSection />);

      const urlInput = screen.getByLabelText(/url to shorten/i);
      expect(urlInput).toBeInTheDocument();
    });

    it('contains Shorten button', () => {
      renderWithRouter(<HeroSection />);

      const shortenButton = screen.getByRole('button', { name: /shorten/i });
      expect(shortenButton).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('hero section has proper aria-labelledby', () => {
      renderWithRouter(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');
    });

    it('visual elements are hidden from screen readers', () => {
      renderWithRouter(<HeroSection />);

      // The visual transformation element should be aria-hidden
      const heroSection = screen.getByTestId('hero-section');
      const hiddenElements = heroSection.querySelectorAll('[aria-hidden="true"]');
      expect(hiddenElements.length).toBeGreaterThan(0);
    });
  });
});
