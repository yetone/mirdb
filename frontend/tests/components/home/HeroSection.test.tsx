/**
 * HeroSection component tests.
 * Owner: Scenario 2 - Hero Section Content and CTAs
 *
 * Test coverage:
 * - Headline displays URL shortening value proposition
 * - Subheadline provides supporting description
 * - 'Get Started' CTA navigates to /register
 * - 'Sign In' CTA navigates to /login
 * - CTAs have proper ARIA labels and keyboard accessibility
 */
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { HeroSection } from '@/components/home/HeroSection';
import { renderWithProviders } from '../../utils/renderWithProviders';

describe('HeroSection', () => {
  describe('Test Case 1: Headline displays URL shortening value proposition', () => {
    it('renders headline with URL shortening value proposition', () => {
      renderWithProviders(<HeroSection />);

      const headline = screen.getByTestId('hero-headline');
      expect(headline).toBeInTheDocument();

      // Verify the headline contains URL shortening related text
      expect(headline.textContent).toMatch(/shorten|url|link/i);
    });

    it('headline clearly communicates the product purpose', () => {
      renderWithProviders(<HeroSection />);

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();

      // Check for value proposition keywords
      const headlineText = headline.textContent?.toLowerCase() || '';
      const hasValueProposition =
        headlineText.includes('shorten') ||
        headlineText.includes('url') ||
        headlineText.includes('link') ||
        headlineText.includes('insight');

      expect(hasValueProposition).toBe(true);
    });
  });

  describe('Test Case 5: Subheadline provides supporting description', () => {
    it('renders subheadline with product benefits', () => {
      renderWithProviders(<HeroSection />);

      const subheadline = screen.getByTestId('hero-subheadline');
      expect(subheadline).toBeInTheDocument();

      // Verify subheadline contains supporting text about benefits
      const subheadlineText = subheadline.textContent?.toLowerCase() || '';
      const hasBenefits =
        subheadlineText.includes('analytic') ||
        subheadlineText.includes('track') ||
        subheadlineText.includes('insight') ||
        subheadlineText.includes('click');

      expect(hasBenefits).toBe(true);
    });

    it('subheadline explains product value', () => {
      renderWithProviders(<HeroSection />);

      const subheadline = screen.getByTestId('hero-subheadline');

      // Subheadline should have meaningful content
      expect(subheadline.textContent?.length).toBeGreaterThan(50);
    });
  });

  describe('Test Case 2: Get Started CTA navigates to /register', () => {
    it('renders Get Started button with correct link', () => {
      renderWithProviders(<HeroSection />);

      const getStartedButton = screen.getByTestId('cta-get-started');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    it('Get Started button is visible and clickable', () => {
      renderWithProviders(<HeroSection />);

      const getStartedButton = screen.getByTestId('cta-get-started');
      expect(getStartedButton).toBeVisible();

      // Verify it's a link element
      expect(getStartedButton.tagName).toBe('A');
    });
  });

  describe('Test Case 3: Sign In CTA navigates to /login', () => {
    it('renders Sign In button with correct link', () => {
      renderWithProviders(<HeroSection />);

      const signInButton = screen.getByTestId('cta-sign-in');
      expect(signInButton).toBeInTheDocument();
      expect(signInButton).toHaveAttribute('href', '/login');
    });

    it('Sign In button is visible and clickable', () => {
      renderWithProviders(<HeroSection />);

      const signInButton = screen.getByTestId('cta-sign-in');
      expect(signInButton).toBeVisible();

      // Verify it's a link element
      expect(signInButton.tagName).toBe('A');
    });
  });

  describe('Test Case 4: CTA button accessibility', () => {
    it('Get Started button has proper ARIA label', () => {
      renderWithProviders(<HeroSection />);

      const getStartedButton = screen.getByTestId('cta-get-started');
      expect(getStartedButton).toHaveAttribute('aria-label');
      expect(getStartedButton.getAttribute('aria-label')).toMatch(/get started|url|shortening/i);
    });

    it('Sign In button has proper ARIA label', () => {
      renderWithProviders(<HeroSection />);

      const signInButton = screen.getByTestId('cta-sign-in');
      expect(signInButton).toHaveAttribute('aria-label');
      expect(signInButton.getAttribute('aria-label')).toMatch(/sign in|account/i);
    });

    it('CTA buttons are keyboard focusable', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HeroSection />);

      const getStartedButton = screen.getByTestId('cta-get-started');
      const signInButton = screen.getByTestId('cta-sign-in');

      // Tab to first button
      await user.tab();
      expect(getStartedButton).toHaveFocus();

      // Tab to second button
      await user.tab();
      expect(signInButton).toHaveFocus();
    });

    it('CTA buttons have correct roles', () => {
      renderWithProviders(<HeroSection />);

      const getStartedButton = screen.getByTestId('cta-get-started');
      const signInButton = screen.getByTestId('cta-sign-in');

      // Both buttons are links, so they should have implicit link role
      expect(getStartedButton.tagName).toBe('A');
      expect(signInButton.tagName).toBe('A');
    });
  });

  describe('Hero section structure', () => {
    it('renders hero section container', () => {
      renderWithProviders(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
      expect(heroSection.tagName).toBe('SECTION');
    });

    it('hero section has proper aria label reference', () => {
      renderWithProviders(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');
    });
  });
});
