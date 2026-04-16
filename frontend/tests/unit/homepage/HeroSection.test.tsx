/**
 * Unit Tests for HeroSection Component.
 * Owner: Scenario 2 - Hero Section Display and CTA
 *
 * Tests the hero section displays a clear value proposition
 * with prominent CTA buttons that guide visitors appropriately.
 */
import { describe, it, expect, vi } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import { renderWithProviders } from './setup';
import { HeroSection } from '../../../src/components/homepage/HeroSection';

// Mock react-router-dom navigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

describe('HeroSection', () => {
  beforeEach(() => {
    mockNavigate.mockClear();
  });

  describe('Test Case 1: Hero section displays with headline containing shorten and track keywords', () => {
    it('renders the hero section with the value proposition headline', () => {
      renderWithProviders(<HeroSection />);

      const headline = screen.getByTestId('hero-headline');
      expect(headline).toBeInTheDocument();

      const headlineText = headline.textContent?.toLowerCase() ?? '';
      expect(headlineText).toContain('shorten');
      expect(headlineText).toContain('track');
    });

    it('displays the hero section immediately visible', () => {
      renderWithProviders(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toBeVisible();
    });

    it('contains a subheadline with product description', () => {
      renderWithProviders(<HeroSection />);

      const subheadline = screen.getByTestId('hero-subheadline');
      expect(subheadline).toBeInTheDocument();
      expect(subheadline.textContent).toBeTruthy();
    });
  });

  describe('Test Case 2: Button with text Get Started or Get Started Free is visible', () => {
    it('displays the primary CTA button with "Get Started Free" text for unauthenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta).toBeInTheDocument();
      expect(primaryCta.textContent).toMatch(/get started/i);
    });

    it('displays "Go to Dashboard" for authenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} username="TestUser" />);

      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta).toBeInTheDocument();
      expect(primaryCta.textContent).toMatch(/go to dashboard/i);
    });
  });

  describe('Test Case 3: Optional Learn More button may be present', () => {
    it('displays a secondary "Learn More" CTA button', () => {
      renderWithProviders(<HeroSection />);

      const secondaryCta = screen.getByTestId('hero-secondary-cta');
      expect(secondaryCta).toBeInTheDocument();
      expect(secondaryCta.textContent).toMatch(/learn more/i);
    });

    it('secondary CTA is accessible', () => {
      renderWithProviders(<HeroSection />);

      const secondaryCta = screen.getByTestId('hero-secondary-cta');
      expect(secondaryCta).toBeInTheDocument();
      expect(secondaryCta).toHaveAttribute('aria-label', 'Learn More');
    });
  });

  describe('Test Case 4: Click Get Started CTA button navigates to /register route', () => {
    it('navigates to /register when unauthenticated user clicks primary CTA', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const primaryCta = screen.getByTestId('hero-primary-cta');
      fireEvent.click(primaryCta);

      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });

    it('navigates to /dashboard when authenticated user clicks primary CTA', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} username="TestUser" />);

      const primaryCta = screen.getByTestId('hero-primary-cta');
      fireEvent.click(primaryCta);

      expect(mockNavigate).toHaveBeenCalledWith('/dashboard');
    });
  });

  describe('Test Case 5: CTA buttons use FuturisticButton component with proper styling', () => {
    it('primary CTA uses FuturisticButton component', () => {
      renderWithProviders(<HeroSection />);

      const primaryCta = screen.getByTestId('hero-primary-cta');
      // FuturisticButton adds data-testid="futuristic-button" internally
      // and has the btn class structure
      expect(primaryCta).toHaveClass('btn');
      expect(primaryCta).toHaveClass('btn-primary');
    });

    it('secondary CTA uses FuturisticButton with ghost variant', () => {
      renderWithProviders(<HeroSection />);

      const secondaryCta = screen.getByTestId('hero-secondary-cta');
      expect(secondaryCta).toHaveClass('btn');
      expect(secondaryCta).toHaveClass('btn-ghost');
    });

    it('CTAs have large size styling', () => {
      renderWithProviders(<HeroSection />);

      const primaryCta = screen.getByTestId('hero-primary-cta');
      const secondaryCta = screen.getByTestId('hero-secondary-cta');

      expect(primaryCta).toHaveClass('btn-lg');
      expect(secondaryCta).toHaveClass('btn-lg');
    });
  });

  describe('Authenticated User Experience', () => {
    it('displays personalized greeting for authenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} username="JohnDoe" />);

      const greeting = screen.getByTestId('hero-greeting');
      expect(greeting).toBeInTheDocument();
      expect(greeting.textContent).toContain('JohnDoe');
    });

    it('does not display greeting for unauthenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const greeting = screen.queryByTestId('hero-greeting');
      expect(greeting).not.toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('primary CTA has appropriate aria-label', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta).toHaveAttribute('aria-label', 'Get Started Free');
    });

    it('authenticated primary CTA has appropriate aria-label', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} />);

      const primaryCta = screen.getByTestId('hero-primary-cta');
      expect(primaryCta).toHaveAttribute('aria-label', 'Go to Dashboard');
    });

    it('headline uses semantic h1 element', () => {
      renderWithProviders(<HeroSection />);

      const headline = screen.getByTestId('hero-headline');
      expect(headline.tagName).toBe('H1');
    });

    it('hero section uses semantic section element', () => {
      renderWithProviders(<HeroSection />);

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection.tagName).toBe('SECTION');
    });
  });
});
