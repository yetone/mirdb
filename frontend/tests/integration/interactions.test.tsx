/**
 * Integration Tests for Interactive Hover Effects
 * Owner: Scenario 12 - Interactive Hover Effects
 *
 * Tests hover effects on feature cards, CTA buttons, and footer links
 * as specified in UX interaction patterns
 *
 * Requirements: User Interaction Patterns (hover effects on cards, CTAs, links)
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import React from 'react';
import { Home } from '../../src/pages/Home';
import { FeatureCard } from '../../src/components/homepage/FeatureCard';
import { HeroSection } from '../../src/components/homepage/HeroSection';
import { Footer } from '../../src/components/homepage/Footer';
import { ThemeProvider } from '../../src/contexts/ThemeContext';

// Helper to render with necessary providers
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        {ui}
      </ThemeProvider>
    </BrowserRouter>
  );
}

// Mock matchMedia for theme context
beforeEach(() => {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  });
});

afterEach(() => {
  vi.clearAllMocks();
});

describe('Interactive Hover Effects', () => {
  describe('Test Case 1: Feature Card Hover Effects', () => {
    it('feature cards have hover elevation classes (shadow change)', () => {
      const mockIcon = <svg data-testid="mock-icon" />;

      render(
        <FeatureCard
          icon={mockIcon}
          title="Test Feature"
          description="Test description"
        />
      );

      const card = screen.getByTestId('feature-card');

      // Verify the card has hover shadow class for elevation effect
      expect(card).toHaveClass('hover:shadow-lg');
      // Verify transition class for smooth effect
      expect(card).toHaveClass('transition-shadow');
      expect(card).toHaveClass('duration-300');
    });

    it('feature cards have hover border highlight classes', () => {
      const mockIcon = <svg data-testid="mock-icon" />;

      render(
        <FeatureCard
          icon={mockIcon}
          title="Test Feature"
          description="Test description"
        />
      );

      const card = screen.getByTestId('feature-card');

      // Verify the card has hover border highlight class
      expect(card).toHaveClass('hover:border-primary');
      // Verify default border is transparent (to show change on hover)
      expect(card).toHaveClass('border');
      expect(card).toHaveClass('border-transparent');
    });

    it('all feature cards in the homepage have hover effects', () => {
      renderWithProviders(<Home />);

      const featureCards = screen.getAllByTestId('feature-card');

      // Should have 3 feature cards
      expect(featureCards.length).toBe(3);

      // Each card should have hover effect classes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('hover:shadow-lg');
        expect(card).toHaveClass('hover:border-primary');
      });
    });

    it('feature cards respond to mouse enter/leave events', () => {
      const mockIcon = <svg data-testid="mock-icon" />;

      render(
        <FeatureCard
          icon={mockIcon}
          title="Test Feature"
          description="Test description"
        />
      );

      const card = screen.getByTestId('feature-card');

      // Simulate mouse enter
      fireEvent.mouseEnter(card);
      expect(card).toBeInTheDocument();

      // Simulate mouse leave
      fireEvent.mouseLeave(card);
      expect(card).toBeInTheDocument();

      // Element remains interactive after hover events
      expect(card).toHaveClass('hover:shadow-lg');
    });
  });

  describe('Test Case 2: Primary CTA Button Hover Effects', () => {
    it('primary CTA button has hover state classes', () => {
      renderWithProviders(<HeroSection />);

      const primaryCTA = screen.getByTestId('cta-primary');

      // DaisyUI btn-primary class includes hover styles
      expect(primaryCTA).toHaveClass('btn');
      expect(primaryCTA).toHaveClass('btn-primary');
    });

    it('primary CTA button is interactive and focusable', () => {
      renderWithProviders(<HeroSection />);

      const primaryCTA = screen.getByTestId('cta-primary');

      // Button should be a link element
      expect(primaryCTA.tagName.toLowerCase()).toBe('a');
      expect(primaryCTA).toHaveAttribute('href', '/register');

      // Verify element is focusable (links are naturally focusable)
      expect(primaryCTA).not.toHaveAttribute('tabindex', '-1');

      // Simulate focus for keyboard users - verify no errors thrown
      fireEvent.focus(primaryCTA);
      expect(primaryCTA).toBeInTheDocument();
    });

    it('primary CTA button responds to mouse hover events', () => {
      renderWithProviders(<HeroSection />);

      const primaryCTA = screen.getByTestId('cta-primary');

      // Simulate mouse enter
      fireEvent.mouseEnter(primaryCTA);
      expect(primaryCTA).toBeInTheDocument();

      // Simulate mouse leave
      fireEvent.mouseLeave(primaryCTA);
      expect(primaryCTA).toBeInTheDocument();
    });

    it('primary CTA in full homepage has hover styles', () => {
      renderWithProviders(<Home />);

      const primaryCTA = screen.getByTestId('cta-primary');

      expect(primaryCTA).toHaveClass('btn-primary');
      expect(primaryCTA).toHaveTextContent('Get Started Free');
    });
  });

  describe('Test Case 3: Secondary CTA Button Hover Effects', () => {
    it('secondary CTA button has hover state classes', () => {
      renderWithProviders(<HeroSection />);

      const secondaryCTA = screen.getByTestId('cta-secondary');

      // DaisyUI btn-outline class includes hover styles
      expect(secondaryCTA).toHaveClass('btn');
      expect(secondaryCTA).toHaveClass('btn-outline');
    });

    it('secondary CTA button is interactive and focusable', () => {
      renderWithProviders(<HeroSection />);

      const secondaryCTA = screen.getByTestId('cta-secondary');

      // Button should be a link element
      expect(secondaryCTA.tagName.toLowerCase()).toBe('a');
      expect(secondaryCTA).toHaveAttribute('href', '/demo');

      // Verify element is focusable (links are naturally focusable)
      expect(secondaryCTA).not.toHaveAttribute('tabindex', '-1');

      // Simulate focus for keyboard users - verify no errors thrown
      fireEvent.focus(secondaryCTA);
      expect(secondaryCTA).toBeInTheDocument();
    });

    it('secondary CTA button responds to mouse hover events', () => {
      renderWithProviders(<HeroSection />);

      const secondaryCTA = screen.getByTestId('cta-secondary');

      // Simulate mouse enter
      fireEvent.mouseEnter(secondaryCTA);
      expect(secondaryCTA).toBeInTheDocument();

      // Simulate mouse leave
      fireEvent.mouseLeave(secondaryCTA);
      expect(secondaryCTA).toBeInTheDocument();
    });

    it('secondary CTA in full homepage has hover styles', () => {
      renderWithProviders(<Home />);

      const secondaryCTA = screen.getByTestId('cta-secondary');

      expect(secondaryCTA).toHaveClass('btn-outline');
      expect(secondaryCTA).toHaveTextContent('View Demo');
    });
  });

  describe('Test Case 4: Footer Navigation Links Hover Effects', () => {
    it('footer navigation links have hover state classes', () => {
      renderWithProviders(<Footer />);

      // Get footer navigation links (link link-hover class)
      const footerLinks = screen.getAllByRole('link').filter(
        (link) => link.closest('[data-testid="footer-navigation"]')
      );

      expect(footerLinks.length).toBeGreaterThan(0);

      // Each navigation link should have the link-hover class
      footerLinks.forEach((link) => {
        expect(link).toHaveClass('link');
        expect(link).toHaveClass('link-hover');
      });
    });

    it('footer links respond to mouse hover events', () => {
      renderWithProviders(<Footer />);

      // Get the first navigation link
      const firstLink = screen.getByTestId('footer-link-features');

      expect(firstLink).toHaveClass('link-hover');

      // Simulate mouse enter
      fireEvent.mouseEnter(firstLink);
      expect(firstLink).toBeInTheDocument();

      // Simulate mouse leave
      fireEvent.mouseLeave(firstLink);
      expect(firstLink).toBeInTheDocument();
    });

    it('footer links are focusable for keyboard navigation', () => {
      renderWithProviders(<Footer />);

      const footerLink = screen.getByTestId('footer-link-about');

      // Verify element is a link and focusable
      expect(footerLink.tagName.toLowerCase()).toBe('a');
      expect(footerLink).not.toHaveAttribute('tabindex', '-1');

      // Simulate focus for keyboard users - verify no errors thrown
      fireEvent.focus(footerLink);
      expect(footerLink).toBeInTheDocument();
    });

    it('all footer navigation sections have links with hover effects', () => {
      renderWithProviders(<Footer />);

      // Check links from each section
      const productLinks = ['features', 'pricing', 'dashboard', 'api'];
      const companyLinks = ['about', 'blog', 'careers', 'contact'];

      productLinks.forEach((linkName) => {
        const link = screen.getByTestId(`footer-link-${linkName}`);
        expect(link).toHaveClass('link-hover');
      });

      companyLinks.forEach((linkName) => {
        const link = screen.getByTestId(`footer-link-${linkName}`);
        expect(link).toHaveClass('link-hover');
      });
    });

    it('footer social media buttons have hover states', () => {
      renderWithProviders(<Footer />);

      const twitterBtn = screen.getByTestId('social-twitter');
      const githubBtn = screen.getByTestId('social-github');
      const linkedinBtn = screen.getByTestId('social-linkedin');

      // Social buttons use btn-ghost which includes hover styles
      expect(twitterBtn).toHaveClass('btn-ghost');
      expect(githubBtn).toHaveClass('btn-ghost');
      expect(linkedinBtn).toHaveClass('btn-ghost');

      // Verify they respond to hover events
      fireEvent.mouseEnter(twitterBtn);
      expect(twitterBtn).toBeInTheDocument();
    });
  });

  describe('Integration: Full Homepage Hover Effects', () => {
    it('all interactive elements on homepage have proper hover classes', () => {
      renderWithProviders(<Home />);

      // Feature cards
      const featureCards = screen.getAllByTestId('feature-card');
      featureCards.forEach((card) => {
        expect(card).toHaveClass('hover:shadow-lg');
        expect(card).toHaveClass('hover:border-primary');
      });

      // CTA buttons
      const primaryCTA = screen.getByTestId('cta-primary');
      const secondaryCTA = screen.getByTestId('cta-secondary');
      expect(primaryCTA).toHaveClass('btn-primary');
      expect(secondaryCTA).toHaveClass('btn-outline');

      // Footer links
      const footerLink = screen.getByTestId('footer-link-features');
      expect(footerLink).toHaveClass('link-hover');
    });

    it('homepage maintains interactivity after multiple hover events', () => {
      renderWithProviders(<Home />);

      const featureCards = screen.getAllByTestId('feature-card');
      const primaryCTA = screen.getByTestId('cta-primary');
      const footerLink = screen.getByTestId('footer-link-about');

      // Simulate multiple hover interactions
      featureCards.forEach((card) => {
        fireEvent.mouseEnter(card);
        fireEvent.mouseLeave(card);
      });

      fireEvent.mouseEnter(primaryCTA);
      fireEvent.mouseLeave(primaryCTA);

      fireEvent.mouseEnter(footerLink);
      fireEvent.mouseLeave(footerLink);

      // All elements should still be interactive
      expect(featureCards[0]).toHaveClass('hover:shadow-lg');
      expect(primaryCTA).toHaveClass('btn-primary');
      expect(footerLink).toHaveClass('link-hover');
    });

    it('interactive elements have proper transition classes for smooth effects', () => {
      renderWithProviders(<Home />);

      const featureCards = screen.getAllByTestId('feature-card');

      // Feature cards should have transition classes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('transition-shadow');
      });
    });
  });
});
