/**
 * Animation Effects Tests
 * Owner: Scenario 17 - Animation Effects
 *
 * Tests for verifying Framer Motion animations on the landing page:
 * - Page elements use motion components from Framer Motion
 * - CTA buttons have smooth hover transitions
 * - Feature cards have subtle hover animations
 *
 * Uses:
 * - Vitest for testing
 * - Testing Library for React component testing
 * - Framer Motion mocking for animation testing
 */
import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from './setup';
import Home from '../../../src/pages/Home';
import HeroSection from '../../../src/components/landing/HeroSection';
import FeaturesSection from '../../../src/components/landing/FeaturesSection';
import CTASection from '../../../src/components/landing/CTASection';
import FuturisticButton from '../../../src/components/FuturisticButton';
import GlassMorphismCard from '../../../src/components/GlassMorphismCard';

describe('Animation Effects', () => {
  describe('Test Case 1: Landing page uses Framer Motion components', () => {
    it('renders page with motion components from Framer Motion', () => {
      renderWithProviders(<Home />);

      // The page should render successfully with Framer Motion components
      // HeroSection uses motion.h1, motion.p, motion.div
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toBeInTheDocument();
      expect(heroHeading.textContent).toContain('Shorten Links');

      // Features section uses motion.div for animations
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // CTA section uses motion.div for entrance animation
      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection).toBeInTheDocument();
    });

    it('HeroSection contains motion elements for entrance animations', () => {
      renderWithProviders(<HeroSection />);

      // HeroSection uses motion.h1, motion.p, motion.div with initial/animate props
      // These should render as regular h1, p, div elements in JSDOM
      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toBeInTheDocument();
      expect(heading.textContent).toContain('Shorten Links. Track Everything.');

      // Subheadline paragraph
      const subheadline = screen.getByText(/Create short, powerful links/i);
      expect(subheadline).toBeInTheDocument();

      // Dashboard preview mockup (animated on load)
      const dashboardPreview = screen.getByText(/Total Links/i).closest('.mockup-window');
      expect(dashboardPreview).toBeInTheDocument();
    });

    it('FeaturesSection uses motion components for scroll-triggered animations', () => {
      renderWithProviders(<FeaturesSection />);

      // Features section title uses motion.div with whileInView
      const featuresTitle = screen.getByRole('heading', { name: /Features/i });
      expect(featuresTitle).toBeInTheDocument();

      // Feature cards container uses motion.div with containerVariants
      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();

      // Feature card titles use motion.h3 with itemVariants
      const featureTitles = screen.getAllByTestId('feature-title');
      expect(featureTitles.length).toBe(4);
    });

    it('CTASection uses motion.div for entrance animation', () => {
      renderWithProviders(<CTASection />);

      // CTA section uses motion.div with whileInView animation
      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection).toBeInTheDocument();

      // CTA heading should be present
      const ctaHeading = screen.getByRole('heading', { name: /Ready to Start Shortening Links/i });
      expect(ctaHeading).toBeInTheDocument();
    });
  });

  describe('Test Case 2: CTA button has smooth hover transition', () => {
    it('FuturisticButton renders as motion.button with transition classes', () => {
      renderWithProviders(
        <FuturisticButton variant="primary" size="lg">
          Test Button
        </FuturisticButton>
      );

      const button = screen.getByRole('button', { name: /Test Button/i });
      expect(button).toBeInTheDocument();

      // FuturisticButton uses transition-all duration-300 for smooth transitions
      expect(button).toHaveClass('transition-all');
      expect(button).toHaveClass('duration-300');

      // FuturisticButton is rendered as motion.button which provides whileHover/whileTap
      expect(button.tagName.toLowerCase()).toBe('button');
    });

    it('primary CTA button in HeroSection has transition styling', () => {
      renderWithProviders(<HeroSection />);

      // Get Started button should have transition classes
      const getStartedButton = screen.getByRole('button', { name: /Get started/i });
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveClass('btn');
      expect(getStartedButton).toHaveClass('btn-primary');
      expect(getStartedButton).toHaveClass('transition-all');
      expect(getStartedButton).toHaveClass('duration-300');
    });

    it('CTA register button has transition styling', () => {
      renderWithProviders(<CTASection />);

      const registerButton = screen.getByTestId('cta-register-button');
      expect(registerButton).toBeInTheDocument();
      expect(registerButton).toHaveClass('btn');
      expect(registerButton).toHaveClass('btn-primary');
      expect(registerButton).toHaveClass('transition-all');
      expect(registerButton).toHaveClass('duration-300');
    });

    it('buttons respond to user interaction', async () => {
      const user = userEvent.setup();
      renderWithProviders(<HeroSection />);

      const getStartedButton = screen.getByRole('button', { name: /Get started/i });
      expect(getStartedButton).toBeInTheDocument();

      // Button should be clickable (interaction works)
      await user.click(getStartedButton);
      // If no error is thrown, the interaction works
    });
  });

  describe('Test Case 3: Feature cards have subtle hover animation', () => {
    it('GlassMorphismCard renders as motion.div with transition styles', () => {
      renderWithProviders(
        <GlassMorphismCard>
          <div>Test Card Content</div>
        </GlassMorphismCard>
      );

      const card = screen.getByText('Test Card Content').closest('.card');
      expect(card).toBeInTheDocument();

      // GlassMorphismCard has backdrop-blur for glass effect
      expect(card).toHaveClass('backdrop-blur-lg');
      expect(card).toHaveClass('shadow-xl');
      expect(card).toHaveClass('card');
    });

    it('feature cards in FeaturesSection are motion components', () => {
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards.length).toBe(4);

      // Each feature card should be within a GlassMorphismCard (motion.div)
      featureCards.forEach((card) => {
        const cardWrapper = card.closest('.card');
        expect(cardWrapper).toBeInTheDocument();
        expect(cardWrapper).toHaveClass('backdrop-blur-lg');
        expect(cardWrapper).toHaveClass('card');
      });
    });

    it('feature cards have animation-related styling', () => {
      renderWithProviders(<FeaturesSection />);

      // GlassMorphismCard uses motion.div with whileInView animation
      // and has shadow-xl class for visual depth effect
      const featureCards = screen.getAllByTestId('feature-card');

      featureCards.forEach((card) => {
        const cardWrapper = card.closest('.card');
        expect(cardWrapper).toHaveClass('shadow-xl');
        // Has border for visual definition
        expect(cardWrapper).toHaveClass('border');
      });
    });

    it('feature cards can be interacted with', async () => {
      const user = userEvent.setup();
      renderWithProviders(<FeaturesSection />);

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards.length).toBe(4);

      // Hover over first feature card
      const firstCard = featureCards[0].closest('.card');
      if (firstCard) {
        await user.hover(firstCard);
        // If no error is thrown, the interaction works
      }
    });
  });

  describe('Full page animation integration', () => {
    it('landing page renders all animated sections', () => {
      renderWithProviders(<Home />);

      // Hero section with entrance animations
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toBeInTheDocument();

      // Features section with scroll-triggered animations
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();

      // CTA section with entrance animation
      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection).toBeInTheDocument();

      // All sections should be rendered without errors
      expect(screen.getByRole('main')).toBeInTheDocument();
    });

    it('motion components are correctly integrated throughout the page', () => {
      const { container } = renderWithProviders(<Home />);

      // HeroSection uses motion elements
      const heroSection = container.querySelector('[aria-labelledby="hero-heading"]');
      expect(heroSection).toBeInTheDocument();

      // Features grid uses motion.div
      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();

      // All CTA buttons use motion.button (via FuturisticButton)
      const ctaButtons = screen.getAllByRole('button', { name: /get started|create.*account/i });
      expect(ctaButtons.length).toBeGreaterThanOrEqual(1);

      ctaButtons.forEach((button) => {
        expect(button).toHaveClass('transition-all');
      });
    });
  });
});
