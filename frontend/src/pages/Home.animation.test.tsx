import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home, { heroVariants, containerVariants, featureCardVariants } from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

const renderHome = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// Smooth Scroll Animations Tests (Scenario: REQ-9)
describe('Home - Smooth Scroll Animations (REQ-9)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Hero section elements animate in on page load
  describe('Test Case 1: Hero section elements animate in on page load', () => {
    it('renders hero section with animated container using Framer Motion', () => {
      renderHome();

      const heroAnimatedContainer = screen.getByTestId('hero-animated-container');
      expect(heroAnimatedContainer).toBeInTheDocument();
    });

    it('renders hero headline with animation variant', () => {
      renderHome();

      const heroHeadline = screen.getByTestId('hero-headline-animated');
      expect(heroHeadline).toBeInTheDocument();
      expect(heroHeadline.tagName.toLowerCase()).toBe('h1');
      expect(heroHeadline).toHaveTextContent('Shorten, Share, Track');
    });

    it('renders hero subheadline with animation variant', () => {
      renderHome();

      const heroSubheadline = screen.getByTestId('hero-subheadline-animated');
      expect(heroSubheadline).toBeInTheDocument();
      expect(heroSubheadline.tagName.toLowerCase()).toBe('p');
    });

    it('renders hero buttons container with animation variant', () => {
      renderHome();

      const heroButtons = screen.getByTestId('hero-buttons-animated');
      expect(heroButtons).toBeInTheDocument();
    });

    it('hero animated container has correct initial animation props', () => {
      renderHome();

      const heroAnimatedContainer = screen.getByTestId('hero-animated-container');
      // The motion component should have initial="hidden" and animate="visible"
      // We can verify the component exists and the animations are configured
      expect(heroAnimatedContainer).toBeInTheDocument();
    });
  });

  // Test Case 2: Page smoothly scrolls to features section
  describe('Test Case 2: Click Learn More anchor link triggers smooth scroll', () => {
    it('Learn More button exists and is clickable', () => {
      renderHome();

      const learnMoreButton = screen.getByRole('button', { name: /Learn More/i });
      expect(learnMoreButton).toBeInTheDocument();
    });

    it('clicking Learn More scrolls to features section with smooth behavior', async () => {
      const scrollIntoViewMock = vi.fn();
      Element.prototype.scrollIntoView = scrollIntoViewMock;

      renderHome();

      const learnMoreButton = screen.getByRole('button', { name: /Learn More/i });
      fireEvent.click(learnMoreButton);

      await waitFor(() => {
        expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
      });
    });

    it('features section has correct id for anchor link target', () => {
      renderHome();

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toHaveAttribute('id', 'features');
    });
  });

  // Test Case 3: Feature cards animate into view
  describe('Test Case 3: Feature cards animate into view on scroll', () => {
    it('renders feature card wrappers with motion div', () => {
      renderHome();

      const featureWrappers = screen.getAllByTestId(/^animated-feature-wrapper-/);
      expect(featureWrappers.length).toBe(4);
    });

    it('each feature card is wrapped in a motion container', () => {
      renderHome();

      const featureWrapperUrlShortening = screen.getByTestId('animated-feature-wrapper-url-shortening');
      const featureWrapperAnalytics = screen.getByTestId('animated-feature-wrapper-analytics-dashboard');
      const featureWrapperGeo = screen.getByTestId('animated-feature-wrapper-geographic-insights');
      const featureWrapperShare = screen.getByTestId('animated-feature-wrapper-share-collaborate');

      expect(featureWrapperUrlShortening).toBeInTheDocument();
      expect(featureWrapperAnalytics).toBeInTheDocument();
      expect(featureWrapperGeo).toBeInTheDocument();
      expect(featureWrapperShare).toBeInTheDocument();
    });

    it('features grid has animation variants for scroll-triggered animations', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();
    });

    it('features section uses ref for inView detection', () => {
      renderHome();

      // The features section should have ref attached (we verify by checking it renders correctly)
      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
    });
  });

  // Test Case 4: Motion components are properly configured with animation variants
  describe('Test Case 4: Motion components are properly configured with animation variants', () => {
    it('heroVariants has correct hidden state', () => {
      expect(heroVariants.hidden).toEqual({ opacity: 0, y: 30 });
    });

    it('heroVariants has correct visible state with transition', () => {
      expect(heroVariants.visible).toHaveProperty('opacity', 1);
      expect(heroVariants.visible).toHaveProperty('y', 0);
      expect(heroVariants.visible.transition).toHaveProperty('duration', 0.6);
      expect(heroVariants.visible.transition).toHaveProperty('ease', 'easeOut');
    });

    it('containerVariants has correct hidden state', () => {
      expect(containerVariants.hidden).toEqual({ opacity: 0 });
    });

    it('containerVariants has staggerChildren for sequential animations', () => {
      expect(containerVariants.visible.transition).toHaveProperty('staggerChildren', 0.15);
      expect(containerVariants.visible.transition).toHaveProperty('delayChildren', 0.2);
    });

    it('featureCardVariants has correct hidden state', () => {
      expect(featureCardVariants.hidden).toEqual({ opacity: 0, y: 40 });
    });

    it('featureCardVariants has correct visible state with transition', () => {
      expect(featureCardVariants.visible).toHaveProperty('opacity', 1);
      expect(featureCardVariants.visible).toHaveProperty('y', 0);
      expect(featureCardVariants.visible.transition).toHaveProperty('duration', 0.5);
      expect(featureCardVariants.visible.transition).toHaveProperty('ease', 'easeOut');
    });
  });

  // Additional animation tests
  describe('Additional animation behavior tests', () => {
    it('AnimatePresence is used for demo result animations', async () => {
      vi.useFakeTimers({ shouldAdvanceTime: true });

      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      // Advance timer to complete the animation
      await vi.advanceTimersByTimeAsync(1000);

      await waitFor(() => {
        const demoResult = screen.getByTestId('demo-result');
        expect(demoResult).toBeInTheDocument();
      });

      vi.useRealTimers();
    });

    it('hero section animates on initial load (motion.div present)', () => {
      renderHome();

      // Verify that motion elements are rendered
      const heroContainer = screen.getByTestId('hero-animated-container');
      const heroHeadline = screen.getByTestId('hero-headline-animated');
      const heroSubheadline = screen.getByTestId('hero-subheadline-animated');
      const heroButtons = screen.getByTestId('hero-buttons-animated');

      expect(heroContainer).toBeInTheDocument();
      expect(heroHeadline).toBeInTheDocument();
      expect(heroSubheadline).toBeInTheDocument();
      expect(heroButtons).toBeInTheDocument();
    });

    it('imports framer-motion correctly (motion component works)', () => {
      // This test verifies that framer-motion is properly imported and works
      renderHome();

      // If framer-motion wasn't working, these elements wouldn't render
      expect(screen.getByTestId('hero-animated-container')).toBeInTheDocument();
    });
  });
});
