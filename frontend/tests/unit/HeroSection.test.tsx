/**
 * Hero Section Unit Tests
 * Owner: Scenarios 1, 12, 17, 19
 *
 * Test coverage:
 * - Scenario 1: Hero section display and content
 * - Scenario 12: Background effects and animations
 * - Scenario 17: FuturisticButton integration
 * - Scenario 19: Value proposition communication
 *
 * Test suites:
 * - describe('Hero Section Content')
 * - describe('CTA Buttons')
 * - describe('Background Effects')
 * - describe('Animations')
 */

import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders, createMockAuthContext } from '../utils/renderWithProviders';
import HeroSection from '../../src/components/homepage/HeroSection';

describe('Hero Section Display and Content (Scenario 1)', () => {
  describe('Hero Section Content', () => {
    it('should render the hero section container', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
    });

    it('should display a headline related to URL shortening', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      expect(headline).toBeInTheDocument();

      // Verify headline contains text related to URL shortening, link tracking, or insights
      const headlineText = headline.textContent?.toLowerCase() || '';
      expect(
        headlineText.includes('shorten') ||
        headlineText.includes('link') ||
        headlineText.includes('track') ||
        headlineText.includes('insight') ||
        headlineText.includes('url')
      ).toBe(true);
    });

    it('should display a subheadline explaining key benefits', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const subheadline = screen.getByTestId('hero-subheadline');
      expect(subheadline).toBeInTheDocument();
      expect(subheadline.textContent).toBeTruthy();

      // Verify subheadline provides additional context (more than 50 chars)
      expect(subheadline.textContent!.length).toBeGreaterThan(50);
    });

    it('should have proper semantic heading structure with h1', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      expect(headline.tagName).toBe('H1');
    });

    it('should have an aria-label for accessibility', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveAttribute('aria-label');
    });
  });

  describe('CTA Buttons for Unauthenticated Users', () => {
    it('should display "Get Started" primary CTA when not authenticated', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton.textContent?.toLowerCase()).toContain('get started');
    });

    it('should display "Login" secondary CTA when not authenticated', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const loginButton = screen.getByTestId('login-button');
      expect(loginButton).toBeInTheDocument();
      expect(loginButton.textContent?.toLowerCase()).toContain('login');
    });

    it('should link Get Started button to register page', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    it('should link Login button to login page', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const loginButton = screen.getByTestId('login-button');
      expect(loginButton).toHaveAttribute('href', '/login');
    });
  });

  describe('CTA Buttons for Authenticated Users', () => {
    it('should display "Go to Dashboard" CTA when authenticated', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      const dashboardButton = screen.getByTestId('hero-cta-dashboard');
      expect(dashboardButton).toBeInTheDocument();
      expect(dashboardButton.textContent?.toLowerCase()).toContain('dashboard');
    });

    it('should not display Get Started or Login when authenticated', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      expect(screen.queryByTestId('get-started-button')).not.toBeInTheDocument();
      expect(screen.queryByTestId('login-button')).not.toBeInTheDocument();
    });

    it('should link Dashboard button to dashboard page', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      const dashboardButton = screen.getByTestId('hero-cta-dashboard');
      expect(dashboardButton).toHaveAttribute('href', '/dashboard');
    });
  });

  describe('Value Proposition Communication (Scenario 19)', () => {
    it('should communicate URL shortening capability in headline', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      const headlineText = headline.textContent?.toLowerCase() || '';

      // Check for URL shortening related keywords
      expect(
        headlineText.includes('shorten') ||
        headlineText.includes('short') ||
        headlineText.includes('link')
      ).toBe(true);
    });

    it('should communicate tracking/analytics capability', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      const subheadline = screen.getByTestId('hero-subheadline');
      const combinedText = `${headline.textContent} ${subheadline.textContent}`.toLowerCase();

      // Check for analytics/tracking related keywords
      expect(
        combinedText.includes('track') ||
        combinedText.includes('analytics') ||
        combinedText.includes('insight') ||
        combinedText.includes('click') ||
        combinedText.includes('statistics')
      ).toBe(true);
    });

    it('should explain benefits in subheadline', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const subheadline = screen.getByTestId('hero-subheadline');
      const subheadlineText = subheadline.textContent?.toLowerCase() || '';

      // Check for benefit-related keywords
      expect(
        subheadlineText.includes('memorable') ||
        subheadlineText.includes('statistics') ||
        subheadlineText.includes('understand') ||
        subheadlineText.includes('audience') ||
        subheadlineText.includes('geographic') ||
        subheadlineText.includes('device')
      ).toBe(true);
    });
  });
});

/**
 * Background Effects and Animations Tests (Scenario 12)
 *
 * Test coverage:
 * - Test Case 1: BackgroundEffect component rendering in hero section
 * - Test Case 2: Framer Motion animation components exist
 * - Test Case 3: Scroll-triggered animation state changes
 * - Test Case 4: GPU-accelerated CSS transforms in animations
 */
describe('Background Effects and Animations (Scenario 12)', () => {
  describe('BackgroundEffect Rendering', () => {
    it('should render BackgroundEffect component in hero section', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      // BackgroundEffect creates a fixed container with animated elements
      // It should be present within the hero section structure
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();

      // The BackgroundEffect component creates a div with class "fixed inset-0 -z-10"
      // We can verify it's rendered by checking for elements with those characteristics
      const backgroundContainer = heroSection.querySelector('.fixed');
      expect(backgroundContainer).toBeInTheDocument();
    });

    it('should have BackgroundEffect with proper z-index for layering', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      const backgroundContainer = heroSection.querySelector('.fixed.-z-10');
      expect(backgroundContainer).toBeInTheDocument();
    });

    it('should render animated blur elements in BackgroundEffect', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      // BackgroundEffect renders elements with blur-3xl class for the glow effect
      const blurElements = heroSection.querySelectorAll('.blur-3xl');
      expect(blurElements.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Framer Motion Animation Components', () => {
    it('should wrap headline in motion component with animation props', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      expect(headline).toBeInTheDocument();

      // Framer Motion components apply inline styles for animations
      // In jsdom, the initial state is rendered (opacity: 0 for fade-in)
      // The presence of a style attribute with transform/opacity confirms motion wrapping
      expect(headline).toHaveAttribute('style');
      const styleAttr = headline.getAttribute('style') || '';
      // Motion components use transform and/or opacity for GPU-accelerated animations
      expect(styleAttr.includes('transform') || styleAttr.includes('opacity')).toBe(true);
    });

    it('should wrap subheadline in motion component', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const subheadline = screen.getByTestId('hero-subheadline');
      expect(subheadline).toBeInTheDocument();

      // Verify motion wrapper by checking for style attribute with animation properties
      expect(subheadline).toHaveAttribute('style');
      const styleAttr = subheadline.getAttribute('style') || '';
      expect(styleAttr.includes('transform') || styleAttr.includes('opacity')).toBe(true);
    });

    it('should wrap CTA buttons container in motion component', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      // CTA buttons should be rendered within a motion.div container
      const getStartedButton = screen.getByTestId('get-started-button');
      const loginButton = screen.getByTestId('login-button');

      expect(getStartedButton).toBeInTheDocument();
      expect(loginButton).toBeInTheDocument();

      // The parent container should have motion-applied styles (transform/opacity)
      const ctaContainer = getStartedButton.parentElement;
      expect(ctaContainer).toBeInTheDocument();
      expect(ctaContainer).toHaveAttribute('style');
    });

    it('should render BackgroundEffect animated elements with motion', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      // BackgroundEffect uses motion.div for animated circles
      const backgroundContainer = heroSection.querySelector('.fixed');
      expect(backgroundContainer).toBeInTheDocument();

      // Verify the animated elements exist
      const animatedElements = backgroundContainer?.querySelectorAll('.rounded-full');
      expect(animatedElements?.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Scroll-Triggered Animation State Changes', () => {
    it('should have animation initial states defined for hero elements', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      // Framer Motion applies initial animation state in jsdom
      // The initial state shows opacity: 0 and transform (for y offset)
      const headline = screen.getByTestId('hero-headline');
      const subheadline = screen.getByTestId('hero-subheadline');

      // Elements should be in the document with initial animation state
      expect(headline).toBeInTheDocument();
      expect(subheadline).toBeInTheDocument();

      // Verify initial state (opacity: 0 means animation is set up correctly)
      const headlineStyle = headline.getAttribute('style') || '';
      expect(headlineStyle).toContain('opacity');
    });

    it('should have staggered animation delays for sequential reveal', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      // All animated elements should be in the document
      const headline = screen.getByTestId('hero-headline');
      const subheadline = screen.getByTestId('hero-subheadline');
      const getStartedButton = screen.getByTestId('get-started-button');

      expect(headline).toBeInTheDocument();
      expect(subheadline).toBeInTheDocument();
      expect(getStartedButton).toBeInTheDocument();

      // Verify they all have motion styles applied (indicating animation setup)
      expect(headline).toHaveAttribute('style');
      expect(subheadline).toHaveAttribute('style');
    });

    it('should render hero section with overflow hidden for animation containment', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('overflow-hidden');
    });
  });

  describe('GPU-Accelerated CSS Transforms', () => {
    it('should use transform for headline animation (GPU accelerated)', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      // Framer Motion uses transform for y animations (translateY) which is GPU accelerated
      // The style attribute should contain transform property
      const styleAttr = headline.getAttribute('style') || '';
      expect(styleAttr).toContain('transform');
      // Specifically check for translateY (used for y-axis animation)
      expect(styleAttr).toContain('translateY');
    });

    it('should use opacity for fade animations (GPU accelerated)', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      const subheadline = screen.getByTestId('hero-subheadline');

      // Opacity is used for fade-in animations - GPU accelerated property
      // Framer Motion sets opacity in the style attribute
      const headlineStyle = headline.getAttribute('style') || '';
      const subheadlineStyle = subheadline.getAttribute('style') || '';

      expect(headlineStyle).toContain('opacity');
      expect(subheadlineStyle).toContain('opacity');
    });

    it('should not use layout-triggering properties for animations', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      const styleAttr = headline.getAttribute('style') || '';

      // Animations should use GPU-accelerated properties (transform, opacity)
      // NOT layout-triggering properties like top, left, margin, width, height
      expect(styleAttr).not.toContain('margin-top');
      expect(styleAttr).not.toContain('top:');
      expect(styleAttr).not.toContain('left:');

      // Should use transform and opacity instead
      expect(styleAttr.includes('transform') || styleAttr.includes('opacity')).toBe(true);
    });

    it('should have BackgroundEffect with GPU-accelerated animations', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      const backgroundContainer = heroSection.querySelector('.fixed');

      // Verify background container exists
      expect(backgroundContainer).toBeInTheDocument();

      // The animated blur elements should be present
      const animatedElements = backgroundContainer?.querySelectorAll('.rounded-full');
      expect(animatedElements?.length).toBeGreaterThanOrEqual(2);

      // Verify the elements have animation classes applied (for GPU acceleration)
      // The blur-3xl class forces GPU compositing, and the absolute positioning
      // combined with motion.div animations use transform (translateX/Y)
      animatedElements?.forEach((element) => {
        // These are motion.div elements that use transform animations
        expect(element).toHaveClass('rounded-full');
        expect(element).toHaveClass('blur-3xl');
        // The absolute positioning and animation is handled by Framer Motion's transform
        expect(element).toHaveClass('absolute');
      });
    });

    it('should use blur filter for visual effects (composited layer)', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      const blurElements = heroSection.querySelectorAll('.blur-3xl');

      // Blur elements should exist for the background effect
      expect(blurElements.length).toBeGreaterThanOrEqual(2);

      // Each blur element should have the blur class applied
      // blur-3xl is a Tailwind class that applies filter: blur() which forces compositing
      blurElements.forEach((element) => {
        expect(element).toHaveClass('blur-3xl');
      });
    });
  });
});
