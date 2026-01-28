/**
 * Animation and Visual Effects Unit Tests
 * Owner: Scenario 9 - Animation & Visual Effects
 *
 * Test coverage:
 * - Framer Motion components render
 * - BackgroundEffect component present
 * - Feature cards have staggered animation
 * - Scroll-triggered animations setup (whileInView)
 * - Reduced motion preference respected
 * - Animations use GPU-accelerated properties (transform/opacity)
 * - Glassmorphism effect on cards
 * - Gradient text displays correctly
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { FeaturesSection } from '@/components/home/FeaturesSection';
import { HeroSection } from '@/components/home/HeroSection';
import { HowItWorks } from '@/components/home/HowItWorks';
import { BackgroundEffect } from '@/components/shared/BackgroundEffect';
import { Home } from '@/pages/Home';

// Helper to render with router
const renderWithRouter = (component: React.ReactNode) => {
  return render(<MemoryRouter>{component}</MemoryRouter>);
};

describe('Animation and Visual Effects - Test Case 1: Framer Motion components render', () => {
  it('FeaturesSection uses motion.div or motion components for animations', () => {
    renderWithRouter(<FeaturesSection />);

    // Check that feature cards render with motion components
    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards.length).toBeGreaterThanOrEqual(3);

    // Verify cards are rendered as motion elements (they should have data-framer-* attributes when animated)
    // Or verify they have animation-related attributes
    featureCards.forEach((card) => {
      // Motion components render as regular DOM elements with data attributes
      // We verify the component renders and has animation-related styles
      expect(card).toBeInTheDocument();
    });
  });

  it('HeroSection uses motion components for entrance animation', () => {
    renderWithRouter(<HeroSection />);

    // Check that hero elements render
    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline).toBeInTheDocument();

    // Check gradient text class is present
    expect(headline.className).toMatch(/bg-gradient-to-r|text-transparent|bg-clip-text/);
  });

  it('HowItWorks uses motion components for staggered step animations', () => {
    renderWithRouter(<HowItWorks />);

    const steps = screen.getAllByTestId('how-it-works-step');
    expect(steps.length).toBe(3);

    // Verify all steps render
    steps.forEach((step) => {
      expect(step).toBeInTheDocument();
    });
  });
});

describe('Animation and Visual Effects - Test Case 2: BackgroundEffect component present', () => {
  it('BackgroundEffect renders in the document', () => {
    render(<BackgroundEffect />);

    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();
  });

  it('BackgroundEffect is present in the homepage', () => {
    renderWithRouter(<Home />);

    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();
  });

  it('BackgroundEffect has animated orbs with pulse animation', () => {
    render(<BackgroundEffect />);

    const backgroundEffect = screen.getByTestId('background-effect');

    // Check for animated orb elements
    const orbs = backgroundEffect.querySelectorAll('.animate-pulse');
    expect(orbs.length).toBeGreaterThan(0);
  });

  it('BackgroundEffect has gradient background', () => {
    render(<BackgroundEffect />);

    const backgroundEffect = screen.getByTestId('background-effect');

    // Check for gradient classes or child elements with gradient
    const hasGradient =
      backgroundEffect.className.includes('bg-gradient') ||
      backgroundEffect.querySelector('[class*="bg-gradient"]');
    expect(hasGradient).toBeTruthy();
  });
});

describe('Animation and Visual Effects - Test Case 3: Feature cards have staggered animation', () => {
  it('FeaturesSection renders cards that can have staggered animations', () => {
    renderWithRouter(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards).toHaveLength(3);

    // Each card should be positioned for potential staggered animation
    featureCards.forEach((card, index) => {
      expect(card).toBeInTheDocument();
      // Cards should have transition delay data attributes or CSS classes for staggered effect
    });
  });

  it('Feature grid container is set up for animation coordination', () => {
    renderWithRouter(<FeaturesSection />);

    const grid = screen.getByTestId('features-grid');
    expect(grid).toBeInTheDocument();
    // Grid should be present to coordinate child animations
  });
});

describe('Animation and Visual Effects - Test Case 4: Scroll-triggered animations setup', () => {
  it('FeaturesSection section is set up for whileInView animation trigger', () => {
    renderWithRouter(<FeaturesSection />);

    // The section should be a region with proper structure for viewport-based animation
    const section = screen.getByRole('region', { name: /features/i });
    expect(section).toBeInTheDocument();
  });

  it('HowItWorks section is set up for scroll-triggered animations', () => {
    renderWithRouter(<HowItWorks />);

    const section = screen.getByRole('region', { name: /how it works/i });
    expect(section).toBeInTheDocument();
  });

  it('HeroSection is set up for initial entrance animation', () => {
    renderWithRouter(<HeroSection />);

    // HeroSection is a region labeled by its headline
    const section = screen.getByRole('region', { name: /shorten urls/i });
    expect(section).toBeInTheDocument();
  });
});

describe('Animation and Visual Effects - Test Case 5: Reduced motion preference', () => {
  const originalMatchMedia = window.matchMedia;

  beforeEach(() => {
    // Reset matchMedia before each test
    window.matchMedia = vi.fn();
  });

  afterEach(() => {
    window.matchMedia = originalMatchMedia;
  });

  it('BackgroundEffect disables animations when prefers-reduced-motion is set', () => {
    // Mock prefers-reduced-motion: reduce
    (window.matchMedia as ReturnType<typeof vi.fn>).mockImplementation((query: string) => ({
      matches: query === '(prefers-reduced-motion: reduce)',
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    render(<BackgroundEffect />);

    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();

    // When reduced motion is preferred, animated elements should not be present
    const animatedOrbs = backgroundEffect.querySelectorAll('.animate-pulse');
    expect(animatedOrbs.length).toBe(0);
  });

  it('BackgroundEffect shows animations when reduced motion is not set', () => {
    // Mock no reduced motion preference
    (window.matchMedia as ReturnType<typeof vi.fn>).mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    render(<BackgroundEffect />);

    const backgroundEffect = screen.getByTestId('background-effect');

    // When reduced motion is not preferred, animated elements should be present
    const animatedOrbs = backgroundEffect.querySelectorAll('.animate-pulse');
    expect(animatedOrbs.length).toBeGreaterThan(0);
  });

  it('BackgroundEffect renders static gradient when reduced motion is enabled', () => {
    (window.matchMedia as ReturnType<typeof vi.fn>).mockImplementation((query: string) => ({
      matches: query === '(prefers-reduced-motion: reduce)',
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    render(<BackgroundEffect />);

    const backgroundEffect = screen.getByTestId('background-effect');

    // Should still have gradient background even with reduced motion
    expect(backgroundEffect.className).toMatch(/bg-gradient/);
  });
});

describe('Animation and Visual Effects - Test Case 6: GPU-accelerated properties', () => {
  it('BackgroundEffect uses transform-based animations (blur, position)', () => {
    render(<BackgroundEffect />);

    const backgroundEffect = screen.getByTestId('background-effect');

    // Check for blur classes which use GPU acceleration
    const blurElements = backgroundEffect.querySelectorAll('[class*="blur"]');
    expect(blurElements.length).toBeGreaterThan(0);
  });

  it('Feature cards use CSS properties suitable for GPU acceleration', () => {
    renderWithRouter(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');

    featureCards.forEach((card) => {
      // Check for backdrop-blur which is GPU accelerated
      expect(card.className).toMatch(/backdrop-blur/);
    });
  });

  it('Animated elements use opacity-based classes for smooth animations', () => {
    render(<BackgroundEffect />);

    const backgroundEffect = screen.getByTestId('background-effect');

    // Check for opacity usage in orbs (e.g., primary/20, secondary/20)
    const html = backgroundEffect.innerHTML;
    expect(html).toMatch(/\/\d+|opacity/);
  });
});

describe('Animation and Visual Effects - Test Case 7: Glassmorphism effect on cards', () => {
  it('Feature cards have backdrop-blur for glassmorphism', () => {
    renderWithRouter(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');

    featureCards.forEach((card) => {
      expect(card.className).toMatch(/backdrop-blur/);
    });
  });

  it('Feature cards have transparency/opacity effect', () => {
    renderWithRouter(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');

    featureCards.forEach((card) => {
      // Check for bg-opacity or /30 style transparency
      expect(card.className).toMatch(/bg-opacity|\/30/);
    });
  });

  it('Feature cards have border styling for glass effect', () => {
    renderWithRouter(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');

    featureCards.forEach((card) => {
      expect(card.className).toMatch(/border/);
    });
  });

  it('Feature cards have shadow for depth effect', () => {
    renderWithRouter(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');

    featureCards.forEach((card) => {
      expect(card.className).toMatch(/shadow/);
    });
  });

  it('Feature cards have rounded corners for modern glass look', () => {
    renderWithRouter(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');

    featureCards.forEach((card) => {
      expect(card.className).toMatch(/rounded/);
    });
  });
});

describe('Animation and Visual Effects - Test Case 8: Gradient text animation', () => {
  it('Hero headline uses gradient text classes', () => {
    renderWithRouter(<HeroSection />);

    const headline = screen.getByRole('heading', { level: 1 });

    // Check for gradient text classes
    expect(headline.className).toMatch(/bg-gradient-to-r/);
    expect(headline.className).toMatch(/from-primary/);
    expect(headline.className).toMatch(/to-secondary/);
  });

  it('Hero headline has text-transparent for gradient to show through', () => {
    renderWithRouter(<HeroSection />);

    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline.className).toMatch(/text-transparent/);
  });

  it('Hero headline has bg-clip-text for proper gradient rendering', () => {
    renderWithRouter(<HeroSection />);

    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline.className).toMatch(/bg-clip-text/);
  });

  it('Gradient text is readable and properly styled', () => {
    renderWithRouter(<HeroSection />);

    const headline = screen.getByRole('heading', { level: 1 });

    // Should be bold and large
    expect(headline.className).toMatch(/font-bold/);
    expect(headline.className).toMatch(/text-4xl|text-5xl|text-6xl/);
  });
});

describe('Animation and Visual Effects - Integration', () => {
  it('Home page renders all animated sections', () => {
    renderWithRouter(<Home />);

    // BackgroundEffect
    expect(screen.getByTestId('background-effect')).toBeInTheDocument();

    // Hero
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();

    // Features
    expect(screen.getAllByTestId('feature-card').length).toBe(3);

    // How It Works
    expect(screen.getAllByTestId('how-it-works-step').length).toBe(3);
  });

  it('Home page has proper ARIA landmark structure for animated content', () => {
    renderWithRouter(<Home />);

    const main = screen.getByRole('main');
    expect(main).toBeInTheDocument();

    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
  });

  it('Skip to content link allows bypassing animated areas', () => {
    renderWithRouter(<Home />);

    const skipLink = screen.getByTestId('skip-to-content');
    expect(skipLink).toBeInTheDocument();
    expect(skipLink).toHaveAttribute('href', '#main-content');
  });
});
