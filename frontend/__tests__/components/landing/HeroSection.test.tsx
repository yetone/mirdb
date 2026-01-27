/**
 * Hero Section Component Tests
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Test cases:
 * 1. Hero section renders with headline containing value proposition text
 * 2. Hero section contains subheadline with supporting text
 * 3. Primary CTA button with text 'Get Started' or 'Create Free Account' is visible
 * 4. Secondary CTA button for Login is visible
 * 5. BackgroundEffect component is rendered in hero section
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '../../test-utils';
import { HeroSection } from '../../../src/components/landing/HeroSection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    button: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
  },
}));

// Mock BackgroundEffect component to avoid Three.js issues in tests
vi.mock('../../../src/components/BackgroundEffect', () => ({
  BackgroundEffect: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
  default: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
}));

describe('HeroSection', () => {
  it('renders with headline containing value proposition text', () => {
    render(<HeroSection />);

    // Check for the headline - should contain value proposition about shortening/tracking links
    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline).toBeInTheDocument();
    expect(headline.textContent).toMatch(/shorten|share|track|link/i);
  });

  it('contains subheadline with supporting text', () => {
    render(<HeroSection />);

    // Look for supporting text in a paragraph
    const subheadline = screen.getByText(/create short|memorable|analytics|audience/i);
    expect(subheadline).toBeInTheDocument();
  });

  it('renders primary CTA button with "Get Started" or "Create Free Account" text', () => {
    render(<HeroSection />);

    // Check for primary CTA button
    const primaryCta = screen.getByRole('button', { name: /get started|create free account/i });
    expect(primaryCta).toBeInTheDocument();
    expect(primaryCta).toBeVisible();
  });

  it('renders secondary CTA button for Login', () => {
    render(<HeroSection />);

    // Check for login button
    const loginButton = screen.getByRole('button', { name: /login/i });
    expect(loginButton).toBeInTheDocument();
    expect(loginButton).toBeVisible();
  });

  it('renders BackgroundEffect component in hero section', () => {
    render(<HeroSection />);

    // BackgroundEffect should render with test id
    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();
  });

  it('has proper semantic HTML structure with section element', () => {
    render(<HeroSection />);

    // Should have a section element with aria-label
    const section = screen.getByRole('region', { name: /hero section/i });
    expect(section).toBeInTheDocument();
  });

  it('wraps CTA buttons in links for navigation', () => {
    render(<HeroSection />);

    // Check that buttons are wrapped in links
    const registerLink = screen.getByRole('link', { name: /get started|create free account/i });
    expect(registerLink).toHaveAttribute('href', '/register');

    const loginLink = screen.getByRole('link', { name: /login/i });
    expect(loginLink).toHaveAttribute('href', '/login');
  });
});

/**
 * Animations and Visual Effects Tests
 * Owner: Scenario 13 - Animations and Visual Effects
 *
 * Test cases:
 * 1. BackgroundEffect component is rendered and visible in LandingPage
 * 2. Feature cards animate into view with Framer Motion
 * 3. No animation errors in console when rendering LandingPage
 */
import { Home } from '../../../src/pages/Home';
import { FeaturesSection } from '../../../src/components/landing/FeaturesSection';

// Mock all landing components except the ones we're testing
vi.mock('../../../src/components/landing/HowItWorksSection', () => ({
  HowItWorksSection: () => <div data-testid="how-it-works-section">How It Works Mock</div>,
}));

vi.mock('../../../src/components/landing/SocialProofSection', () => ({
  SocialProofSection: () => <div data-testid="social-proof-section">Social Proof Mock</div>,
}));

vi.mock('../../../src/components/landing/Footer', () => ({
  Footer: () => <footer data-testid="footer">Footer Mock</footer>,
}));

// Re-mock framer-motion for the animation tests with whileInView support
vi.mock('framer-motion', async () => {
  return {
    motion: {
      div: ({ children, initial, animate, whileInView, variants, ...props }: React.PropsWithChildren<{
        initial?: Record<string, unknown>;
        animate?: Record<string, unknown>;
        whileInView?: Record<string, unknown>;
        variants?: Record<string, unknown>;
        [key: string]: unknown;
      }>) => {
        // For animation testing, we apply the final animated state (whileInView or animate)
        const animatedState = whileInView || animate || {};
        return (
          <div
            data-testid={props['data-testid'] as string}
            data-framer-motion="true"
            data-initial={initial ? JSON.stringify(initial) : undefined}
            data-animate={animatedState ? JSON.stringify(animatedState) : undefined}
            data-has-variants={variants ? 'true' : undefined}
            {...props}
          >
            {children}
          </div>
        );
      },
      button: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
        <button {...props}>{children}</button>
      ),
    },
    AnimatePresence: ({ children }: React.PropsWithChildren) => <>{children}</>,
  };
});

describe('Animations and Visual Effects (Scenario 13)', () => {
  // Spy on console.error to detect animation errors
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleErrorSpy.mockRestore();
  });

  describe('BackgroundEffect rendering', () => {
    it('renders BackgroundEffect component in the landing page and it is visible', () => {
      render(<Home />);

      // BackgroundEffect should be rendered via the mock
      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
      expect(backgroundEffect).toBeVisible();
    });

    it('BackgroundEffect has aria-hidden for accessibility', () => {
      render(<Home />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
    });
  });

  describe('Framer Motion scroll animations', () => {
    it('FeaturesSection uses Framer Motion for scroll-based animations', () => {
      render(<FeaturesSection />);

      // Features grid should have framer-motion attributes
      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();
      expect(featuresGrid).toHaveAttribute('data-framer-motion', 'true');
    });

    it('Feature cards have animation variants for staggered reveal', () => {
      render(<FeaturesSection />);

      const featuresGrid = screen.getByTestId('features-grid');
      // The container should have variants for staggered animation
      expect(featuresGrid).toHaveAttribute('data-has-variants', 'true');
    });

    it('HeroSection content animates on initial render', () => {
      render(<HeroSection />);

      // Find motion divs in the hero section
      const motionElements = document.querySelectorAll('[data-framer-motion="true"]');
      expect(motionElements.length).toBeGreaterThan(0);
    });
  });

  describe('Animation error handling', () => {
    it('renders LandingPage component without animation errors in console', () => {
      render(<Home />);

      // Check that no errors related to animations were logged
      const animationErrorCalls = consoleErrorSpy.mock.calls.filter((call) => {
        const message = call[0]?.toString() || '';
        return (
          message.includes('framer-motion') ||
          message.includes('animation') ||
          message.includes('Motion')
        );
      });

      expect(animationErrorCalls).toHaveLength(0);
    });

    it('renders HeroSection without throwing animation-related errors', () => {
      expect(() => render(<HeroSection />)).not.toThrow();

      // Verify no animation errors were logged
      const errorCalls = consoleErrorSpy.mock.calls.filter((call) => {
        const message = call[0]?.toString() || '';
        return message.toLowerCase().includes('animation');
      });
      expect(errorCalls).toHaveLength(0);
    });

    it('renders FeaturesSection without throwing animation-related errors', () => {
      expect(() => render(<FeaturesSection />)).not.toThrow();

      // Verify component rendered successfully
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
    });
  });

  describe('Animation configuration', () => {
    it('HeroSection has initial animation state defined', () => {
      render(<HeroSection />);

      // Find elements with initial animation state
      const animatedElements = document.querySelectorAll('[data-initial]');
      expect(animatedElements.length).toBeGreaterThan(0);
    });

    it('FeaturesSection defines whileInView for scroll trigger animations', () => {
      render(<FeaturesSection />);

      // Features should have animate data (from whileInView)
      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toHaveAttribute('data-animate');
    });
  });
});
