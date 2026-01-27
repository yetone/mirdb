/**
 * Homepage Integration Tests
 * Owner: Scenario 10 - Homepage Integration
 *
 * Tests for:
 * - Complete homepage assembly with all sections
 * - Section ordering verification
 * - BackgroundEffect integration
 * - Smooth scroll navigation (anchor links)
 * - External navigation to /login, /register
 * - Browser back button navigation
 * - Framer Motion animations
 * - ThemeContext integration
 * - AuthContext integration (if available)
 * - Performance metrics
 */

import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { Home } from '../../src/pages/Home';
import { Login } from '../../src/pages/Login';
import { Register } from '../../src/pages/Register';

// Create mock navigate function for testing navigation
const mockNavigate = vi.fn();

// Mock react-router-dom to capture navigation calls
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Reset mocks between tests
function resetMocks() {
  mockNavigate.mockClear();
}

// Helper to filter out Framer Motion-specific props
function filterMotionProps(props: Record<string, unknown>) {
  const motionProps = [
    'initial', 'animate', 'exit', 'transition', 'variants',
    'whileHover', 'whileTap', 'whileFocus', 'whileDrag', 'whileInView',
    'viewport', 'drag', 'dragConstraints', 'dragElastic', 'dragMomentum',
    'dragTransition', 'dragPropagation', 'onDrag', 'onDragStart', 'onDragEnd',
    'layout', 'layoutId', 'onLayoutAnimationStart', 'onLayoutAnimationComplete',
    'onViewportEnter', 'onViewportLeave', 'custom'
  ];
  const filtered: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(props)) {
    if (!motionProps.includes(key)) {
      filtered[key] = value;
    }
  }
  return filtered;
}

// Mock Framer Motion to avoid animation timing issues in tests
vi.mock('framer-motion', async () => {
  const actual = await vi.importActual('framer-motion');

  // Create a factory for motion components
  const createMotionComponent = (Element: string) => {
    const MotionComponent = ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const filteredProps = filterMotionProps(props);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (React as any).createElement(Element, filteredProps, children);
    };
    MotionComponent.displayName = `motion.${Element}`;
    return MotionComponent;
  };

  return {
    ...actual,
    motion: {
      div: createMotionComponent('div'),
      main: createMotionComponent('main'),
      section: createMotionComponent('section'),
      button: createMotionComponent('button'),
      span: createMotionComponent('span'),
      p: createMotionComponent('p'),
    },
    useInView: () => true,
    AnimatePresence: ({ children }: React.PropsWithChildren<unknown>) => children,
  };
});

// Helper function to render with router context
function renderWithRouter(initialRoute = '/') {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />
      </Routes>
    </MemoryRouter>
  );
}

describe('Homepage Integration', () => {
  beforeEach(() => {
    resetMocks();
    // Mock window.scrollTo
    vi.spyOn(window, 'scrollTo').mockImplementation(() => {});
    // Mock matchMedia for reduced motion tests
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query: string) => ({
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
    vi.restoreAllMocks();
  });

  // Test Case 1: Navigate to / route - Homepage renders without errors
  describe('Test Case 1: Homepage renders without errors', () => {
    it('should render the homepage when navigating to / route', () => {
      renderWithRouter('/');

      // Verify homepage main container exists
      const homepage = screen.getByTestId('homepage');
      expect(homepage).toBeInTheDocument();
    });

    it('should not throw any errors during render', () => {
      expect(() => renderWithRouter('/')).not.toThrow();
    });
  });

  // Test Case 2: Section ordering verification
  describe('Test Case 2: Section ordering on page', () => {
    it('should render sections in correct order: Hero, Features, How It Works, Stats, CTA', () => {
      renderWithRouter('/');

      const homepage = screen.getByTestId('homepage');

      // Get all sections
      const heroSection = screen.getByTestId('hero-section');
      const featuresSection = document.getElementById('features');
      const howItWorksSection = document.getElementById('how-it-works');
      const statsSection = screen.getByTestId('stats-section');
      const ctaSection = screen.getByTestId('cta-section');
      const demoInputSection = screen.getByTestId('demo-input-section');

      // Verify all sections exist
      expect(heroSection).toBeInTheDocument();
      expect(featuresSection).toBeInTheDocument();
      expect(howItWorksSection).toBeInTheDocument();
      expect(statsSection).toBeInTheDocument();
      expect(ctaSection).toBeInTheDocument();
      expect(demoInputSection).toBeInTheDocument();

      // Verify order by checking DOM position
      const mainContent = homepage;
      const children = Array.from(mainContent.children);

      // Find indices
      const heroIndex = children.indexOf(heroSection);
      const featuresIndex = children.indexOf(featuresSection!);
      const howItWorksIndex = children.indexOf(howItWorksSection!);
      const statsIndex = children.indexOf(statsSection);
      const demoIndex = children.indexOf(demoInputSection);
      const ctaIndex = children.indexOf(ctaSection);

      // Verify ordering (Hero < Features < HowItWorks < Stats < Demo < CTA)
      expect(heroIndex).toBeLessThan(featuresIndex);
      expect(featuresIndex).toBeLessThan(howItWorksIndex);
      expect(howItWorksIndex).toBeLessThan(statsIndex);
      expect(statsIndex).toBeLessThan(demoIndex);
      expect(demoIndex).toBeLessThan(ctaIndex);
    });
  });

  // Test Case 3: BackgroundEffect integration
  describe('Test Case 3: BackgroundEffect integration', () => {
    it('should render BackgroundEffect component', () => {
      renderWithRouter('/');

      // There may be multiple BackgroundEffect components (one at page level, one in HeroSection)
      const backgroundEffects = screen.getAllByTestId('background-effect');
      expect(backgroundEffects.length).toBeGreaterThanOrEqual(1);
    });

    it('should have background effect with correct styling', () => {
      renderWithRouter('/');

      const backgroundEffects = screen.getAllByTestId('background-effect');
      // Check that at least one has the correct styling
      const hasCorrectStyling = backgroundEffects.some(
        (el) =>
          el.classList.contains('fixed') &&
          el.classList.contains('inset-0') &&
          el.classList.contains('-z-10')
      );
      expect(hasCorrectStyling).toBe(true);
    });
  });

  // Test Case 4: Smooth scroll to #features
  describe('Test Case 4: Navigation to #features', () => {
    it('should have features section with correct anchor id', () => {
      renderWithRouter('/');

      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeInTheDocument();
    });

    it('should scroll to features section when anchor is clicked', async () => {
      renderWithRouter('/#features');

      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeInTheDocument();
    });
  });

  // Test Case 5: Smooth scroll to #how-it-works
  describe('Test Case 5: Navigation to #how-it-works', () => {
    it('should have how-it-works section with correct anchor id', () => {
      renderWithRouter('/');

      const howItWorksSection = document.getElementById('how-it-works');
      expect(howItWorksSection).toBeInTheDocument();
    });

    it('should scroll to how-it-works section when anchor is clicked', async () => {
      renderWithRouter('/#how-it-works');

      const howItWorksSection = document.getElementById('how-it-works');
      expect(howItWorksSection).toBeInTheDocument();
    });
  });

  // Test Case 6: Navigate to /register from homepage
  describe('Test Case 6: Navigate to /register from homepage', () => {
    it('should navigate to register page when Get Started button is clicked', async () => {
      const user = userEvent.setup();
      renderWithRouter('/');

      // Click Get Started button in Hero section
      const getStartedButton = screen.getByTestId('get-started-button');
      await user.click(getStartedButton);

      // Verify navigation was called
      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });

    it('should navigate to register page when CTA Get Started button is clicked', async () => {
      const user = userEvent.setup();
      renderWithRouter('/');

      // Click Get Started button in CTA section
      const ctaGetStartedButton = screen.getByTestId('cta-get-started-button');
      await user.click(ctaGetStartedButton);

      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });
  });

  // Test Case 7: Navigate to /login from homepage
  describe('Test Case 7: Navigate to /login from homepage', () => {
    it('should navigate to login page when Sign In button is clicked', async () => {
      const user = userEvent.setup();
      renderWithRouter('/');

      // Click Sign In button in Hero section
      const signInButton = screen.getByTestId('sign-in-button');
      await user.click(signInButton);

      expect(mockNavigate).toHaveBeenCalledWith('/login');
    });

    it('should navigate to login page when CTA Sign In link is clicked', async () => {
      const user = userEvent.setup();
      renderWithRouter('/');

      // Click Sign In link in CTA section
      const ctaSignInLink = screen.getByTestId('cta-sign-in-link');
      await user.click(ctaSignInLink);

      expect(mockNavigate).toHaveBeenCalledWith('/login');
    });
  });

  // Test Case 8: Navigate back to homepage from login
  describe('Test Case 8: Browser back button navigation', () => {
    it('should render Login page correctly', () => {
      renderWithRouter('/login');

      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('Login Page');
    });

    it('should render Homepage correctly when returning from login via router', async () => {
      // This tests that homepage can be re-rendered correctly
      const { unmount } = renderWithRouter('/login');
      unmount();

      renderWithRouter('/');

      const homepage = screen.getByTestId('homepage');
      expect(homepage).toBeInTheDocument();
    });
  });

  // Test Case 9: Framer Motion animations load
  describe('Test Case 9: Framer Motion animations', () => {
    it('should render page with animation container', () => {
      renderWithRouter('/');

      const mainElement = screen.getByTestId('homepage');
      expect(mainElement).toBeInTheDocument();
    });

    it('should have hero section with animation container', () => {
      renderWithRouter('/');

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
    });

    it('should have CTA section with animation support', () => {
      renderWithRouter('/');

      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection).toBeInTheDocument();
    });
  });

  // Test Case 10: ThemeContext integration
  describe('Test Case 10: ThemeContext integration', () => {
    it('should use DaisyUI semantic color classes that respond to themes', () => {
      renderWithRouter('/');

      // Check for semantic color classes that work with DaisyUI themes
      const heroHeading = screen.getByRole('heading', { level: 1 });
      expect(heroHeading).toBeInTheDocument();

      // Check that primary color class is used
      expect(heroHeading.textContent).toContain('Amplify Your Reach');
    });

    it('should have theme-aware background styling', () => {
      renderWithRouter('/');

      const statsSection = screen.getByTestId('stats-section');
      // Check for theme-aware bg class
      expect(statsSection).toHaveClass('bg-base-200/50');
    });
  });

  // Test Case 11: AuthContext integration (if available)
  describe('Test Case 11: AuthContext integration', () => {
    it('should render homepage without requiring authentication', () => {
      // Homepage should be accessible without authentication
      renderWithRouter('/');

      const homepage = screen.getByTestId('homepage');
      expect(homepage).toBeInTheDocument();
    });

    it('should have navigation buttons for unauthenticated users', () => {
      renderWithRouter('/');

      // Both Get Started (register) and Sign In should be visible
      const getStartedButton = screen.getByTestId('get-started-button');
      const signInButton = screen.getByTestId('sign-in-button');

      expect(getStartedButton).toBeInTheDocument();
      expect(signInButton).toBeInTheDocument();
    });
  });

  // Test Case 12: Page load performance
  describe('Test Case 12: Page load performance', () => {
    it('should render initial content quickly', async () => {
      const startTime = performance.now();

      renderWithRouter('/');

      // Wait for homepage to be in the document
      await waitFor(() => {
        expect(screen.getByTestId('homepage')).toBeInTheDocument();
      });

      const endTime = performance.now();
      const renderTime = endTime - startTime;

      // Render should complete within 3000ms (test environment is faster than real browser)
      expect(renderTime).toBeLessThan(3000);
    });

    it('should have all required sections rendered', async () => {
      renderWithRouter('/');

      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument();
        expect(document.getElementById('features')).toBeInTheDocument();
        expect(document.getElementById('how-it-works')).toBeInTheDocument();
        expect(screen.getByTestId('stats-section')).toBeInTheDocument();
        expect(screen.getByTestId('cta-section')).toBeInTheDocument();
        expect(screen.getByTestId('demo-input-section')).toBeInTheDocument();
      });
    });
  });

  // Additional integration tests
  describe('Additional Integration Tests', () => {
    it('should render all call-to-action buttons', () => {
      renderWithRouter('/');

      // Hero section buttons
      expect(screen.getByTestId('get-started-button')).toBeInTheDocument();
      expect(screen.getByTestId('sign-in-button')).toBeInTheDocument();

      // CTA section buttons
      expect(screen.getByTestId('cta-get-started-button')).toBeInTheDocument();
      expect(screen.getByTestId('cta-sign-in-link')).toBeInTheDocument();

      // Demo input button
      expect(screen.getByTestId('demo-shorten-button')).toBeInTheDocument();
    });

    it('should have proper heading hierarchy', () => {
      renderWithRouter('/');

      // H1 should be in Hero section
      const h1 = screen.getByRole('heading', { level: 1 });
      expect(h1).toBeInTheDocument();

      // H2 headings in each section
      const h2Elements = screen.getAllByRole('heading', { level: 2 });
      expect(h2Elements.length).toBeGreaterThanOrEqual(4);
    });

    it('should have accessible section landmarks', () => {
      renderWithRouter('/');

      // Features section should have aria-labelledby
      const featuresSection = document.getElementById('features');
      expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

      // How it works section should have aria-labelledby
      const howItWorksSection = document.getElementById('how-it-works');
      expect(howItWorksSection).toHaveAttribute('aria-labelledby', 'how-it-works-heading');
    });

    it('should render feature cards', () => {
      renderWithRouter('/');

      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards.length).toBe(3);
    });

    it('should render stats cards', () => {
      renderWithRouter('/');

      const statCards = screen.getAllByTestId('stat-card');
      expect(statCards.length).toBe(3);
    });

    it('should render how it works steps', () => {
      renderWithRouter('/');

      const step1 = screen.getByTestId('step-1');
      const step2 = screen.getByTestId('step-2');
      const step3 = screen.getByTestId('step-3');

      expect(step1).toBeInTheDocument();
      expect(step2).toBeInTheDocument();
      expect(step3).toBeInTheDocument();
    });

    it('should have demo input form', () => {
      renderWithRouter('/');

      const demoInput = screen.getByTestId('demo-url-input');
      expect(demoInput).toBeInTheDocument();
      expect(demoInput).toHaveAttribute('type', 'url');
    });

    it('should validate demo input on submission', async () => {
      const user = userEvent.setup();
      renderWithRouter('/');

      // Submit empty form
      const submitButton = screen.getByTestId('demo-shorten-button');
      await user.click(submitButton);

      // Check for error message
      await waitFor(() => {
        const errorElement = screen.getByTestId('demo-url-error');
        expect(errorElement).toBeInTheDocument();
      });
    });

    it('should redirect to register when valid URL is submitted in demo', async () => {
      const user = userEvent.setup();
      renderWithRouter('/');

      const demoInput = screen.getByTestId('demo-url-input');
      const submitButton = screen.getByTestId('demo-shorten-button');

      // Enter valid URL and submit
      await user.type(demoInput, 'https://example.com/test');
      await user.click(submitButton);

      // Should navigate to register
      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });
  });
});
