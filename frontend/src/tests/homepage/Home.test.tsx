/**
 * Home Page Integration Tests
 * Owner: Scenario 5 - Homepage Integration
 *
 * Tests for the complete homepage including:
 * - All sections rendering
 * - Section order verification
 * - Navbar presence
 * - Framer Motion animation setup
 * - Barrel export validation
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { renderWithRouter } from './test-utils';
import Home from '../../pages/Home';
import App from '../../App';
import * as homepageExports from '../../components/homepage';

// Mock framer-motion to avoid animation timing issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div data-testid="motion-div" {...props}>
        {children}
      </div>
    ),
  },
  AnimatePresence: ({ children }: React.PropsWithChildren<unknown>) => <>{children}</>,
}));

describe('Home Page Integration', () => {
  describe('Test Case 1: Homepage renders with all sections visible', () => {
    it('should render the homepage with all sections', () => {
      renderWithRouter(<Home />);

      // Check homepage container exists
      const homepage = screen.getByTestId('homepage');
      expect(homepage).toBeInTheDocument();

      // Check all sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByText('Key Features')).toBeInTheDocument();
      expect(screen.getByTestId('analytics-preview-section')).toBeInTheDocument();
      expect(screen.getByTestId('cta-section')).toBeInTheDocument();
    });

    it('should render hero section with tagline', () => {
      renderWithRouter(<Home />);
      expect(screen.getByTestId('hero-tagline')).toHaveTextContent('SHORTEN. TRACK. GROW.');
    });

    it('should render features section with all 4 features', () => {
      renderWithRouter(<Home />);
      expect(screen.getByText('URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Click Analytics')).toBeInTheDocument();
      expect(screen.getByText('Geographic Insights')).toBeInTheDocument();
      expect(screen.getByText('Share Stats')).toBeInTheDocument();
    });

    it('should render analytics preview with charts', () => {
      renderWithRouter(<Home />);
      expect(screen.getByTestId('clicks-chart')).toBeInTheDocument();
      expect(screen.getByTestId('referrer-chart')).toBeInTheDocument();
      expect(screen.getByTestId('location-chart')).toBeInTheDocument();
      expect(screen.getByTestId('browser-chart')).toBeInTheDocument();
    });

    it('should render CTA section with call to action', () => {
      renderWithRouter(<Home />);
      expect(screen.getByTestId('cta-heading')).toHaveTextContent('Ready to supercharge your links?');
      expect(screen.getByText('Create Free Account')).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Sections appear in correct order', () => {
    it('should render sections in order: Hero, Features, Analytics, CTA', () => {
      renderWithRouter(<Home />);

      const homepage = screen.getByTestId('homepage');
      // Get only direct children with IDs (the motion.div wrappers)
      const directChildren = Array.from(homepage.children);
      const sectionIds = directChildren.map((child) => child.id);

      expect(sectionIds).toEqual(['hero', 'features', 'analytics', 'cta']);
    });

    it('should have hero section first in DOM order', () => {
      renderWithRouter(<Home />);

      const homepage = screen.getByTestId('homepage');
      const firstChild = homepage.firstElementChild;

      expect(firstChild?.id).toBe('hero');
    });

    it('should have CTA section last in DOM order', () => {
      renderWithRouter(<Home />);

      const homepage = screen.getByTestId('homepage');
      const lastChild = homepage.lastElementChild;

      expect(lastChild?.id).toBe('cta');
    });
  });

  describe('Test Case 3: Navbar is visible at top of page', () => {
    it('should render Navbar when viewing full App', () => {
      // App already includes BrowserRouter, so render without wrapper
      render(<App />);

      // Navbar contains the logo link
      expect(screen.getByText('URLShortener')).toBeInTheDocument();
    });

    it('should render login and signup links in Navbar', () => {
      render(<App />);

      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /sign up/i })).toBeInTheDocument();
    });

    it('should render theme toggle in Navbar', () => {
      render(<App />);

      // ThemeToggle component should be present - its aria-label changes based on current theme
      expect(screen.getByRole('button', { name: /switch to (light|dark) mode/i })).toBeInTheDocument();
    });
  });

  describe('Test Case 4: All sections are reachable by scrolling', () => {
    it('should have all sections with unique IDs for scroll navigation', () => {
      renderWithRouter(<Home />);

      const hero = document.getElementById('hero');
      const features = document.getElementById('features');
      const analytics = document.getElementById('analytics');
      const cta = document.getElementById('cta');

      expect(hero).toBeInTheDocument();
      expect(features).toBeInTheDocument();
      expect(analytics).toBeInTheDocument();
      expect(cta).toBeInTheDocument();
    });

    it('should have all sections as direct children of main', () => {
      renderWithRouter(<Home />);

      const homepage = screen.getByTestId('homepage');
      const children = homepage.children;

      // Each section should be a motion.div wrapper
      expect(children.length).toBe(4);
    });
  });

  describe('Test Case 5: Framer Motion animations are configured', () => {
    it('should wrap sections with motion.div for animations', () => {
      renderWithRouter(<Home />);

      // Our mock replaces motion.div with a regular div with data-testid="motion-div"
      const motionDivs = screen.getAllByTestId('motion-div');

      // Should have 4 motion.div wrappers (one for each section)
      expect(motionDivs.length).toBe(4);
    });

    it('should have initial and whileInView props for scroll animations', () => {
      // This test verifies the component structure includes animation config
      // The actual animation behavior is tested in e2e tests
      renderWithRouter(<Home />);

      const motionDivs = screen.getAllByTestId('motion-div');

      // Each motion div should have the initial and whileInView attributes
      motionDivs.forEach((div) => {
        expect(div).toHaveAttribute('initial', 'hidden');
        expect(div).toHaveAttribute('whileInView', 'visible');
      });
    });
  });

  describe('Test Case 6: No JavaScript errors', () => {
    it('should render without throwing errors', () => {
      expect(() => {
        renderWithRouter(<Home />);
      }).not.toThrow();
    });

    it('should render full App without throwing errors', () => {
      expect(() => {
        // App includes its own BrowserRouter, so render directly
        render(<App />);
      }).not.toThrow();
    });

    it('should handle homepage at root route', () => {
      // App includes its own router, so just render it
      render(<App />);

      expect(screen.getByTestId('homepage')).toBeInTheDocument();
    });
  });

  describe('Test Case 7: Barrel export validation', () => {
    it('should export HeroSection from index.ts', () => {
      expect(homepageExports.HeroSection).toBeDefined();
      expect(typeof homepageExports.HeroSection).toBe('function');
    });

    it('should export FeaturesSection from index.ts', () => {
      expect(homepageExports.FeaturesSection).toBeDefined();
      expect(typeof homepageExports.FeaturesSection).toBe('function');
    });

    it('should export AnalyticsPreview from index.ts', () => {
      expect(homepageExports.AnalyticsPreview).toBeDefined();
      expect(typeof homepageExports.AnalyticsPreview).toBe('function');
    });

    it('should export CTASection from index.ts', () => {
      expect(homepageExports.CTASection).toBeDefined();
      expect(typeof homepageExports.CTASection).toBe('function');
    });

    it('should have all 4 expected exports', () => {
      const expectedExports = ['HeroSection', 'FeaturesSection', 'AnalyticsPreview', 'CTASection'];
      const actualExports = Object.keys(homepageExports);

      expectedExports.forEach((exportName) => {
        expect(actualExports).toContain(exportName);
      });
    });
  });
});

describe('CTASection Component', () => {
  it('should render with proper structure', () => {
    renderWithRouter(<homepageExports.CTASection />);

    expect(screen.getByTestId('cta-section')).toBeInTheDocument();
    expect(screen.getByTestId('cta-heading')).toBeInTheDocument();
  });

  it('should have accessible heading', () => {
    renderWithRouter(<homepageExports.CTASection />);

    const section = screen.getByTestId('cta-section');
    expect(section).toHaveAttribute('aria-labelledby', 'cta-heading');
  });

  it('should have Create Free Account button', () => {
    renderWithRouter(<homepageExports.CTASection />);

    expect(screen.getByText('Create Free Account')).toBeInTheDocument();
  });

  it('should have Sign In link', () => {
    renderWithRouter(<homepageExports.CTASection />);

    expect(screen.getByText(/sign in/i)).toBeInTheDocument();
  });
});
