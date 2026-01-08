import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
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

// Utility to mock viewport width via matchMedia
const mockViewport = (width: number) => {
  // Store the original matchMedia
  const originalMatchMedia = window.matchMedia;

  // Create a mock matchMedia that responds to breakpoint queries
  window.matchMedia = vi.fn().mockImplementation((query: string) => {
    // Parse common Tailwind breakpoint queries
    const minWidthMatch = query.match(/\(min-width:\s*(\d+)px\)/);
    const maxWidthMatch = query.match(/\(max-width:\s*(\d+)px\)/);

    let matches = false;

    if (minWidthMatch) {
      const minWidth = parseInt(minWidthMatch[1], 10);
      matches = width >= minWidth;
    } else if (maxWidthMatch) {
      const maxWidth = parseInt(maxWidthMatch[1], 10);
      matches = width <= maxWidth;
    }

    return {
      matches,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    };
  });

  // Also set innerWidth for components that check it directly
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });

  return () => {
    window.matchMedia = originalMatchMedia;
  };
};

const renderHome = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// Responsive Design Tests - Tablet Viewport (768px)
// Scenario: Responsive Design - Tablet (REQ-7)
describe('Home - Responsive Design - Tablet Viewport (768px)', () => {
  let restoreViewport: () => void;

  beforeEach(() => {
    vi.clearAllMocks();
    // Set tablet viewport (768px - md breakpoint in Tailwind)
    restoreViewport = mockViewport(768);
  });

  afterEach(() => {
    restoreViewport();
  });

  // Test Case 1: Render Home component at 768px viewport width - Layout adapts to tablet-appropriate sizing
  describe('Test Case 1: Home component renders correctly at 768px viewport', () => {
    it('renders Home component without errors at tablet viewport', () => {
      renderHome();

      // Verify the main page elements render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
      expect(screen.getByTestId('demo-section')).toBeInTheDocument();
    });

    it('hero section is visible at tablet viewport', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toBeVisible();
    });

    it('hero headline renders correctly at tablet viewport', () => {
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent('Shorten, Share, Track');
    });

    it('hero CTA buttons are visible at tablet viewport', () => {
      renderHome();

      expect(screen.getByRole('button', { name: /Get Started Free/i })).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Learn More/i })).toBeInTheDocument();
    });

    it('verifies viewport is set to tablet width (768px)', () => {
      expect(window.innerWidth).toBe(768);
    });
  });

  // Test Case 2: Feature cards display in 2-column or appropriate tablet grid
  describe('Test Case 2: Feature cards display appropriately at tablet viewport', () => {
    it('renders features grid at tablet viewport', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();
    });

    it('features grid has responsive classes for tablet layout', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      // The grid has md:grid-cols-2 which means 2 columns at 768px (md breakpoint)
      expect(featuresGrid).toHaveClass('md:grid-cols-2');
    });

    it('features grid has mobile-first single column class', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      // Should have grid-cols-1 for mobile baseline
      expect(featuresGrid).toHaveClass('grid-cols-1');
    });

    it('renders all 4 feature cards at tablet viewport', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards).toHaveLength(4);
    });

    it('each feature card renders at tablet viewport', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);
      featureCards.forEach((card) => {
        expect(card).toBeInTheDocument();
      });
    });

    it('feature card for URL Shortening renders at tablet', () => {
      renderHome();

      const urlCard = screen.getByTestId('feature-card-url-shortening');
      expect(urlCard).toBeInTheDocument();
    });

    it('feature card for Analytics Dashboard renders at tablet', () => {
      renderHome();

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard');
      expect(analyticsCard).toBeInTheDocument();
    });

    it('feature card for Geographic Insights renders at tablet', () => {
      renderHome();

      const geoCard = screen.getByTestId('feature-card-geographic-insights');
      expect(geoCard).toBeInTheDocument();
    });

    it('feature card for Share & Collaborate renders at tablet', () => {
      renderHome();

      const shareCard = screen.getByTestId('feature-card-share-collaborate');
      expect(shareCard).toBeInTheDocument();
    });
  });

  // Test Case 3: Hero section properly sized for tablet viewing
  describe('Test Case 3: Hero section displays correctly at tablet viewport', () => {
    it('hero section renders at tablet viewport', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
    });

    it('hero section has appropriate minimum height class', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      // Hero has min-h-[60vh] for proper sizing
      expect(heroSection).toHaveClass('min-h-[60vh]');
    });

    it('hero section has gradient background classes', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('bg-gradient-to-br');
    });

    it('hero headline is accessible at tablet viewport', () => {
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      // Check that it has styling classes for visibility
      expect(headline).toHaveClass('text-5xl', 'font-bold');
    });

    it('hero subheadline/value proposition renders at tablet', () => {
      renderHome();

      const subheadline = screen.getByText(/Create short, memorable links/i);
      expect(subheadline).toBeInTheDocument();
    });

    it('hero content container has max-width constraint for tablet', () => {
      renderHome();

      const heroContent = screen.getByTestId('hero-section').querySelector('.hero-content');
      expect(heroContent).toBeInTheDocument();

      // Check for max-w-2xl on the inner container
      const innerContainer = heroContent?.querySelector('.max-w-2xl');
      expect(innerContainer).toBeInTheDocument();
    });

    it('CTA buttons container uses flexbox for proper layout', () => {
      renderHome();

      const getStartedButton = screen.getByRole('button', { name: /Get Started Free/i });
      const buttonContainer = getStartedButton.parentElement;

      expect(buttonContainer).toHaveClass('flex', 'gap-4', 'justify-center');
    });
  });

  // Additional tablet-specific layout tests
  describe('Additional tablet layout verifications', () => {
    it('statistics grid has tablet-appropriate columns', () => {
      renderHome();

      const statisticsGrid = screen.getByTestId('statistics-grid');
      // Should have md:grid-cols-4 for tablet (4 columns)
      expect(statisticsGrid).toHaveClass('md:grid-cols-4');
      // And grid-cols-2 as mobile fallback
      expect(statisticsGrid).toHaveClass('grid-cols-2');
    });

    it('testimonials grid has tablet-appropriate columns', () => {
      renderHome();

      const testimonialsGrid = screen.getByTestId('testimonials-grid');
      // Should have md:grid-cols-3 for tablet
      expect(testimonialsGrid).toHaveClass('md:grid-cols-3');
    });

    it('demo section is visible at tablet viewport', () => {
      renderHome();

      const demoSection = screen.getByTestId('demo-section');
      expect(demoSection).toBeInTheDocument();
      expect(demoSection).toBeVisible();
    });

    it('demo section has appropriate max-width for tablet', () => {
      renderHome();

      const demoSection = screen.getByTestId('demo-section');
      const demoContainer = demoSection.querySelector('.max-w-3xl');
      expect(demoContainer).toBeInTheDocument();
    });

    it('social proof section is visible at tablet viewport', () => {
      renderHome();

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection).toBeInTheDocument();
      expect(socialProofSection).toBeVisible();
    });

    it('footer is visible at tablet viewport', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toBeInTheDocument();
      expect(footer).toBeVisible();
    });
  });

  // Navigation behavior at tablet viewport
  describe('Navigation at tablet viewport (768px)', () => {
    it('navbar renders at tablet viewport', () => {
      renderHome();

      const navbar = screen.getByRole('navigation', { name: /main navigation/i });
      expect(navbar).toBeInTheDocument();
    });

    it('login link is visible in navbar at tablet viewport', () => {
      renderHome();

      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toBeInTheDocument();
    });

    it('sign up link is visible in navbar at tablet viewport', () => {
      renderHome();

      const signUpLink = screen.getByLabelText(/go to registration page/i);
      expect(signUpLink).toBeInTheDocument();
    });

    it('logo/brand is visible at tablet viewport', () => {
      renderHome();

      const logo = screen.getByRole('link', { name: /go to homepage/i });
      expect(logo).toBeInTheDocument();
      expect(logo).toHaveTextContent('URLShort');
    });

    it('mobile menu button exists at tablet viewport (below lg breakpoint)', () => {
      renderHome();

      // Mobile menu button should exist (it's shown below lg:1024px)
      const menuButton = screen.getByLabelText(/open menu/i);
      expect(menuButton).toBeInTheDocument();
    });
  });
});

// Additional viewport variation tests
describe('Home - Responsive Design - Various Tablet Viewports', () => {
  let restoreViewport: () => void;

  afterEach(() => {
    if (restoreViewport) {
      restoreViewport();
    }
  });

  it('renders correctly at 768px (iPad portrait)', () => {
    restoreViewport = mockViewport(768);
    renderHome();

    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(window.innerWidth).toBe(768);
  });

  it('renders correctly at 820px (iPad Air portrait)', () => {
    restoreViewport = mockViewport(820);
    renderHome();

    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(window.innerWidth).toBe(820);
  });

  it('renders correctly at 834px (iPad Pro 11 portrait)', () => {
    restoreViewport = mockViewport(834);
    renderHome();

    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(window.innerWidth).toBe(834);
  });

  it('renders correctly at 912px (Surface Pro 7)', () => {
    restoreViewport = mockViewport(912);
    renderHome();

    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(window.innerWidth).toBe(912);
  });

  it('renders correctly at 1024px (iPad landscape - lg breakpoint)', () => {
    restoreViewport = mockViewport(1024);
    renderHome();

    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    expect(window.innerWidth).toBe(1024);
  });
});
