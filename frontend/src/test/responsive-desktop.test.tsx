import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from '../pages/Home';
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

// Helper function to mock matchMedia for desktop viewport (1440px)
const mockDesktopViewport = () => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: 1440,
  });
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: 900,
  });

  window.matchMedia = vi.fn().mockImplementation((query: string) => ({
    matches: query.includes('min-width') && (query.includes('1024') || query.includes('lg')),
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
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

// Responsive Design - Desktop Tests (Scenario: REQ-7)
describe('Responsive Design - Desktop (REQ-7)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDesktopViewport();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Test Case 1: Render Home component at 1440px viewport width
  // Expected: Full navigation bar displayed with Login and Register visible
  describe('Test Case 1: Full navigation bar at desktop viewport (1440px)', () => {
    it('renders full navigation bar at desktop viewport', () => {
      renderHome();

      const navbar = screen.getByRole('navigation');
      expect(navbar).toBeInTheDocument();
    });

    it('Login button is visible in navbar at desktop viewport', () => {
      renderHome();

      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toBeVisible();
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('Register/Sign Up button is visible in navbar at desktop viewport', () => {
      renderHome();

      const signUpLink = screen.getByLabelText(/go to registration page/i);
      expect(signUpLink).toBeInTheDocument();
      expect(signUpLink).toBeVisible();
      expect(signUpLink).toHaveAttribute('href', '/register');
    });

    it('navbar-end contains Login and Sign Up links for desktop', () => {
      renderHome();

      const navbar = screen.getByRole('navigation');
      const navbarEnd = navbar.querySelector('.navbar-end');

      expect(navbarEnd).toBeInTheDocument();

      // Both Login and Sign Up should be in navbar-end
      const loginLink = navbarEnd?.querySelector('[aria-label="Go to login page"]');
      const signUpLink = navbarEnd?.querySelector('[aria-label="Go to registration page"]');

      expect(loginLink).toBeInTheDocument();
      expect(signUpLink).toBeInTheDocument();
    });

    it('desktop navigation shows Features and How It Works links', () => {
      renderHome();

      const navbar = screen.getByRole('navigation');
      const navbarCenter = navbar.querySelector('.navbar-center');

      expect(navbarCenter).toBeInTheDocument();
      expect(navbarCenter).toHaveClass('hidden');
      expect(navbarCenter).toHaveClass('lg:flex');

      // Features and How It Works links should be in the navbar-center
      const featuresLink = navbarCenter?.querySelector('a[href="#features"]');
      const howItWorksLink = navbarCenter?.querySelector('a[href="#how-it-works"]');

      expect(featuresLink).toBeInTheDocument();
      expect(howItWorksLink).toBeInTheDocument();
    });

    it('hamburger menu is hidden on desktop (has lg:hidden class)', () => {
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      expect(menuButton).toHaveClass('lg:hidden');

      // The dropdown container should also be hidden on large screens
      const dropdown = menuButton.closest('.dropdown');
      expect(dropdown).toHaveClass('lg:hidden');
    });

    it('navbar has appropriate desktop padding (lg:px-8)', () => {
      renderHome();

      const navbar = screen.getByRole('navigation');
      expect(navbar).toHaveClass('lg:px-8');
    });

    it('logo/brand is visible at desktop viewport', () => {
      renderHome();

      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toHaveTextContent('URLShort');
      expect(logoLink).toHaveAttribute('href', '/');
    });
  });

  // Test Case 2: Render feature cards at 1440px viewport
  // Expected: Feature cards display in 3-4 column grid layout
  describe('Test Case 2: Feature cards in 3-4 column grid layout at desktop viewport', () => {
    it('feature grid uses lg:grid-cols-4 for 4-column layout on large screens', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toHaveClass('grid');
      expect(featuresGrid).toHaveClass('lg:grid-cols-4');
    });

    it('feature grid has responsive breakpoints (grid-cols-1, md:grid-cols-2, lg:grid-cols-4)', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toHaveClass('grid-cols-1');
      expect(featuresGrid).toHaveClass('md:grid-cols-2');
      expect(featuresGrid).toHaveClass('lg:grid-cols-4');
    });

    it('renders exactly 4 feature cards for 4-column desktop layout', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards).toHaveLength(4);
    });

    it('all 4 feature cards are within the grid container', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        expect(featuresGrid.contains(card)).toBe(true);
      });
    });

    it('feature cards have consistent gap spacing (gap-6)', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toHaveClass('gap-6');
    });

    it('features section container has max-w-6xl for wide desktop displays', () => {
      renderHome();

      const featuresSection = screen.getByTestId('features-section');
      const maxWidthContainer = featuresSection.querySelector('.max-w-6xl');

      expect(maxWidthContainer).toBeInTheDocument();
    });

    it('features section container is centered with mx-auto', () => {
      renderHome();

      const featuresSection = screen.getByTestId('features-section');
      const centeredContainer = featuresSection.querySelector('.max-w-6xl.mx-auto');

      expect(centeredContainer).toBeInTheDocument();
    });

    it('each feature card has title, description, and icon', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        const title = card.querySelector('[data-testid="card-title"]');
        const description = card.querySelector('[data-testid="card-description"]');
        const icon = card.querySelector('[data-testid="card-icon"]');

        expect(title).toBeInTheDocument();
        expect(description).toBeInTheDocument();
        expect(icon).toBeInTheDocument();
      });
    });
  });

  // Test Case 3: Render hero section at 1440px viewport
  // Expected: Hero section uses appropriate width with side margins
  describe('Test Case 3: Hero section layout at desktop viewport', () => {
    it('hero section is rendered', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
    });

    it('hero section has min-h-[60vh] for appropriate height', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('min-h-[60vh]');
    });

    it('hero section uses hero class for proper layout', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('hero');
    });

    it('hero content container has max-w-2xl for appropriate width constraint', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      const heroContent = heroSection.querySelector('.hero-content');
      const contentContainer = heroContent?.querySelector('.max-w-2xl');

      expect(contentContainer).toBeInTheDocument();
    });

    it('hero content is centered with text-center', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      const heroContent = heroSection.querySelector('.hero-content');

      expect(heroContent).toHaveClass('text-center');
    });

    it('hero headline has appropriate font size (text-5xl)', () => {
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toHaveClass('text-5xl');
      expect(headline).toHaveClass('font-bold');
    });

    it('hero section has gradient background', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('bg-gradient-to-br');
    });

    it('hero CTA buttons are present and properly styled', () => {
      renderHome();

      const getStartedButton = screen.getByRole('button', { name: /get started free/i });
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i });

      expect(getStartedButton).toBeInTheDocument();
      expect(learnMoreButton).toBeInTheDocument();
    });

    it('hero CTAs container has flex layout with gap for desktop', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      const ctaContainer = heroSection.querySelector('.flex.gap-4.justify-center');

      expect(ctaContainer).toBeInTheDocument();
    });

    it('hero subheadline has appropriate styling for readability', () => {
      renderHome();

      const subheadline = screen.getByText(/Create short, memorable links/i);
      expect(subheadline).toHaveClass('text-xl');
      expect(subheadline).toHaveClass('mb-8');
    });
  });

  // Additional desktop-specific layout tests
  describe('Additional desktop responsive layout tests', () => {
    it('statistics grid uses md:grid-cols-4 for 4-column layout on desktop', () => {
      renderHome();

      const statisticsGrid = screen.getByTestId('statistics-grid');
      expect(statisticsGrid).toHaveClass('md:grid-cols-4');
    });

    it('testimonials grid uses md:grid-cols-3 for 3-column layout on desktop', () => {
      renderHome();

      const testimonialsGrid = screen.getByTestId('testimonials-grid');
      expect(testimonialsGrid).toHaveClass('md:grid-cols-3');
    });

    it('how it works section uses md:flex-row for horizontal layout on desktop', () => {
      renderHome();

      const howItWorksSection = document.getElementById('how-it-works');
      const stepsContainer = howItWorksSection?.querySelector('.md\\:flex-row');

      expect(stepsContainer).toBeInTheDocument();
    });

    it('demo section container has max-w-3xl for appropriate width', () => {
      renderHome();

      const demoSection = screen.getByTestId('demo-section');
      const maxWidthContainer = demoSection.querySelector('.max-w-3xl');

      expect(maxWidthContainer).toBeInTheDocument();
    });

    it('social proof section has max-w-6xl container for desktop width', () => {
      renderHome();

      const socialProofSection = screen.getByTestId('social-proof-section');
      const maxWidthContainer = socialProofSection.querySelector('.max-w-6xl');

      expect(maxWidthContainer).toBeInTheDocument();
    });

    it('all max-width containers are centered with mx-auto', () => {
      renderHome();

      const demoMaxWidth = screen.getByTestId('demo-section').querySelector('.max-w-3xl');
      const featuresMaxWidth = screen.getByTestId('features-section').querySelector('.max-w-6xl');
      const socialProofMaxWidth = screen.getByTestId('social-proof-section').querySelector('.max-w-6xl');

      expect(demoMaxWidth).toHaveClass('mx-auto');
      expect(featuresMaxWidth).toHaveClass('mx-auto');
      expect(socialProofMaxWidth).toHaveClass('mx-auto');
    });

    it('CTA section content has max-w-2xl constraint for desktop', () => {
      renderHome();

      const ctaSection = document.querySelector('section.bg-primary.text-primary-content');
      const maxWidthContainer = ctaSection?.querySelector('.max-w-2xl');

      expect(maxWidthContainer).toBeInTheDocument();
    });

    it('footer is rendered with footer-center for consistent layout', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toBeInTheDocument();
      expect(footer).toHaveClass('footer-center');
    });
  });
});
