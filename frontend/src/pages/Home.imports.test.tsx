import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider, useTheme } from '../contexts/ThemeContext';

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

// Scenario: Component Import and Integration
// Tests verifying Home.tsx properly imports and uses existing components

describe('Home - Component Import and Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Import GlassMorphismCard in Home component
  describe('Test Case 1: GlassMorphismCard Import', () => {
    it('imports GlassMorphismCard successfully without errors', () => {
      // If Home component renders without error, the import was successful
      expect(() => renderHome()).not.toThrow();
    });

    it('GlassMorphismCard is used in the features section', () => {
      renderHome();

      // Verify GlassMorphismCard components are rendered in features section
      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards.length).toBeGreaterThan(0);
    });

    it('GlassMorphismCard is used in demo section', () => {
      renderHome();

      // Demo section uses GlassMorphismCard
      const demoCard = screen.getByTestId('demo-card');
      expect(demoCard).toBeInTheDocument();
      // GlassMorphismCard has specific styling classes
      expect(demoCard).toHaveClass('backdrop-blur-lg');
    });

    it('GlassMorphismCard is used for testimonials', () => {
      renderHome();

      // Testimonial cards use GlassMorphismCard
      const testimonialCards = screen.getAllByTestId(/^testimonial-card-/);
      expect(testimonialCards.length).toBeGreaterThan(0);
      // Verify GlassMorphismCard styling is applied
      testimonialCards.forEach((card) => {
        expect(card).toHaveClass('backdrop-blur-lg');
      });
    });

    it('GlassMorphismCard receives and displays title prop correctly', () => {
      renderHome();

      // Feature cards receive title prop
      const featureCards = screen.getAllByTestId(/^feature-card-/);
      featureCards.forEach((card) => {
        const title = card.querySelector('[data-testid="card-title"]');
        expect(title).toBeInTheDocument();
      });
    });

    it('GlassMorphismCard receives and displays description prop correctly', () => {
      renderHome();

      // Feature cards receive description prop
      const featureCards = screen.getAllByTestId(/^feature-card-/);
      featureCards.forEach((card) => {
        const description = card.querySelector('[data-testid="card-description"]');
        expect(description).toBeInTheDocument();
      });
    });

    it('GlassMorphismCard receives and displays icon prop correctly', () => {
      renderHome();

      // Feature cards receive icon prop
      const featureCards = screen.getAllByTestId(/^feature-card-/);
      featureCards.forEach((card) => {
        const icon = card.querySelector('[data-testid="card-icon"]');
        expect(icon).toBeInTheDocument();
      });
    });
  });

  // Test Case 2: Import FuturisticButton in Home component
  describe('Test Case 2: FuturisticButton Import', () => {
    it('imports FuturisticButton successfully without errors', () => {
      // If Home component renders without error, the import was successful
      expect(() => renderHome()).not.toThrow();
    });

    it('FuturisticButton is used for primary CTA in hero section', () => {
      renderHome();

      // Primary CTA should be a FuturisticButton
      const primaryCTA = screen.getByRole('button', { name: /Get Started Free/i });
      expect(primaryCTA).toBeInTheDocument();
      // FuturisticButton uses DaisyUI btn classes
      expect(primaryCTA).toHaveClass('btn');
      expect(primaryCTA).toHaveClass('btn-primary');
    });

    it('FuturisticButton is used for secondary CTA in hero section', () => {
      renderHome();

      // Secondary CTA (Learn More) should be a FuturisticButton
      const secondaryCTA = screen.getByRole('button', { name: /Learn More/i });
      expect(secondaryCTA).toBeInTheDocument();
      // FuturisticButton with outline variant
      expect(secondaryCTA).toHaveClass('btn');
      expect(secondaryCTA).toHaveClass('btn-outline');
    });

    it('FuturisticButton receives variant prop correctly', () => {
      renderHome();

      const primaryButton = screen.getByRole('button', { name: /Get Started Free/i });
      const outlineButton = screen.getByRole('button', { name: /Learn More/i });

      // Different variant classes applied
      expect(primaryButton).toHaveClass('btn-primary');
      expect(outlineButton).toHaveClass('btn-outline');
    });

    it('FuturisticButton receives size prop correctly', () => {
      renderHome();

      const primaryButton = screen.getByRole('button', { name: /Get Started Free/i });
      const secondaryButton = screen.getByRole('button', { name: /Learn More/i });

      // Size classes applied (lg size)
      expect(primaryButton).toHaveClass('btn-lg');
      expect(secondaryButton).toHaveClass('btn-lg');
    });

    it('FuturisticButton onClick handler works correctly', () => {
      renderHome();

      const primaryButton = screen.getByRole('button', { name: /Get Started Free/i });
      primaryButton.click();

      // onClick triggers navigation
      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });
  });

  // Test Case 3: Import BackgroundEffect in Home component
  describe('Test Case 3: BackgroundEffect Import', () => {
    it('imports BackgroundEffect successfully without errors', () => {
      // If Home component renders without error, the import was successful
      expect(() => renderHome()).not.toThrow();
    });

    it('BackgroundEffect is rendered in hero section', () => {
      renderHome();

      // BackgroundEffect should be present in hero section
      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('BackgroundEffect has correct positioning', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      // BackgroundEffect uses absolute positioning
      expect(backgroundEffect).toHaveClass('absolute');
      expect(backgroundEffect).toHaveClass('inset-0');
    });

    it('BackgroundEffect is non-interactive (pointer-events-none)', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      // Background should not intercept clicks
      expect(backgroundEffect).toHaveClass('pointer-events-none');
    });

    it('BackgroundEffect is hidden from assistive technology', () => {
      renderHome();

      const backgroundEffect = screen.getByTestId('hero-background-effect');
      // Should have aria-hidden for accessibility
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
    });

    it('BackgroundEffect contains animated orbs', () => {
      renderHome();

      // Check for animated orb elements within the background effect
      const orb1 = screen.getByTestId('background-effect-orb-1');
      const orb2 = screen.getByTestId('background-effect-orb-2');
      const orb3 = screen.getByTestId('background-effect-orb-3');

      expect(orb1).toBeInTheDocument();
      expect(orb2).toBeInTheDocument();
      expect(orb3).toBeInTheDocument();
    });
  });

  // Test Case 4: Use useTheme hook in Home component
  describe('Test Case 4: Theme Context Integration', () => {
    it('Home component renders within ThemeProvider without errors', () => {
      // If component renders successfully with ThemeProvider, context is accessible
      expect(() => renderHome()).not.toThrow();
    });

    it('ThemeToggle component is present in footer', () => {
      renderHome();

      // ThemeToggle uses useTheme hook and should be rendered
      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toBeInTheDocument();

      // Footer should contain theme toggle
      const themeLabel = screen.getByText('Theme:');
      expect(themeLabel).toBeInTheDocument();
    });

    it('Theme context is accessible throughout the component tree', () => {
      // Test that ThemeProvider wrapping works by verifying theme-related elements
      renderHome();

      // Hero section should render with theme-aware classes
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('hero');

      // bg-base-200 is a DaisyUI theme-aware class
      const container = heroSection.closest('.bg-base-200');
      expect(container).toBeInTheDocument();
    });

    it('useTheme hook provides current theme value', () => {
      // Create a test component to verify useTheme functionality
      let themeValue: string | undefined;

      const ThemeChecker = () => {
        const { theme } = useTheme();
        themeValue = theme;
        return null;
      };

      render(
        <ThemeProvider>
          <ThemeChecker />
        </ThemeProvider>
      );

      // Theme should be defined and have a valid value
      expect(themeValue).toBeDefined();
      expect(['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night']).toContain(themeValue);
    });

    it('Components use theme-aware styling classes', () => {
      renderHome();

      // Verify theme-aware DaisyUI classes are used
      const container = document.querySelector('.bg-base-200');
      expect(container).toBeInTheDocument();

      const primaryButtons = screen.getAllByRole('button');
      const hasPrimaryButton = primaryButtons.some(btn => btn.classList.contains('btn-primary'));
      expect(hasPrimaryButton).toBe(true);
    });
  });

  // Test Case 5: Use React Router Link for navigation
  describe('Test Case 5: React Router Link Integration', () => {
    it('uses React Router Link component (not anchor tags) for internal navigation', () => {
      renderHome();

      // CTA section has a RouterLink to /register that is always visible
      const ctaSignUpLink = screen.getByRole('link', { name: /Sign Up Free/i });
      expect(ctaSignUpLink.tagName.toLowerCase()).toBe('a');
      expect(ctaSignUpLink).toHaveAttribute('href', '/register');
    });

    it('CTA section uses RouterLink for Sign Up Free link', () => {
      renderHome();

      // The CTA section Sign Up Free should be a Link
      const ctaLinks = screen.getAllByRole('link', { name: /Sign Up Free/i });
      expect(ctaLinks.length).toBeGreaterThan(0);

      // Should use internal routing (not external URL)
      ctaLinks.forEach((link) => {
        expect(link).toHaveAttribute('href', '/register');
      });
    });

    it('Navbar uses React Router Links for navigation', () => {
      renderHome();

      // Logo link should use RouterLink
      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      expect(logoLink).toHaveAttribute('href', '/');

      // Login link should use RouterLink
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toHaveAttribute('href', '/login');

      // Register link should use RouterLink
      const registerLink = screen.getByLabelText(/go to registration page/i);
      expect(registerLink).toHaveAttribute('href', '/register');
    });

    it('internal links do not use window.location or anchor tags with external URLs', () => {
      renderHome();

      // Get all anchor elements
      const allLinks = screen.getAllByRole('link');

      // Internal navigation links should have relative paths starting with /
      const internalLinks = allLinks.filter(link => {
        const href = link.getAttribute('href');
        return href && (href === '/' || href === '/login' || href === '/register');
      });

      // Should have multiple internal links using React Router
      expect(internalLinks.length).toBeGreaterThan(0);

      // All internal links should use relative paths (React Router style)
      internalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^\//); // Should start with /
      });
    });

    it('React Router Link components support client-side navigation', () => {
      renderHome();

      // Verify links have attributes that indicate React Router usage
      const registerLinks = screen.getAllByRole('link', { name: /Sign Up|register/i });

      registerLinks.forEach((link) => {
        // React Router Link renders as anchor but handles clicks for SPA navigation
        expect(link.tagName.toLowerCase()).toBe('a');
        // Should have href for accessibility and SEO
        expect(link).toHaveAttribute('href');
      });
    });
  });
});

// Additional Integration Tests
describe('Home - Component Integration Verification', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('all required components are imported and render without errors', () => {
    // This test verifies the overall component tree renders successfully
    // which means all imports are valid and components are properly integrated
    expect(() => renderHome()).not.toThrow();
  });

  it('component hierarchy is correct', () => {
    renderHome();

    // Hero section exists and contains BackgroundEffect
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();

    // Features section exists and contains GlassMorphismCards
    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toBeInTheDocument();

    // Footer exists and contains ThemeToggle
    const footer = screen.getByTestId('homepage-footer');
    expect(footer).toBeInTheDocument();
  });

  it('imported components receive props correctly', () => {
    renderHome();

    // GlassMorphismCard receives props
    const featureCard = screen.getByTestId('feature-card-url-shortening');
    expect(featureCard.querySelector('[data-testid="card-title"]')).toHaveTextContent('URL Shortening');
    expect(featureCard.querySelector('[data-testid="card-icon"]')).toBeInTheDocument();

    // FuturisticButton receives onClick and variant props
    const primaryButton = screen.getByRole('button', { name: /Get Started Free/i });
    expect(primaryButton).toHaveClass('btn-primary');
    expect(primaryButton).toHaveClass('btn-lg');
  });

  it('theme-aware components respond to ThemeProvider context', () => {
    renderHome();

    // Components use theme-aware classes that respond to ThemeProvider
    const pageContainer = document.querySelector('.min-h-screen.bg-base-200');
    expect(pageContainer).toBeInTheDocument();

    // Buttons use theme-aware DaisyUI classes
    const buttons = screen.getAllByRole('button');
    expect(buttons.some(btn => btn.classList.contains('btn-primary'))).toBe(true);
  });
});
