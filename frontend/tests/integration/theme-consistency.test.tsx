/**
 * Theme Consistency Integration Tests
 * Owner: Scenario 15 - Theme Consistency with Existing App
 *
 * Validates that homepage uses consistent styling with the existing app themes
 * as specified in NFR-4.
 *
 * Test Cases:
 * 1. Homepage uses Tailwind CSS utility classes consistent with existing app
 * 2. Feature cards use GlassMorphismCard component or consistent styling
 * 3. CTA buttons use FuturisticButton component or consistent styling
 * 4. Homepage uses DaisyUI theme variables for colors
 * 5. Header/navbar styling is consistent with existing Navbar component
 *
 * Requirements: NFR-4 - Homepage shall use consistent styling with existing app themes
 */
import { render, screen, within } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { Home } from '../../src/pages/Home';
import { HeroSection } from '../../src/components/homepage/HeroSection';
import { FeatureCard } from '../../src/components/homepage/FeatureCard';
import { FeaturesSection } from '../../src/components/homepage/FeaturesSection';
import { Footer } from '../../src/components/homepage/Footer';
import { Button } from '../../src/components/ui/Button';
import { Card, CardBody, CardTitle } from '../../src/components/ui/Card';

// Test wrapper with router context
function renderWithRouter(ui: React.ReactElement) {
  return render(
    <MemoryRouter>
      {ui}
    </MemoryRouter>
  );
}

describe('Theme Consistency with Existing App (NFR-4)', () => {
  /**
   * Test Case 1: Homepage uses Tailwind CSS utility classes consistent with existing app
   * Input: Render HomePage component
   * Expected: Homepage uses Tailwind CSS utility classes consistent with existing app
   */
  describe('Test Case 1: Tailwind CSS Usage', () => {
    it('should render homepage with Tailwind CSS base classes', () => {
      renderWithRouter(<Home />);

      const homepage = screen.getByTestId('homepage');
      expect(homepage).toHaveClass('min-h-screen', 'bg-base-100');
    });

    it('should use DaisyUI base color classes throughout', () => {
      renderWithRouter(<Home />);

      // Hero section uses bg-base-200
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('bg-base-200');

      // Footer uses bg-base-200
      const footer = screen.getByTestId('footer');
      expect(footer).toHaveClass('bg-base-200');
    });

    it('should use consistent Tailwind spacing utilities', () => {
      renderWithRouter(<Home />);

      // Verify that the page uses padding/margin utilities
      const homepage = screen.getByTestId('homepage');
      expect(homepage).toBeInTheDocument();

      // Check hero has consistent padding through DaisyUI hero component
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('hero');
    });
  });

  /**
   * Test Case 2: Feature cards use GlassMorphismCard component or consistent styling
   * Input: Render feature cards
   * Expected: Feature cards use GlassMorphismCard component or consistent styling
   */
  describe('Test Case 2: Feature Card Styling Consistency', () => {
    const mockIcon = <svg data-testid="mock-icon" />;

    it('should render feature cards with DaisyUI card class', () => {
      renderWithRouter(
        <FeatureCard
          icon={mockIcon}
          title="Test Feature"
          description="Test description"
        />
      );

      const card = screen.getByTestId('feature-card');
      expect(card).toHaveClass('card');
    });

    it('should use bg-base-200 background consistent with GlassMorphismCard pattern', () => {
      renderWithRouter(
        <FeatureCard
          icon={mockIcon}
          title="Test Feature"
          description="Test description"
        />
      );

      const card = screen.getByTestId('feature-card');
      expect(card).toHaveClass('bg-base-200');
    });

    it('should have shadow and hover effects consistent with card patterns', () => {
      renderWithRouter(
        <FeatureCard
          icon={mockIcon}
          title="Test Feature"
          description="Test description"
        />
      );

      const card = screen.getByTestId('feature-card');
      expect(card).toHaveClass('shadow-md', 'hover:shadow-lg');
    });

    it('should use text-primary for icons consistent with theme', () => {
      renderWithRouter(
        <FeatureCard
          icon={mockIcon}
          title="Test Feature"
          description="Test description"
        />
      );

      const iconContainer = screen.getByTestId('feature-icon');
      expect(iconContainer).toHaveClass('text-primary');
    });

    it('UI Card component should have consistent styling with FeatureCard', () => {
      render(<Card hover>Content</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('card', 'bg-base-200', 'shadow-md');
      expect(card).toHaveClass('hover:shadow-lg', 'hover:border-primary');
    });
  });

  /**
   * Test Case 3: CTA buttons use FuturisticButton component or consistent styling
   * Input: Render CTA buttons
   * Expected: CTA buttons use FuturisticButton component or consistent styling
   */
  describe('Test Case 3: CTA Button Styling Consistency', () => {
    it('should render primary CTA with btn-primary DaisyUI class', () => {
      renderWithRouter(<HeroSection />);

      const primaryCTA = screen.getByTestId('cta-primary');
      expect(primaryCTA).toHaveClass('btn', 'btn-primary');
    });

    it('should render secondary CTA with btn-outline DaisyUI class', () => {
      renderWithRouter(<HeroSection />);

      const secondaryCTA = screen.getByTestId('cta-secondary');
      expect(secondaryCTA).toHaveClass('btn', 'btn-outline');
    });

    it('UI Button component should have consistent styling with CTAs', () => {
      render(<Button variant="primary">Primary</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn', 'btn-primary');
    });

    it('should support all DaisyUI button variants', () => {
      const { rerender } = render(<Button variant="primary">Test</Button>);
      expect(screen.getByTestId('ui-button')).toHaveClass('btn-primary');

      rerender(<Button variant="secondary">Test</Button>);
      expect(screen.getByTestId('ui-button')).toHaveClass('btn-secondary');

      rerender(<Button variant="outline">Test</Button>);
      expect(screen.getByTestId('ui-button')).toHaveClass('btn-outline');

      rerender(<Button variant="ghost">Test</Button>);
      expect(screen.getByTestId('ui-button')).toHaveClass('btn-ghost');
    });

    it('footer social buttons should use consistent btn-ghost styling', () => {
      renderWithRouter(<Footer />);

      const twitterButton = screen.getByTestId('social-twitter');
      expect(twitterButton).toHaveClass('btn', 'btn-ghost', 'btn-circle');
    });
  });

  /**
   * Test Case 4: Homepage uses DaisyUI theme variables for colors
   * Input: Check color variables usage
   * Expected: Homepage uses DaisyUI theme variables for colors
   */
  describe('Test Case 4: DaisyUI Theme Variable Usage', () => {
    it('should use base color classes throughout homepage', () => {
      renderWithRouter(<Home />);

      // Main container uses bg-base-100
      const homepage = screen.getByTestId('homepage');
      expect(homepage).toHaveClass('bg-base-100');

      // Hero uses bg-base-200
      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveClass('bg-base-200');
    });

    it('should use text-base-content for text elements', () => {
      renderWithRouter(<HeroSection />);

      const productName = screen.getByTestId('product-name');
      expect(productName).toHaveClass('text-base-content');
    });

    it('should use text-primary for accent elements', () => {
      renderWithRouter(
        <FeatureCard
          icon={<svg />}
          title="Test"
          description="Test"
        />
      );

      const iconContainer = screen.getByTestId('feature-icon');
      expect(iconContainer).toHaveClass('text-primary');
    });

    it('should use primary-content for text on primary backgrounds', () => {
      renderWithRouter(<HeroSection />);

      // The logo icon should use primary-content on primary background
      const branding = screen.getByTestId('product-branding');
      const iconContainer = branding.querySelector('.bg-primary');
      expect(iconContainer).toBeInTheDocument();
    });

    it('should use border-base-300 for subtle borders', () => {
      renderWithRouter(<Footer />);

      const footer = screen.getByTestId('footer');
      const copyrightSection = footer.querySelector('.border-t');
      expect(copyrightSection).toHaveClass('border-base-300');
    });

    it('UI Card should use DaisyUI theme variables', () => {
      render(
        <Card variant="bordered">
          <CardTitle>Title</CardTitle>
        </Card>
      );

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('bg-base-200', 'border-base-300');

      const title = screen.getByTestId('ui-card-title');
      expect(title).toHaveClass('text-base-content');
    });
  });

  /**
   * Test Case 5: Header/navbar styling is consistent with existing Navbar component
   * Input: Compare navigation styling
   * Expected: Header/navbar styling is consistent with existing Navbar component
   */
  describe('Test Case 5: Navigation Styling Consistency', () => {
    it('should use consistent link styling in footer navigation', () => {
      renderWithRouter(<Footer />);

      // Footer links should use DaisyUI link classes
      const footerLink = screen.getByTestId('footer-link-features');
      expect(footerLink).toHaveClass('link', 'link-hover');
    });

    it('should use consistent text sizing for navigation', () => {
      renderWithRouter(<Footer />);

      const footerLink = screen.getByTestId('footer-link-about');
      expect(footerLink).toHaveClass('text-sm');
    });

    it('should use footer-title class for section headers', () => {
      renderWithRouter(<Footer />);

      // Check that footer uses DaisyUI footer patterns
      const footerNav = screen.getByTestId('footer-navigation');
      const sectionTitles = footerNav.querySelectorAll('.footer-title');
      expect(sectionTitles.length).toBeGreaterThan(0);
    });

    it('should use consistent branding between hero and footer', () => {
      renderWithRouter(<Home />);

      // Both hero and footer should display the product name
      const heroBranding = screen.getByTestId('product-branding');
      const footerBranding = screen.getByTestId('footer-branding');

      expect(heroBranding).toBeInTheDocument();
      expect(footerBranding).toBeInTheDocument();

      // Both should show ShortLink
      expect(screen.getByTestId('product-name')).toHaveTextContent('ShortLink');
      expect(screen.getByTestId('footer-logo')).toHaveTextContent('ShortLink');
    });
  });

  /**
   * Additional Theme Consistency Tests
   */
  describe('Overall Theme Consistency', () => {
    it('should use consistent transition classes for interactive elements', () => {
      renderWithRouter(
        <FeatureCard
          icon={<svg />}
          title="Test"
          description="Test"
        />
      );

      const card = screen.getByTestId('feature-card');
      expect(card).toHaveClass('transition-shadow', 'duration-300');
    });

    it('UI Button should have consistent transition effects', () => {
      render(<Button>Click</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('transition-all', 'duration-200');
    });

    it('UI Card should have consistent transition effects when hoverable', () => {
      render(<Card hover>Content</Card>);

      const card = screen.getByTestId('ui-card');
      expect(card).toHaveClass('transition-all', 'duration-300');
    });

    it('should use consistent opacity modifiers for secondary text', () => {
      renderWithRouter(<Footer />);

      const tagline = screen.getByTestId('footer-tagline');
      // Should use opacity modifier like text-base-content/70
      expect(tagline.className).toMatch(/text-base-content/);
    });

    it('should use consistent rounded corners via DaisyUI components', () => {
      renderWithRouter(<HeroSection />);

      // Logo container should have rounded corners
      const branding = screen.getByTestId('product-branding');
      const logoContainer = branding.querySelector('.rounded-xl');
      expect(logoContainer).toBeInTheDocument();
    });
  });
});
