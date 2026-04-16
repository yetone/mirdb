/**
 * FeaturesSection Component Tests
 * Owner: Scenario 3 - Features Section Display
 *
 * Test cases:
 * 1. Features section renders with minimum 4 feature cards
 * 2. URL Shortening feature card displays with icon, title, description
 * 3. Click Analytics feature card displays with icon, title, description
 * 4. User Dashboard feature card displays with icon, title, description
 * 5. Secure Authentication feature card displays with icon, title, description
 * 6. Feature cards use GlassMorphismCard component with proper styling
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FeaturesSection } from '@/components/homepage/FeaturesSection';

describe('FeaturesSection', () => {
  // Test Case 1: Features section renders with minimum 4 feature cards
  describe('renders with minimum 4 feature cards', () => {
    it('should render the features section', () => {
      render(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toBeInTheDocument();
    });

    it('should display at least 4 feature cards', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards.length).toBeGreaterThanOrEqual(4);
    });

    it('should have a features heading', () => {
      render(<FeaturesSection />);

      const heading = screen.getByRole('heading', { name: /powerful features/i });
      expect(heading).toBeInTheDocument();
    });
  });

  // Test Case 2: URL Shortening feature card
  describe('URL Shortening feature card', () => {
    it('should display the URL Shortening card', () => {
      render(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-url-shortening');
      expect(card).toBeInTheDocument();
    });

    it('should display URL Shortening title', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-url-shortening');
      expect(title).toHaveTextContent('URL Shortening');
    });

    it('should display URL Shortening description', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-url-shortening');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toBeTruthy();
    });

    it('should display URL Shortening icon', () => {
      render(<FeaturesSection />);

      const icon = screen.getByTestId('feature-icon-url-shortening');
      expect(icon).toBeInTheDocument();
    });
  });

  // Test Case 3: Click Analytics feature card
  describe('Click Analytics feature card', () => {
    it('should display the Click Analytics card', () => {
      render(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-click-analytics');
      expect(card).toBeInTheDocument();
    });

    it('should display Click Analytics title', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-click-analytics');
      expect(title).toHaveTextContent('Click Analytics');
    });

    it('should display Click Analytics description', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-click-analytics');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toBeTruthy();
    });

    it('should display Click Analytics icon', () => {
      render(<FeaturesSection />);

      const icon = screen.getByTestId('feature-icon-click-analytics');
      expect(icon).toBeInTheDocument();
    });
  });

  // Test Case 4: User Dashboard feature card
  describe('User Dashboard feature card', () => {
    it('should display the User Dashboard card', () => {
      render(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-user-dashboard');
      expect(card).toBeInTheDocument();
    });

    it('should display User Dashboard title', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-user-dashboard');
      expect(title).toHaveTextContent('User Dashboard');
    });

    it('should display User Dashboard description', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-user-dashboard');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toBeTruthy();
    });

    it('should display User Dashboard icon', () => {
      render(<FeaturesSection />);

      const icon = screen.getByTestId('feature-icon-user-dashboard');
      expect(icon).toBeInTheDocument();
    });
  });

  // Test Case 5: Secure Authentication feature card
  describe('Secure Authentication feature card', () => {
    it('should display the Secure Authentication card', () => {
      render(<FeaturesSection />);

      const card = screen.getByTestId('feature-card-secure-authentication');
      expect(card).toBeInTheDocument();
    });

    it('should display Secure Authentication title', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-secure-authentication');
      expect(title).toHaveTextContent('Secure Authentication');
    });

    it('should display Secure Authentication description', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-secure-authentication');
      expect(description).toBeInTheDocument();
      expect(description.textContent).toBeTruthy();
    });

    it('should display Secure Authentication icon', () => {
      render(<FeaturesSection />);

      const icon = screen.getByTestId('feature-icon-secure-authentication');
      expect(icon).toBeInTheDocument();
    });
  });

  // Test Case 6: GlassMorphismCard styling
  describe('GlassMorphismCard styling', () => {
    it('should render feature cards with GlassMorphismCard styling', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        // GlassMorphismCard applies backdrop-blur and bg-base-100 classes
        expect(card.className).toContain('backdrop-blur');
        expect(card.className).toContain('bg-base-100');
        expect(card.className).toContain('rounded-xl');
      });
    });

    it('should apply shadow styling to feature cards', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        expect(card.className).toContain('shadow-xl');
      });
    });

    it('should apply border styling to feature cards', () => {
      render(<FeaturesSection />);

      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        expect(card.className).toContain('border');
      });
    });
  });

  // Additional accessibility tests
  describe('accessibility', () => {
    it('should have proper section labeling', () => {
      render(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
    });

    it('should have icons with aria-hidden for screen readers', () => {
      render(<FeaturesSection />);

      const icons = screen.getAllByTestId(/^feature-icon-/);
      icons.forEach((iconContainer) => {
        const svg = iconContainer.querySelector('svg');
        expect(svg).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });
});
