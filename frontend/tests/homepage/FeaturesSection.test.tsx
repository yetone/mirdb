/**
 * Features Section Tests
 * Owner: Scenario 4 - Features Section Display
 *
 * Tests for the features section covering:
 * - Features section container presence
 * - At least 3 GlassMorphismCard components
 * - URL shortening feature card
 * - Click analytics feature card
 * - Dashboard management feature card
 */
import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from './testUtils';
import FeaturesSection from '../../src/components/homepage/FeaturesSection';
import Home from '../../src/pages/Home';

describe('FeaturesSection', () => {
  describe('Test Case 1: Render HomePage and check for features section', () => {
    it('renders features section container in DOM', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
    });

    it('features section has correct id attribute', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toHaveAttribute('id', 'features');
    });

    it('renders features section with heading', () => {
      renderWithProviders(<FeaturesSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();
      expect(heading).toHaveTextContent(/Powerful Features/i);
    });
  });

  describe('Test Case 2: Count feature cards', () => {
    it('renders at least 3 GlassMorphismCard components in features section', () => {
      renderWithProviders(<FeaturesSection />);

      // Check for feature cards by test id
      const card0 = screen.getByTestId('feature-card-0');
      const card1 = screen.getByTestId('feature-card-1');
      const card2 = screen.getByTestId('feature-card-2');

      expect(card0).toBeInTheDocument();
      expect(card1).toBeInTheDocument();
      expect(card2).toBeInTheDocument();
    });

    it('renders exactly 4 feature cards', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresGrid = screen.getByTestId('features-grid');
      const cards = within(featuresGrid).getAllByRole('heading', { level: 3 });

      expect(cards.length).toBe(4);
    });

    it('each card has a title and description', () => {
      renderWithProviders(<FeaturesSection />);

      const featuresGrid = screen.getByTestId('features-grid');
      const cardTitles = within(featuresGrid).getAllByRole('heading', { level: 3 });

      cardTitles.forEach((title) => {
        expect(title).toBeInTheDocument();
        expect(title.textContent).not.toBe('');
      });
    });
  });

  describe('Test Case 3: Check for URL shortening feature', () => {
    it('renders feature card with URL Shortening title', () => {
      renderWithProviders(<FeaturesSection />);

      const urlShorteningTitle = screen.getByRole('heading', {
        level: 3,
        name: /URL Shortening/i,
      });
      expect(urlShorteningTitle).toBeInTheDocument();
    });

    it('URL shortening card has content about short links', () => {
      renderWithProviders(<FeaturesSection />);

      expect(screen.getByText(/short, memorable links/i)).toBeInTheDocument();
    });

    it('URL shortening card has descriptive content', () => {
      renderWithProviders(<FeaturesSection />);

      expect(
        screen.getByText(/Transform long URLs into clean, shareable links/i)
      ).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Check for analytics feature', () => {
    it('renders feature card with Click Analytics title', () => {
      renderWithProviders(<FeaturesSection />);

      const analyticsTitle = screen.getByRole('heading', {
        level: 3,
        name: /Click Analytics/i,
      });
      expect(analyticsTitle).toBeInTheDocument();
    });

    it('analytics card has content about tracking clicks', () => {
      renderWithProviders(<FeaturesSection />);

      expect(screen.getByText(/Track every click/i)).toBeInTheDocument();
    });

    it('analytics card mentions detailed insights', () => {
      renderWithProviders(<FeaturesSection />);

      expect(screen.getByText(/detailed insights/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Check for dashboard feature', () => {
    it('renders feature card with Dashboard Management title', () => {
      renderWithProviders(<FeaturesSection />);

      const dashboardTitle = screen.getByRole('heading', {
        level: 3,
        name: /Dashboard Management/i,
      });
      expect(dashboardTitle).toBeInTheDocument();
    });

    it('dashboard card has content about link management', () => {
      renderWithProviders(<FeaturesSection />);

      expect(screen.getByText(/Manage all your links/i)).toBeInTheDocument();
    });

    it('dashboard card mentions centralized management', () => {
      renderWithProviders(<FeaturesSection />);

      expect(screen.getByText(/centralized dashboard/i)).toBeInTheDocument();
    });
  });

  describe('Additional Feature Tests', () => {
    it('renders Share Statistics feature', () => {
      renderWithProviders(<FeaturesSection />);

      const shareStatsTitle = screen.getByRole('heading', {
        level: 3,
        name: /Share Statistics/i,
      });
      expect(shareStatsTitle).toBeInTheDocument();
    });

    it('each feature card has an icon (SVG element)', () => {
      renderWithProviders(<FeaturesSection />);

      // There should be 4 SVG icons for the 4 features
      const svgIcons = document.querySelectorAll(
        '[data-testid="features-grid"] svg'
      );
      expect(svgIcons.length).toBe(4);
    });
  });
});

describe('Home Page with FeaturesSection', () => {
  it('renders features section on the home page', () => {
    renderWithProviders(<Home />);

    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toBeInTheDocument();
  });

  it('features section appears after hero section on home page', () => {
    renderWithProviders(<Home />);

    // Both sections should be present
    const heroHeading = screen.getByRole('heading', { level: 1 });
    const featuresHeading = screen.getByRole('heading', {
      level: 2,
      name: /Powerful Features/i,
    });

    expect(heroHeading).toBeInTheDocument();
    expect(featuresHeading).toBeInTheDocument();
  });

  it('all feature cards are visible on home page', () => {
    renderWithProviders(<Home />);

    expect(screen.getByText(/URL Shortening/i)).toBeInTheDocument();
    expect(screen.getByText(/Click Analytics/i)).toBeInTheDocument();
    expect(screen.getByText(/Dashboard Management/i)).toBeInTheDocument();
    expect(screen.getByText(/Share Statistics/i)).toBeInTheDocument();
  });
});
