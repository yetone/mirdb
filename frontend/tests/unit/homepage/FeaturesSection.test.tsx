/**
 * Unit tests for FeaturesSection component
 * Owner: Scenario 5 - Features Section Display
 *
 * Tests:
 * 1. Four feature cards are rendered with correct titles
 * 2. All feature cards have icons
 * 3. All feature cards have titles and descriptions
 * 4. Accessibility attributes are present
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    h2: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h2 {...props}>{children}</h2>
    ),
  },
}));

describe('FeaturesSection', () => {
  describe('Test Case 1: Four feature cards are rendered', () => {
    it('should render all four feature cards with correct titles', () => {
      render(<FeaturesSection />);

      // Check for all four feature titles
      expect(screen.getByText('URL Shortening')).toBeInTheDocument();
      expect(screen.getByText('Click Analytics')).toBeInTheDocument();
      expect(screen.getByText('Geographic Insights')).toBeInTheDocument();
      expect(screen.getByText('Shareable Stats')).toBeInTheDocument();
    });

    it('should render the features section container', () => {
      render(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toBeInTheDocument();
    });

    it('should render the features grid', () => {
      render(<FeaturesSection />);

      const grid = screen.getByTestId('features-grid');
      expect(grid).toBeInTheDocument();
    });

    it('should render exactly four feature cards', () => {
      render(<FeaturesSection />);

      const cards = [
        screen.getByTestId('feature-card-url-shortening'),
        screen.getByTestId('feature-card-click-analytics'),
        screen.getByTestId('feature-card-geographic-insights'),
        screen.getByTestId('feature-card-shareable-stats'),
      ];

      expect(cards).toHaveLength(4);
      cards.forEach((card) => {
        expect(card).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 2: Each feature card has icon element', () => {
    it('should have an icon for URL Shortening feature', () => {
      render(<FeaturesSection />);

      const icon = screen.getByTestId('icon-url-shortening');
      expect(icon).toBeInTheDocument();
      expect(icon.tagName.toLowerCase()).toBe('svg');
    });

    it('should have an icon for Click Analytics feature', () => {
      render(<FeaturesSection />);

      const icon = screen.getByTestId('icon-click-analytics');
      expect(icon).toBeInTheDocument();
      expect(icon.tagName.toLowerCase()).toBe('svg');
    });

    it('should have an icon for Geographic Insights feature', () => {
      render(<FeaturesSection />);

      const icon = screen.getByTestId('icon-geographic-insights');
      expect(icon).toBeInTheDocument();
      expect(icon.tagName.toLowerCase()).toBe('svg');
    });

    it('should have an icon for Shareable Stats feature', () => {
      render(<FeaturesSection />);

      const icon = screen.getByTestId('icon-shareable-stats');
      expect(icon).toBeInTheDocument();
      expect(icon.tagName.toLowerCase()).toBe('svg');
    });

    it('should have icon containers for all features', () => {
      render(<FeaturesSection />);

      const iconContainers = [
        screen.getByTestId('feature-icon-url-shortening'),
        screen.getByTestId('feature-icon-click-analytics'),
        screen.getByTestId('feature-icon-geographic-insights'),
        screen.getByTestId('feature-icon-shareable-stats'),
      ];

      iconContainers.forEach((container) => {
        expect(container).toBeInTheDocument();
        // Each container should have an SVG icon inside
        const svg = container.querySelector('svg');
        expect(svg).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 3: Each feature card has title and description', () => {
    it('should have a non-empty title for URL Shortening', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-url-shortening');
      expect(title).toBeInTheDocument();
      expect(title.textContent).toBe('URL Shortening');
      expect(title.textContent!.length).toBeGreaterThan(0);
    });

    it('should have a non-empty description for URL Shortening', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-url-shortening');
      expect(description).toBeInTheDocument();
      expect(description.textContent!.length).toBeGreaterThan(0);
      expect(description.textContent).toContain('Create clean, memorable short links');
    });

    it('should have a non-empty title for Click Analytics', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-click-analytics');
      expect(title).toBeInTheDocument();
      expect(title.textContent).toBe('Click Analytics');
      expect(title.textContent!.length).toBeGreaterThan(0);
    });

    it('should have a non-empty description for Click Analytics', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-click-analytics');
      expect(description).toBeInTheDocument();
      expect(description.textContent!.length).toBeGreaterThan(0);
      expect(description.textContent).toContain('Monitor every click');
    });

    it('should have a non-empty title for Geographic Insights', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-geographic-insights');
      expect(title).toBeInTheDocument();
      expect(title.textContent).toBe('Geographic Insights');
      expect(title.textContent!.length).toBeGreaterThan(0);
    });

    it('should have a non-empty description for Geographic Insights', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-geographic-insights');
      expect(description).toBeInTheDocument();
      expect(description.textContent!.length).toBeGreaterThan(0);
      expect(description.textContent).toContain('Discover where your audience');
    });

    it('should have a non-empty title for Shareable Stats', () => {
      render(<FeaturesSection />);

      const title = screen.getByTestId('feature-title-shareable-stats');
      expect(title).toBeInTheDocument();
      expect(title.textContent).toBe('Shareable Stats');
      expect(title.textContent!.length).toBeGreaterThan(0);
    });

    it('should have a non-empty description for Shareable Stats', () => {
      render(<FeaturesSection />);

      const description = screen.getByTestId('feature-description-shareable-stats');
      expect(description).toBeInTheDocument();
      expect(description.textContent!.length).toBeGreaterThan(0);
      expect(description.textContent).toContain('Generate secure share tokens');
    });

    it('should have titles rendered as h3 elements', () => {
      render(<FeaturesSection />);

      const titles = screen.getAllByRole('heading', { level: 3 });
      expect(titles).toHaveLength(4);

      const expectedTitles = ['URL Shortening', 'Click Analytics', 'Geographic Insights', 'Shareable Stats'];
      titles.forEach((title, index) => {
        expect(title.textContent).toBe(expectedTitles[index]);
      });
    });
  });

  describe('Accessibility', () => {
    it('should have a section heading for screen readers', () => {
      render(<FeaturesSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();
      expect(heading.textContent).toBe('Key Features');
    });

    it('should have aria-labelledby on the section', () => {
      render(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
    });

    it('should have aria-hidden on decorative icons', () => {
      render(<FeaturesSection />);

      const icons = [
        screen.getByTestId('icon-url-shortening'),
        screen.getByTestId('icon-click-analytics'),
        screen.getByTestId('icon-geographic-insights'),
        screen.getByTestId('icon-shareable-stats'),
      ];

      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true');
      });
    });
  });

  describe('Content validation', () => {
    it('should display the section title', () => {
      render(<FeaturesSection />);

      expect(screen.getByText('Key Features')).toBeInTheDocument();
    });

    it('should have proper section id for anchor navigation', () => {
      render(<FeaturesSection />);

      const section = screen.getByTestId('features-section');
      expect(section).toHaveAttribute('id', 'features');
    });
  });
});
