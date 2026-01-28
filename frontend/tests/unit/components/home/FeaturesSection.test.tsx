/**
 * FeaturesSection Unit Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Test coverage:
 * - Component renders without errors
 * - Exactly 3 feature cards displayed
 * - URL Shortening feature present with correct content
 * - Analytics feature present with correct content
 * - Link Management feature present with correct content
 * - Each feature has an icon
 * - Glassmorphism styling applied
 * - Responsive layout (mobile/desktop)
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { FeaturesSection } from '@/components/home/FeaturesSection';

describe('FeaturesSection', () => {
  // Test Case 1: Component renders without throwing errors
  it('renders without throwing errors', () => {
    expect(() => render(<FeaturesSection />)).not.toThrow();
  });

  // Test Case 2: Exactly 3 feature cards are present
  it('displays exactly 3 feature cards', () => {
    render(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards).toHaveLength(3);
  });

  // Test Case 3: URL Shortening feature title is present
  it('displays URL Shortening feature title', () => {
    render(<FeaturesSection />);

    const urlShorteningHeading = screen.getByRole('heading', { name: /url shortening/i });
    expect(urlShorteningHeading).toBeInTheDocument();
  });

  // Test Case 4: URL Shortening feature description mentions short/memorable URLs
  it('displays URL Shortening description about creating short URLs', () => {
    render(<FeaturesSection />);

    const description = screen.getByText(/short.*memorable.*urls/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 5: Analytics feature title is present
  it('displays Analytics Dashboard feature title', () => {
    render(<FeaturesSection />);

    const analyticsHeading = screen.getByRole('heading', { name: /analytics/i });
    expect(analyticsHeading).toBeInTheDocument();
  });

  // Test Case 6: Analytics feature description mentions tracking clicks/statistics/referrers
  it('displays Analytics description about tracking clicks and statistics', () => {
    render(<FeaturesSection />);

    const description = screen.getByText(/track.*clicks|statistics|referrers/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 7: Link Management feature title is present
  it('displays Link Management feature title', () => {
    render(<FeaturesSection />);

    const linkManagementHeading = screen.getByRole('heading', { name: /link management/i });
    expect(linkManagementHeading).toBeInTheDocument();
  });

  // Test Case 8: Link Management feature description mentions organizing/managing URLs
  it('displays Link Management description about organizing URLs', () => {
    render(<FeaturesSection />);

    const description = screen.getByText(/organize.*manage.*urls/i);
    expect(description).toBeInTheDocument();
  });

  // Test Case 9: Each feature card contains an icon element (SVG)
  it('each feature card contains an icon element', () => {
    render(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards).toHaveLength(3);

    featureCards.forEach((card) => {
      const iconContainer = within(card).getByTestId('feature-icon');
      expect(iconContainer).toBeInTheDocument();

      // Check for SVG inside the icon container
      const svg = iconContainer.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });
  });

  // Test Case 10: Feature cards have glassmorphism styling (backdrop-blur, bg-opacity)
  it('feature cards have glassmorphism CSS classes', () => {
    render(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');

    featureCards.forEach((card) => {
      // Check for glassmorphism-related classes
      expect(card.className).toMatch(/backdrop-blur/);
      expect(card.className).toMatch(/bg-opacity/);
    });
  });

  // Test Case 11: Mobile viewport - single column layout (grid-cols-1)
  it('has single column layout classes for mobile', () => {
    render(<FeaturesSection />);

    const grid = screen.getByTestId('features-grid');
    expect(grid.className).toMatch(/grid-cols-1/);
  });

  // Test Case 12: Desktop viewport - 3-column grid layout (md:grid-cols-3)
  it('has 3-column grid layout classes for desktop', () => {
    render(<FeaturesSection />);

    const grid = screen.getByTestId('features-grid');
    expect(grid.className).toMatch(/md:grid-cols-3/);
  });

  // Additional tests for accessibility and semantic structure
  it('has a section with aria-labelledby', () => {
    render(<FeaturesSection />);

    const section = screen.getByRole('region', { name: /powerful features/i });
    expect(section).toHaveAttribute('aria-labelledby', 'features-heading');
  });

  it('has a level 2 heading for the section', () => {
    render(<FeaturesSection />);

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
    expect(heading.textContent).toMatch(/features/i);
  });

  it('feature cards use article elements for semantic structure', () => {
    render(<FeaturesSection />);

    const articles = screen.getAllByRole('article');
    expect(articles).toHaveLength(3);
  });

  it('icons have aria-hidden for accessibility', () => {
    render(<FeaturesSection />);

    const featureCards = screen.getAllByTestId('feature-card');

    featureCards.forEach((card) => {
      const svg = card.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });
});
