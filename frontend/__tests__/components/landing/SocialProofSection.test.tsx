/**
 * SocialProofSection Component Tests
 * Owner: Scenario 5 - Social Proof Section
 *
 * Test cases:
 * 1. Social proof section is present
 * 2. Stat counter for 'Links Created' is displayed
 * 3. Stat counter for 'Clicks Tracked' is displayed
 * 4. Stat counter for 'Happy Users' is displayed
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '../../test-utils';
import { SocialProofSection } from '../../../src/components/landing/SocialProofSection';

describe('SocialProofSection', () => {
  describe('Test Case 1: Social proof section is present', () => {
    it('should render the social proof section', () => {
      render(<SocialProofSection />);

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection).toBeInTheDocument();
    });

    it('should render the stats grid container', () => {
      render(<SocialProofSection />);

      const statsGrid = screen.getByTestId('stats-grid');
      expect(statsGrid).toBeInTheDocument();
    });

    it('should render exactly 3 stat cards', () => {
      render(<SocialProofSection />);

      const statCards = screen.getAllByTestId(/^stat-card-\d$/);
      expect(statCards).toHaveLength(3);
    });
  });

  describe("Test Case 2: Stat counter for 'Links Created' is displayed", () => {
    it("should render the 'Links Created' label", () => {
      render(<SocialProofSection />);

      const label = screen.getByText('Links Created');
      expect(label).toBeInTheDocument();
    });

    it("should render a value for 'Links Created'", () => {
      render(<SocialProofSection />);

      // First stat card (index 0) should be Links Created
      const statValue = screen.getByTestId('stat-value-0');
      expect(statValue).toBeInTheDocument();
      expect(statValue.textContent).toBeTruthy();
    });

    it("should render an icon for 'Links Created'", () => {
      render(<SocialProofSection />);

      const statIcon = screen.getByTestId('stat-icon-0');
      expect(statIcon).toBeInTheDocument();

      // Check that the icon container has an SVG
      const svgElement = statIcon.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });
  });

  describe("Test Case 3: Stat counter for 'Clicks Tracked' is displayed", () => {
    it("should render the 'Clicks Tracked' label", () => {
      render(<SocialProofSection />);

      const label = screen.getByText('Clicks Tracked');
      expect(label).toBeInTheDocument();
    });

    it("should render a value for 'Clicks Tracked'", () => {
      render(<SocialProofSection />);

      // Second stat card (index 1) should be Clicks Tracked
      const statValue = screen.getByTestId('stat-value-1');
      expect(statValue).toBeInTheDocument();
      expect(statValue.textContent).toBeTruthy();
    });

    it("should render an icon for 'Clicks Tracked'", () => {
      render(<SocialProofSection />);

      const statIcon = screen.getByTestId('stat-icon-1');
      expect(statIcon).toBeInTheDocument();

      const svgElement = statIcon.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });
  });

  describe("Test Case 4: Stat counter for 'Happy Users' is displayed", () => {
    it("should render the 'Happy Users' label", () => {
      render(<SocialProofSection />);

      const label = screen.getByText('Happy Users');
      expect(label).toBeInTheDocument();
    });

    it("should render a value for 'Happy Users'", () => {
      render(<SocialProofSection />);

      // Third stat card (index 2) should be Happy Users
      const statValue = screen.getByTestId('stat-value-2');
      expect(statValue).toBeInTheDocument();
      expect(statValue.textContent).toBeTruthy();
    });

    it("should render an icon for 'Happy Users'", () => {
      render(<SocialProofSection />);

      const statIcon = screen.getByTestId('stat-icon-2');
      expect(statIcon).toBeInTheDocument();

      const svgElement = statIcon.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });
  });

  describe('Accessibility', () => {
    it('should have proper semantic structure', () => {
      render(<SocialProofSection />);

      // Should have a section element
      const section = screen.getByTestId('social-proof-section');
      expect(section.tagName).toBe('SECTION');

      // Should have a heading
      const heading = screen.getByRole('heading', { name: /Trusted by Thousands/i });
      expect(heading).toBeInTheDocument();
    });

    it('should have aria-labelledby on the section', () => {
      render(<SocialProofSection />);

      const section = screen.getByTestId('social-proof-section');
      expect(section).toHaveAttribute('aria-labelledby', 'social-proof-heading');
    });
  });

  describe('Responsive Layout', () => {
    it('should have grid layout classes for responsive design', () => {
      render(<SocialProofSection />);

      const grid = screen.getByTestId('stats-grid');
      expect(grid.className).toContain('grid');
      expect(grid.className).toContain('grid-cols-1');
      expect(grid.className).toContain('md:grid-cols-3');
    });
  });
});
