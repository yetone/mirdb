/**
 * SocialProofSection Tests
 * Owner: Scenario 5 - Social Proof Section
 *
 * Tests for the Social Proof Section component:
 * - Section container rendering
 * - Statistics display (user count, links count)
 * - Testimonial elements with quote and attribution
 * - Number formatting (e.g., '10K+' or '1,000,000')
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import {
  SocialProofSection,
  formatStatNumber,
} from '@/components/homepage/SocialProofSection';
import type { Statistic, Testimonial } from '@/types/homepage';

describe('SocialProofSection', () => {
  // Test Case 1: Social proof section container exists
  describe('Social proof section container', () => {
    it('renders the social proof section container element', () => {
      render(<SocialProofSection />);

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection).toBeInTheDocument();
    });

    it('has proper accessibility attributes', () => {
      render(<SocialProofSection />);

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection).toHaveAttribute(
        'aria-labelledby',
        'social-proof-heading'
      );

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveAttribute('id', 'social-proof-heading');
    });

    it('renders with section element', () => {
      render(<SocialProofSection />);

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection.tagName).toBe('SECTION');
    });

    it('accepts custom className', () => {
      render(<SocialProofSection className="custom-class" />);

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection).toHaveClass('custom-class');
    });
  });

  // Test Case 2: At least one statistic counter is displayed
  describe('Statistics display', () => {
    it('renders at least one statistic counter', () => {
      render(<SocialProofSection />);

      const statisticCounters = screen.getAllByTestId('statistic-counter');
      expect(statisticCounters.length).toBeGreaterThanOrEqual(1);
    });

    it('renders statistics container', () => {
      render(<SocialProofSection />);

      const statisticsContainer = screen.getByTestId('statistics-container');
      expect(statisticsContainer).toBeInTheDocument();
    });

    it('displays default statistics when none provided', () => {
      render(<SocialProofSection />);

      const statisticCounters = screen.getAllByTestId('statistic-counter');
      expect(statisticCounters).toHaveLength(3); // 3 default stats
    });

    it('renders custom statistics when provided', () => {
      const customStats: Statistic[] = [
        { id: 'custom-1', value: 5000, label: 'Custom Stat 1' },
        { id: 'custom-2', value: 10000, label: 'Custom Stat 2' },
      ];

      render(<SocialProofSection statistics={customStats} />);

      const statisticCounters = screen.getAllByTestId('statistic-counter');
      expect(statisticCounters).toHaveLength(2);
    });

    it('each statistic has a value display', () => {
      render(<SocialProofSection />);

      const statisticValues = screen.getAllByTestId('statistic-value');
      expect(statisticValues.length).toBeGreaterThan(0);

      statisticValues.forEach((value) => {
        expect(value.textContent).not.toBe('');
      });
    });

    it('each statistic has a label', () => {
      render(<SocialProofSection />);

      const statisticLabels = screen.getAllByTestId('statistic-label');
      expect(statisticLabels.length).toBeGreaterThan(0);

      statisticLabels.forEach((label) => {
        expect(label.textContent).not.toBe('');
      });
    });

    it('each statistic has an icon', () => {
      render(<SocialProofSection />);

      const statisticIcons = screen.getAllByTestId('statistic-icon');
      expect(statisticIcons.length).toBeGreaterThan(0);

      statisticIcons.forEach((iconContainer) => {
        expect(iconContainer).toHaveAttribute('aria-hidden', 'true');
        const svg = iconContainer.querySelector('svg');
        expect(svg).toBeInTheDocument();
      });
    });
  });

  // Test Case 3: Testimonials have quote text and attribution if implemented
  describe('Testimonials', () => {
    it('renders testimonials container when testimonials exist', () => {
      render(<SocialProofSection />);

      const testimonialsContainer = screen.getByTestId('testimonials-container');
      expect(testimonialsContainer).toBeInTheDocument();
    });

    it('renders testimonial cards', () => {
      render(<SocialProofSection />);

      const testimonialCards = screen.getAllByTestId('testimonial-card');
      expect(testimonialCards.length).toBeGreaterThanOrEqual(1);
    });

    it('each testimonial has quote text', () => {
      render(<SocialProofSection />);

      const testimonialQuotes = screen.getAllByTestId('testimonial-quote');
      expect(testimonialQuotes.length).toBeGreaterThan(0);

      testimonialQuotes.forEach((quote) => {
        expect(quote.textContent).not.toBe('');
        expect(quote.textContent!.length).toBeGreaterThan(10);
      });
    });

    it('each testimonial has attribution (author name)', () => {
      render(<SocialProofSection />);

      const testimonialAuthors = screen.getAllByTestId('testimonial-author');
      expect(testimonialAuthors.length).toBeGreaterThan(0);

      testimonialAuthors.forEach((author) => {
        expect(author.textContent).not.toBe('');
      });
    });

    it('each testimonial has attribution (role/title)', () => {
      render(<SocialProofSection />);

      const testimonialRoles = screen.getAllByTestId('testimonial-role');
      expect(testimonialRoles.length).toBeGreaterThan(0);

      testimonialRoles.forEach((role) => {
        expect(role.textContent).not.toBe('');
      });
    });

    it('renders custom testimonials when provided', () => {
      const customTestimonials: Testimonial[] = [
        {
          id: 'test-1',
          author: 'Test User',
          role: 'Developer',
          content: 'This is a test testimonial content.',
        },
      ];

      render(
        <SocialProofSection
          testimonials={customTestimonials}
          statistics={[{ id: 'stat', value: 100, label: 'Test' }]}
        />
      );

      const testimonialCards = screen.getAllByTestId('testimonial-card');
      expect(testimonialCards).toHaveLength(1);

      expect(screen.getByText('Test User')).toBeInTheDocument();
      expect(screen.getByText('Developer')).toBeInTheDocument();
    });

    it('does not render testimonials section when empty array provided', () => {
      render(<SocialProofSection testimonials={[]} />);

      const testimonialsContainer = screen.queryByTestId('testimonials-container');
      expect(testimonialsContainer).not.toBeInTheDocument();
    });
  });

  // Test Case 4: Large numbers use appropriate formatting
  describe('Number formatting', () => {
    it('formats numbers >= 1,000,000 with M+ suffix', () => {
      expect(formatStatNumber(1000000)).toBe('1M+');
      expect(formatStatNumber(2500000)).toBe('2.5M+');
      expect(formatStatNumber(150000000)).toBe('150M+');
    });

    it('formats numbers >= 1,000 with K+ suffix', () => {
      expect(formatStatNumber(1000)).toBe('1K+');
      expect(formatStatNumber(5000)).toBe('5K+');
      expect(formatStatNumber(50000)).toBe('50K+');
      expect(formatStatNumber(1500)).toBe('1.5K+');
    });

    it('does not add suffix for numbers < 1,000', () => {
      expect(formatStatNumber(999)).toBe('999');
      expect(formatStatNumber(100)).toBe('100');
      expect(formatStatNumber(1)).toBe('1');
    });

    it('can format without compact notation (comma-separated)', () => {
      expect(formatStatNumber(1000000, false)).toBe('1,000,000');
      expect(formatStatNumber(1234567, false)).toBe('1,234,567');
      expect(formatStatNumber(50000, false)).toBe('50,000');
    });

    it('displays formatted statistics in the component', () => {
      const stats: Statistic[] = [
        { id: 'users', value: 50000, label: 'Users' },
        { id: 'links', value: 2500000, label: 'Links' },
      ];

      render(<SocialProofSection statistics={stats} testimonials={[]} />);

      const valueElements = screen.getAllByTestId('statistic-value');

      // Check that large numbers are formatted with K+ or M+
      const values = valueElements.map((el) => el.textContent);
      expect(values).toContain('50K+');
      expect(values).toContain('2.5M+');
    });

    it('handles prefix and suffix in statistics', () => {
      const stats: Statistic[] = [
        { id: 'price', value: 99, label: 'Starting at', prefix: '$' },
        { id: 'uptime', value: 99, label: 'Uptime', suffix: '%' },
      ];

      render(<SocialProofSection statistics={stats} testimonials={[]} />);

      const valueElements = screen.getAllByTestId('statistic-value');
      const values = valueElements.map((el) => el.textContent);

      expect(values).toContain('$99');
      expect(values).toContain('99%');
    });
  });

  // Additional integration tests
  describe('SocialProofSection integration', () => {
    it('renders with proper grid layout classes for statistics', () => {
      render(<SocialProofSection />);

      const statisticsContainer = screen.getByTestId('statistics-container');
      expect(statisticsContainer).toHaveClass('grid', 'grid-cols-1', 'md:grid-cols-3');
    });

    it('has a section heading', () => {
      render(<SocialProofSection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveTextContent(/trusted/i);
    });

    it('has a section description', () => {
      render(<SocialProofSection />);

      const description = screen.getByText(/join the growing community/i);
      expect(description).toBeInTheDocument();
    });

    it('renders default testimonials with expected content', () => {
      render(<SocialProofSection />);

      // Check that default testimonials are rendered
      expect(screen.getByText(/Sarah Johnson/)).toBeInTheDocument();
      expect(screen.getByText(/Marketing Director/)).toBeInTheDocument();
    });

    it('renders default statistics with expected labels', () => {
      render(<SocialProofSection />);

      expect(screen.getByText('Active Users')).toBeInTheDocument();
      expect(screen.getByText('Links Shortened')).toBeInTheDocument();
      expect(screen.getByText('Total Clicks')).toBeInTheDocument();
    });
  });
});
