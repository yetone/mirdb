/**
 * Social Proof Section Unit Tests
 * Owner: Scenario 4 - Social Proof Section
 *
 * Tests for:
 * - Social proof section presence
 * - Testimonials with quotes and attributions
 * - Company logos with proper alt text
 * - User statistics display
 * - Blockquote semantics with cite attributes
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '../../../tests/setup/test-utils';
import { SocialProof } from './SocialProof';
import type { Testimonial, TrustedCompany, UserStatistics } from '../../types';

const mockTestimonials: Testimonial[] = [
  {
    quote: 'This product has transformed how we work. Highly recommended!',
    author: 'Jane Doe',
    role: 'CTO',
    company: 'TechCorp',
  },
  {
    quote: 'The best development platform we have ever used.',
    author: 'John Smith',
    role: 'Engineering Lead',
    company: 'DevStudio',
  },
];

const mockCompanies: TrustedCompany[] = [
  { name: 'TechCorp', logo: '/images/logos/techcorp.svg' },
  { name: 'DevStudio', logo: '/images/logos/devstudio.svg' },
  { name: 'InnovateCo', logo: '/images/logos/innovateco.svg' },
];

const mockStatistics: UserStatistics = {
  userCount: '10,000+',
  userLabel: 'Happy Users',
  projectCount: '50,000+',
  projectLabel: 'Projects Built',
  uptimePercent: '99.9%',
  uptimeLabel: 'Uptime',
};

describe('SocialProof Component', () => {
  // Test Case 1: Social proof section is present in the page
  it('renders social proof section in the page', () => {
    render(<SocialProof testimonials={mockTestimonials} />);

    const section = screen.getByRole('region', { name: /what our users say/i });
    expect(section).toBeInTheDocument();
    expect(section).toHaveAttribute('id', 'social-proof');
  });

  it('renders with custom section title', () => {
    render(
      <SocialProof
        testimonials={mockTestimonials}
        sectionTitle="Customer Stories"
      />
    );

    expect(screen.getByRole('heading', { name: /customer stories/i })).toBeInTheDocument();
  });

  // Test Case 2: At least one testimonial with quote text and author attribution exists
  describe('Testimonials', () => {
    it('renders testimonials with quote text and author attribution', () => {
      render(<SocialProof testimonials={mockTestimonials} />);

      // Check first testimonial quote
      expect(screen.getByText(/this product has transformed how we work/i)).toBeInTheDocument();

      // Check author attribution
      expect(screen.getByText('Jane Doe')).toBeInTheDocument();
      expect(screen.getByText(/cto, techcorp/i)).toBeInTheDocument();
    });

    it('renders multiple testimonials', () => {
      render(<SocialProof testimonials={mockTestimonials} />);

      // Check both testimonials are rendered
      expect(screen.getByText(/this product has transformed/i)).toBeInTheDocument();
      expect(screen.getByText(/the best development platform/i)).toBeInTheDocument();

      // Check both authors
      expect(screen.getByText('Jane Doe')).toBeInTheDocument();
      expect(screen.getByText('John Smith')).toBeInTheDocument();
    });

    it('renders testimonial without role or company gracefully', () => {
      const simpleTestimonial: Testimonial[] = [
        { quote: 'Great product!', author: 'Anonymous User' },
      ];

      render(<SocialProof testimonials={simpleTestimonial} />);

      expect(screen.getByText(/great product/i)).toBeInTheDocument();
      expect(screen.getByText('Anonymous User')).toBeInTheDocument();
    });
  });

  // Test Case 3: Trusted company logos section exists with proper alt text
  describe('Trust Logos', () => {
    it('renders trusted company logos with proper alt text', () => {
      render(
        <SocialProof
          testimonials={mockTestimonials}
          companies={mockCompanies}
        />
      );

      // Check for logos section
      expect(screen.getByText(/trusted by industry leaders/i)).toBeInTheDocument();

      // Check each logo has proper alt text
      mockCompanies.forEach((company) => {
        const logo = screen.getByAltText(`${company.name} logo`);
        expect(logo).toBeInTheDocument();
        expect(logo).toHaveAttribute('src', company.logo);
      });
    });

    it('renders logos as a list with proper ARIA attributes', () => {
      render(
        <SocialProof
          testimonials={mockTestimonials}
          companies={mockCompanies}
        />
      );

      const logosList = screen.getByRole('list', { name: /trusted companies/i });
      expect(logosList).toBeInTheDocument();

      const listItems = within(logosList).getAllByRole('listitem');
      expect(listItems).toHaveLength(mockCompanies.length);
    });

    it('does not render logos section when companies array is empty', () => {
      render(<SocialProof testimonials={mockTestimonials} companies={[]} />);

      expect(screen.queryByText(/trusted by industry leaders/i)).not.toBeInTheDocument();
    });
  });

  // Test Case 4: User count or similar statistic is displayed
  describe('User Statistics', () => {
    it('displays user count and statistics', () => {
      render(
        <SocialProof
          testimonials={mockTestimonials}
          statistics={mockStatistics}
        />
      );

      // Check user count
      expect(screen.getByText('10,000+')).toBeInTheDocument();
      expect(screen.getByText('Happy Users')).toBeInTheDocument();

      // Check project count
      expect(screen.getByText('50,000+')).toBeInTheDocument();
      expect(screen.getByText('Projects Built')).toBeInTheDocument();

      // Check uptime
      expect(screen.getByText('99.9%')).toBeInTheDocument();
      expect(screen.getByText('Uptime')).toBeInTheDocument();
    });

    it('statistics have proper aria-labels for accessibility', () => {
      render(
        <SocialProof
          testimonials={mockTestimonials}
          statistics={mockStatistics}
        />
      );

      expect(screen.getByLabelText(/10,000\+ happy users/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/50,000\+ projects built/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/99\.9% uptime/i)).toBeInTheDocument();
    });

    it('does not render statistics section when not provided', () => {
      render(<SocialProof testimonials={mockTestimonials} />);

      expect(screen.queryByRole('group', { name: /user statistics/i })).not.toBeInTheDocument();
    });
  });

  // Test Case 5: Testimonials use proper blockquote elements with cite attributes
  describe('Blockquote Semantics', () => {
    it('testimonials use blockquote elements', () => {
      const { container } = render(<SocialProof testimonials={mockTestimonials} />);

      const blockquotes = container.querySelectorAll('blockquote');
      expect(blockquotes.length).toBe(mockTestimonials.length);
    });

    it('blockquotes have cite attributes when company is provided', () => {
      const { container } = render(<SocialProof testimonials={mockTestimonials} />);

      const blockquotes = container.querySelectorAll('blockquote');

      // First testimonial has company, should have cite attribute
      expect(blockquotes[0]).toHaveAttribute('cite', 'https://techcorp.com');

      // Second testimonial has company, should have cite attribute
      expect(blockquotes[1]).toHaveAttribute('cite', 'https://devstudio.com');
    });

    it('testimonials use cite element for author names', () => {
      const { container } = render(<SocialProof testimonials={mockTestimonials} />);

      const citeElements = container.querySelectorAll('cite');
      expect(citeElements.length).toBe(mockTestimonials.length);

      expect(citeElements[0]).toHaveTextContent('Jane Doe');
      expect(citeElements[1]).toHaveTextContent('John Smith');
    });

    it('testimonials without company do not have cite attribute on blockquote', () => {
      const testimonialWithoutCompany: Testimonial[] = [
        { quote: 'Great product!', author: 'User' },
      ];

      const { container } = render(
        <SocialProof testimonials={testimonialWithoutCompany} />
      );

      const blockquote = container.querySelector('blockquote');
      expect(blockquote).not.toHaveAttribute('cite');
    });
  });

  // Accessibility tests
  describe('Accessibility', () => {
    it('section has proper landmark and aria-labelledby', () => {
      render(<SocialProof testimonials={mockTestimonials} />);

      const section = screen.getByRole('region', { name: /what our users say/i });
      expect(section).toHaveAttribute('aria-labelledby', 'social-proof-title');
    });

    it('heading is properly associated with section', () => {
      render(<SocialProof testimonials={mockTestimonials} />);

      const heading = screen.getByRole('heading', { level: 2, name: /what our users say/i });
      expect(heading).toHaveAttribute('id', 'social-proof-title');
    });

    it('trust logos section has proper aria-labelledby', () => {
      render(
        <SocialProof
          testimonials={mockTestimonials}
          companies={mockCompanies}
        />
      );

      const trustSection = screen.getByRole('region', { name: /trusted by industry leaders/i });
      expect(trustSection).toHaveAttribute('aria-labelledby', 'trust-logos-title');
    });
  });

  // Integration test with all features
  describe('Full Integration', () => {
    it('renders complete social proof section with all elements', () => {
      render(
        <SocialProof
          testimonials={mockTestimonials}
          companies={mockCompanies}
          statistics={mockStatistics}
        />
      );

      // Section exists
      expect(screen.getByRole('region', { name: /what our users say/i })).toBeInTheDocument();

      // Statistics exist
      expect(screen.getByText('10,000+')).toBeInTheDocument();

      // Testimonials exist
      expect(screen.getByText(/this product has transformed/i)).toBeInTheDocument();

      // Logos exist
      expect(screen.getByAltText(/techcorp logo/i)).toBeInTheDocument();
    });
  });
});
