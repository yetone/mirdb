/**
 * Unit tests for Social Proof section.
 * Owner: Scenario 4 - Social Proof Section Implementation
 *
 * Test cases:
 * - Test Case 1: Render Social Proof section - Section displays customer logos or testimonials
 * - Test Case 2: Check customer logo images - Logos have proper alt text with company names
 * - Test Case 3: Verify testimonial card structure - Each testimonial includes quote, author name, role, and company
 * - Test Case 5: Verify statistics display - Statistics render with appropriate formatting and icons
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import {
  SocialProof,
  TestimonialCard,
  LogoGrid,
  StatisticsGrid,
  Logo,
  Statistic,
  SocialProofProps,
} from '../../../src/components/sections/SocialProof'
import type { Testimonial } from '../../../src/types'

const mockTestimonials: Testimonial[] = [
  {
    id: 'test-1',
    quote: 'This is an amazing product that changed our workflow.',
    author: 'John Doe',
    role: 'Engineering Manager',
    company: 'TechCorp',
    avatar: '/avatars/john.jpg',
  },
  {
    id: 'test-2',
    quote: 'Incredible tool for team collaboration.',
    author: 'Jane Smith',
    role: 'Product Lead',
    company: 'StartupXYZ',
    avatar: '/avatars/jane.jpg',
  },
  {
    id: 'test-3',
    quote: 'Best investment for our organization.',
    author: 'Bob Wilson',
    role: 'CEO',
    company: 'GlobalInc',
  },
]

const mockLogos: Logo[] = [
  { src: '/logos/company-a.svg', alt: 'Company A logo', company: 'Company A' },
  { src: '/logos/company-b.svg', alt: 'Company B logo', company: 'Company B' },
  { src: '/logos/company-c.svg', alt: 'Company C logo', company: 'Company C' },
]

const mockStatistics: Statistic[] = [
  { label: 'Active Users', value: '10,000+', icon: 'users' },
  { label: 'Countries', value: '50+', icon: 'globe' },
  { label: 'Tasks Completed', value: '1M+', icon: 'chart' },
  { label: 'Rating', value: '4.8/5', icon: 'star' },
]

describe('Social Proof Section', () => {
  // Test Case 1: Render Social Proof section - Section displays customer logos or testimonials
  describe('Test Case 1: Render Social Proof section', () => {
    it('renders social proof section with logos and testimonials', () => {
      render(
        <SocialProof testimonials={mockTestimonials} logos={mockLogos} />
      )

      const section = screen.getByTestId('social-proof-section')
      expect(section).toBeInTheDocument()

      // Check logos are displayed
      const logosContainer = screen.getByTestId('logos-container')
      expect(logosContainer).toBeInTheDocument()

      // Check testimonials are displayed
      const testimonialsContainer = screen.getByTestId('testimonials-container')
      expect(testimonialsContainer).toBeInTheDocument()
    })

    it('renders section with proper heading', () => {
      render(<SocialProof />)

      const heading = screen.getByTestId('social-proof-heading')
      expect(heading).toBeInTheDocument()
      expect(heading.tagName).toBe('H2')
      expect(heading).toHaveTextContent('Trusted by Teams Worldwide')
    })

    it('renders custom section title', () => {
      render(<SocialProof sectionTitle="Our Amazing Customers" />)

      const heading = screen.getByTestId('social-proof-heading')
      expect(heading).toHaveTextContent('Our Amazing Customers')
    })

    it('renders section description', () => {
      render(<SocialProof sectionDescription="Custom description text" />)

      const description = screen.getByTestId('social-proof-description')
      expect(description).toBeInTheDocument()
      expect(description).toHaveTextContent('Custom description text')
    })

    it('renders default logos when no logos prop provided', () => {
      render(<SocialProof />)

      const logoItems = screen.getAllByTestId('logo-item')
      expect(logoItems.length).toBeGreaterThan(0)
    })

    it('renders default testimonials when no testimonials prop provided', () => {
      render(<SocialProof />)

      const testimonialCards = screen.getAllByTestId('testimonial-card')
      expect(testimonialCards.length).toBeGreaterThan(0)
    })

    it('section has proper aria-labelledby attribute', () => {
      render(<SocialProof />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveAttribute('aria-labelledby', 'social-proof-heading')
    })

    it('section has proper id for navigation', () => {
      render(<SocialProof />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveAttribute('id', 'social-proof')
    })
  })

  // Test Case 2: Check customer logo images - Logos have proper alt text with company names
  describe('Test Case 2: Check customer logo images', () => {
    it('logos have proper alt text with company names', () => {
      render(<SocialProof logos={mockLogos} />)

      mockLogos.forEach((logo) => {
        const img = screen.getByAltText(logo.alt)
        expect(img).toBeInTheDocument()
        expect(img).toHaveAttribute('alt', logo.alt)
      })
    })

    it('logo grid has proper role and aria-label', () => {
      render(<SocialProof logos={mockLogos} />)

      const logoGrid = screen.getByTestId('logo-grid')
      expect(logoGrid).toHaveAttribute('role', 'list')
      expect(logoGrid).toHaveAttribute('aria-label', 'Trusted by these companies')
    })

    it('logo items have listitem role', () => {
      render(<SocialProof logos={mockLogos} />)

      const listItems = within(screen.getByTestId('logo-grid')).getAllByRole('listitem')
      expect(listItems).toHaveLength(mockLogos.length)
    })

    it('logos display grayscale by default with hover effect class', () => {
      render(<SocialProof logos={mockLogos} />)

      const logoItems = screen.getAllByTestId('logo-item')
      logoItems.forEach((item) => {
        expect(item).toHaveClass('grayscale')
        expect(item).toHaveClass('hover:grayscale-0')
      })
    })

    it('logo images have lazy loading attribute', () => {
      render(<SocialProof logos={mockLogos} />)

      const images = screen.getAllByRole('img')
      const logoImages = images.filter((img) =>
        mockLogos.some((logo) => img.getAttribute('alt') === logo.alt)
      )

      logoImages.forEach((img) => {
        expect(img).toHaveAttribute('loading', 'lazy')
      })
    })
  })

  // Test Case 3: Verify testimonial card structure
  describe('Test Case 3: Verify testimonial card structure', () => {
    it('each testimonial includes quote, author name, role, and company', () => {
      render(<SocialProof testimonials={mockTestimonials} />)

      const testimonialCards = screen.getAllByTestId('testimonial-card')

      testimonialCards.forEach((card, index) => {
        const quote = within(card).getByTestId('testimonial-quote')
        const author = within(card).getByTestId('testimonial-author')
        const role = within(card).getByTestId('testimonial-role')
        const company = within(card).getByTestId('testimonial-company')

        expect(quote).toBeInTheDocument()
        expect(quote).toHaveTextContent(mockTestimonials[index].quote)

        expect(author).toBeInTheDocument()
        expect(author).toHaveTextContent(mockTestimonials[index].author)

        expect(role).toBeInTheDocument()
        expect(role).toHaveTextContent(mockTestimonials[index].role)

        expect(company).toBeInTheDocument()
        expect(company).toHaveTextContent(mockTestimonials[index].company)
      })
    })

    it('testimonial card has proper aria-labelledby attribute', () => {
      render(<TestimonialCard testimonial={mockTestimonials[0]} />)

      const card = screen.getByTestId('testimonial-card')
      expect(card).toHaveAttribute(
        'aria-labelledby',
        `testimonial-author-${mockTestimonials[0].id}`
      )
    })

    it('testimonial card is keyboard focusable', () => {
      render(<TestimonialCard testimonial={mockTestimonials[0]} />)

      const card = screen.getByTestId('testimonial-card')
      expect(card).toHaveAttribute('tabIndex', '0')
    })

    it('testimonial card has hover transition classes', () => {
      render(<TestimonialCard testimonial={mockTestimonials[0]} />)

      const card = screen.getByTestId('testimonial-card')
      expect(card).toHaveClass('transition-all')
      expect(card).toHaveClass('duration-300')
    })

    it('quote is rendered inside blockquote element', () => {
      const { container } = render(<TestimonialCard testimonial={mockTestimonials[0]} />)

      const blockquote = container.querySelector('blockquote')
      expect(blockquote).toBeInTheDocument()

      const quote = within(blockquote!).getByTestId('testimonial-quote')
      expect(quote).toBeInTheDocument()
    })

    it('author is rendered as cite element', () => {
      render(<TestimonialCard testimonial={mockTestimonials[0]} />)

      const author = screen.getByTestId('testimonial-author')
      expect(author.tagName).toBe('CITE')
    })

    it('testimonials grid has role="list" for screen readers', () => {
      render(<SocialProof testimonials={mockTestimonials} />)

      const grid = screen.getByTestId('testimonials-grid')
      expect(grid).toHaveAttribute('role', 'list')
    })

    it('each testimonial wrapper has role="listitem"', () => {
      render(<SocialProof testimonials={mockTestimonials} />)

      const listItems = within(screen.getByTestId('testimonials-grid')).getAllByRole(
        'listitem'
      )
      expect(listItems).toHaveLength(mockTestimonials.length)
    })

    it('testimonial with avatar displays avatar image', () => {
      render(<TestimonialCard testimonial={mockTestimonials[0]} />)

      const avatarContainer = screen.getByTestId('testimonial-avatar')
      expect(avatarContainer).toBeInTheDocument()

      const avatarImg = within(avatarContainer).getByRole('img')
      expect(avatarImg).toHaveAttribute(
        'alt',
        `${mockTestimonials[0].author}'s profile photo`
      )
    })

    it('testimonial without avatar does not display avatar container', () => {
      const testimonialWithoutAvatar = { ...mockTestimonials[2] }
      delete testimonialWithoutAvatar.avatar

      render(<TestimonialCard testimonial={testimonialWithoutAvatar} />)

      const avatarContainer = screen.queryByTestId('testimonial-avatar')
      expect(avatarContainer).not.toBeInTheDocument()
    })
  })

  // Test Case 5: Verify statistics display
  describe('Test Case 5: Verify statistics display', () => {
    it('statistics render with appropriate formatting and icons', () => {
      render(<SocialProof statistics={mockStatistics} />)

      const statisticItems = screen.getAllByTestId('statistic-item')
      expect(statisticItems).toHaveLength(mockStatistics.length)

      mockStatistics.forEach((stat) => {
        expect(screen.getByText(stat.value)).toBeInTheDocument()
        expect(screen.getByText(stat.label)).toBeInTheDocument()
      })
    })

    it('each statistic has an icon when icon property is provided', () => {
      render(<SocialProof statistics={mockStatistics} />)

      const iconContainers = screen.getAllByTestId('statistic-icon')
      expect(iconContainers.length).toBe(mockStatistics.length)

      iconContainers.forEach((container) => {
        const svg = container.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('statistic icons are decorative with aria-hidden', () => {
      render(<SocialProof statistics={mockStatistics} />)

      const iconContainers = screen.getAllByTestId('statistic-icon')
      iconContainers.forEach((container) => {
        expect(container).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('statistic value has proper styling', () => {
      render(<SocialProof statistics={mockStatistics} />)

      const values = screen.getAllByTestId('statistic-value')
      values.forEach((value) => {
        expect(value).toHaveClass('font-bold')
        expect(value).toHaveClass('text-primary-600')
      })
    })

    it('statistics grid has proper role and aria-label', () => {
      render(<SocialProof statistics={mockStatistics} />)

      const grid = screen.getByTestId('statistics-grid')
      expect(grid).toHaveAttribute('role', 'list')
      expect(grid).toHaveAttribute('aria-label', 'Key statistics')
    })

    it('each statistic item has listitem role', () => {
      render(<SocialProof statistics={mockStatistics} />)

      const listItems = within(screen.getByTestId('statistics-grid')).getAllByRole(
        'listitem'
      )
      expect(listItems).toHaveLength(mockStatistics.length)
    })

    it('statistics without icon property do not render icon container', () => {
      const statsWithoutIcons: Statistic[] = [
        { label: 'Users', value: '100+' },
        { label: 'Projects', value: '50+' },
      ]

      render(<SocialProof statistics={statsWithoutIcons} testimonials={[]} logos={[]} />)

      const iconContainers = screen.queryAllByTestId('statistic-icon')
      expect(iconContainers).toHaveLength(0)
    })
  })

  // LogoGrid Component Tests
  describe('LogoGrid Component', () => {
    it('renders logo grid with all logos', () => {
      render(<LogoGrid logos={mockLogos} />)

      const grid = screen.getByTestId('logo-grid')
      expect(grid).toBeInTheDocument()

      const logoItems = screen.getAllByTestId('logo-item')
      expect(logoItems).toHaveLength(mockLogos.length)
    })

    it('has proper responsive grid classes', () => {
      render(<LogoGrid logos={mockLogos} />)

      const grid = screen.getByTestId('logo-grid')
      expect(grid).toHaveClass('grid-cols-2')
      expect(grid).toHaveClass('sm:grid-cols-3')
      expect(grid).toHaveClass('md:grid-cols-5')
    })
  })

  // StatisticsGrid Component Tests
  describe('StatisticsGrid Component', () => {
    it('renders statistics grid with all statistics', () => {
      render(<StatisticsGrid statistics={mockStatistics} />)

      const grid = screen.getByTestId('statistics-grid')
      expect(grid).toBeInTheDocument()

      const statItems = screen.getAllByTestId('statistic-item')
      expect(statItems).toHaveLength(mockStatistics.length)
    })

    it('has proper responsive grid classes', () => {
      render(<StatisticsGrid statistics={mockStatistics} />)

      const grid = screen.getByTestId('statistics-grid')
      expect(grid).toHaveClass('grid-cols-2')
      expect(grid).toHaveClass('md:grid-cols-4')
    })
  })

  // Empty states
  describe('Empty states', () => {
    it('does not render logos container when logos array is empty', () => {
      render(<SocialProof logos={[]} />)

      const logosContainer = screen.queryByTestId('logos-container')
      expect(logosContainer).not.toBeInTheDocument()
    })

    it('does not render testimonials container when testimonials array is empty', () => {
      render(<SocialProof testimonials={[]} />)

      const testimonialsContainer = screen.queryByTestId('testimonials-container')
      expect(testimonialsContainer).not.toBeInTheDocument()
    })

    it('does not render statistics container when statistics array is empty', () => {
      render(<SocialProof statistics={[]} />)

      const statisticsContainer = screen.queryByTestId('statistics-container')
      expect(statisticsContainer).not.toBeInTheDocument()
    })
  })

  // Responsive layout tests
  describe('Responsive layout classes', () => {
    it('testimonials grid has proper responsive classes', () => {
      render(<SocialProof testimonials={mockTestimonials} />)

      const grid = screen.getByTestId('testimonials-grid')
      expect(grid).toHaveClass('grid-cols-1')
      expect(grid).toHaveClass('md:grid-cols-2')
      expect(grid).toHaveClass('lg:grid-cols-3')
    })

    it('section has proper padding classes', () => {
      render(<SocialProof />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveClass('py-16')
      expect(section).toHaveClass('md:py-24')
    })
  })
})
