/**
 * Social proof section with testimonials and logos.
 * Owner: Scenario 4 - Social Proof Section Implementation
 *
 * Requirements:
 * - Customer logos (grayscale or color) (REQ-4)
 * - Testimonial cards (quote, name, role, company)
 * - Optional statistics/metrics display
 * - Responsive layout
 */

import type { Testimonial } from '../../types'
import {
  UserGroupIcon,
  GlobeAltIcon,
  ChartBarIcon,
  StarIcon,
} from '@heroicons/react/24/outline'

export interface Logo {
  src: string
  alt: string
  company: string
}

export interface Statistic {
  label: string
  value: string
  icon?: string
}

export interface SocialProofProps {
  testimonials?: Testimonial[]
  logos?: Logo[]
  statistics?: Statistic[]
  sectionTitle?: string
  sectionDescription?: string
}

export interface TestimonialCardProps {
  testimonial: Testimonial
}

export interface LogoGridProps {
  logos: Logo[]
}

export interface StatisticsGridProps {
  statistics: Statistic[]
}

const iconMap: Record<string, React.ComponentType<React.SVGProps<SVGSVGElement>>> = {
  users: UserGroupIcon,
  globe: GlobeAltIcon,
  chart: ChartBarIcon,
  star: StarIcon,
}

const defaultLogos: Logo[] = [
  {
    src: '/logos/company-1.svg',
    alt: 'Acme Corp logo',
    company: 'Acme Corp',
  },
  {
    src: '/logos/company-2.svg',
    alt: 'TechStart logo',
    company: 'TechStart',
  },
  {
    src: '/logos/company-3.svg',
    alt: 'GlobalFlow logo',
    company: 'GlobalFlow',
  },
  {
    src: '/logos/company-4.svg',
    alt: 'InnovateLab logo',
    company: 'InnovateLab',
  },
  {
    src: '/logos/company-5.svg',
    alt: 'CloudSync logo',
    company: 'CloudSync',
  },
]

const defaultTestimonials: Testimonial[] = [
  {
    id: 'testimonial-1',
    quote:
      'This platform has completely transformed how our team collaborates. We\'ve seen a 40% increase in productivity since adopting it.',
    author: 'Sarah Chen',
    role: 'Head of Operations',
    company: 'TechStart',
    avatar: '/avatars/sarah.jpg',
  },
  {
    id: 'testimonial-2',
    quote:
      'The automation features are incredible. What used to take hours now happens automatically. I can\'t imagine going back.',
    author: 'Michael Rodriguez',
    role: 'Product Manager',
    company: 'GlobalFlow',
    avatar: '/avatars/michael.jpg',
  },
  {
    id: 'testimonial-3',
    quote:
      'Best investment we\'ve made this year. The ROI was visible within the first month. Highly recommend to any growing team.',
    author: 'Emily Watson',
    role: 'CEO',
    company: 'InnovateLab',
    avatar: '/avatars/emily.jpg',
  },
]

const defaultStatistics: Statistic[] = [
  {
    label: 'Active Users',
    value: '50,000+',
    icon: 'users',
  },
  {
    label: 'Countries',
    value: '120+',
    icon: 'globe',
  },
  {
    label: 'Tasks Automated',
    value: '10M+',
    icon: 'chart',
  },
  {
    label: 'Customer Rating',
    value: '4.9/5',
    icon: 'star',
  },
]

export function TestimonialCard({ testimonial }: TestimonialCardProps) {
  return (
    <article
      className="bg-white rounded-xl p-6 shadow-sm border border-secondary-100
                 transition-all duration-300 ease-in-out
                 hover:shadow-lg hover:border-primary-200
                 focus-within:ring-2 focus-within:ring-primary-500 focus-within:ring-offset-2"
      data-testid="testimonial-card"
      tabIndex={0}
      aria-labelledby={`testimonial-author-${testimonial.id}`}
    >
      <blockquote>
        <p
          className="text-secondary-700 text-base leading-relaxed italic mb-4"
          data-testid="testimonial-quote"
        >
          "{testimonial.quote}"
        </p>
      </blockquote>

      <footer className="flex items-center gap-3">
        {testimonial.avatar && (
          <div
            className="w-12 h-12 rounded-full bg-secondary-100 overflow-hidden flex-shrink-0"
            data-testid="testimonial-avatar"
          >
            <img
              src={testimonial.avatar}
              alt={`${testimonial.author}'s profile photo`}
              className="w-full h-full object-cover"
              loading="lazy"
              onError={(e) => {
                // Fallback to initials on error
                const target = e.target as HTMLImageElement
                target.style.display = 'none'
                target.parentElement!.innerHTML = `<span class="flex items-center justify-center w-full h-full text-secondary-600 font-semibold">${testimonial.author
                  .split(' ')
                  .map((n) => n[0])
                  .join('')}</span>`
              }}
            />
          </div>
        )}
        <div>
          <cite
            id={`testimonial-author-${testimonial.id}`}
            className="text-secondary-900 font-semibold not-italic block"
            data-testid="testimonial-author"
          >
            {testimonial.author}
          </cite>
          <p
            className="text-secondary-600 text-sm"
            data-testid="testimonial-role-company"
          >
            <span data-testid="testimonial-role">{testimonial.role}</span>
            {' at '}
            <span data-testid="testimonial-company">{testimonial.company}</span>
          </p>
        </div>
      </footer>
    </article>
  )
}

export function LogoGrid({ logos }: LogoGridProps) {
  return (
    <div
      className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-8 items-center justify-items-center"
      data-testid="logo-grid"
      role="list"
      aria-label="Trusted by these companies"
    >
      {logos.map((logo, index) => (
        <div
          key={`logo-${index}`}
          role="listitem"
          className="flex items-center justify-center w-full"
        >
          <div
            className="grayscale hover:grayscale-0 transition-all duration-300
                       opacity-60 hover:opacity-100 cursor-default
                       flex items-center justify-center h-12 w-full max-w-[150px]"
            data-testid="logo-item"
          >
            <img
              src={logo.src}
              alt={logo.alt}
              className="h-full w-auto max-w-full object-contain"
              loading="lazy"
              onError={(e) => {
                // Fallback to company name on error
                // Using text-secondary-900 for WCAG 2.1 AA contrast compliance (4.5:1 ratio)
                // since parent container has opacity-60 which reduces effective contrast
                const target = e.target as HTMLImageElement
                target.style.display = 'none'
                target.parentElement!.innerHTML = `<span class="text-secondary-900 font-semibold text-sm">${logo.company}</span>`
              }}
            />
          </div>
        </div>
      ))}
    </div>
  )
}

export function StatisticsGrid({ statistics }: StatisticsGridProps) {
  return (
    <div
      className="grid grid-cols-2 md:grid-cols-4 gap-6 md:gap-8"
      data-testid="statistics-grid"
      role="list"
      aria-label="Key statistics"
    >
      {statistics.map((stat, index) => {
        const IconComponent = stat.icon ? iconMap[stat.icon] : null

        return (
          <div
            key={`stat-${index}`}
            role="listitem"
            className="text-center p-4 md:p-6 bg-white rounded-xl shadow-sm border border-secondary-100
                       hover:shadow-md hover:border-primary-200 transition-all duration-300"
            data-testid="statistic-item"
          >
            {IconComponent && (
              <div
                className="flex items-center justify-center w-10 h-10 mx-auto mb-3
                           bg-primary-50 text-primary-600 rounded-lg"
                data-testid="statistic-icon"
                aria-hidden="true"
              >
                <IconComponent className="w-5 h-5" />
              </div>
            )}
            <p
              className="text-2xl md:text-3xl font-bold text-primary-600 mb-1"
              data-testid="statistic-value"
            >
              {stat.value}
            </p>
            <p
              className="text-secondary-600 text-sm"
              data-testid="statistic-label"
            >
              {stat.label}
            </p>
          </div>
        )
      })}
    </div>
  )
}

export function SocialProof({
  testimonials = defaultTestimonials,
  logos = defaultLogos,
  statistics = defaultStatistics,
  sectionTitle = 'Trusted by Teams Worldwide',
  sectionDescription = 'Join thousands of companies who have transformed their workflows with our platform.',
}: SocialProofProps) {
  return (
    <section
      id="social-proof"
      className="py-16 md:py-24 bg-white"
      aria-labelledby="social-proof-heading"
      data-testid="social-proof-section"
    >
      <div className="container-main">
        {/* Section Header */}
        <div className="text-center mb-12 md:mb-16">
          <h2
            id="social-proof-heading"
            className="text-3xl md:text-4xl font-bold text-secondary-900 mb-4"
            data-testid="social-proof-heading"
          >
            {sectionTitle}
          </h2>
          <p
            className="text-lg text-secondary-600 max-w-2xl mx-auto"
            data-testid="social-proof-description"
          >
            {sectionDescription}
          </p>
        </div>

        {/* Customer Logos */}
        {logos && logos.length > 0 && (
          <div className="mb-16 md:mb-20" data-testid="logos-container">
            <h3 className="sr-only">Companies that trust us</h3>
            <LogoGrid logos={logos} />
          </div>
        )}

        {/* Statistics */}
        {statistics && statistics.length > 0 && (
          <div className="mb-16 md:mb-20" data-testid="statistics-container">
            <h3 className="sr-only">Our impact in numbers</h3>
            <StatisticsGrid statistics={statistics} />
          </div>
        )}

        {/* Testimonials */}
        {testimonials && testimonials.length > 0 && (
          <div data-testid="testimonials-container">
            <h3 className="text-xl md:text-2xl font-semibold text-secondary-900 text-center mb-8">
              What Our Customers Say
            </h3>
            <div
              className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 md:gap-8"
              data-testid="testimonials-grid"
              role="list"
            >
              {testimonials.map((testimonial) => (
                <div key={testimonial.id} role="listitem">
                  <TestimonialCard testimonial={testimonial} />
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </section>
  )
}
