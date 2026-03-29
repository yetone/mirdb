/**
 * Social Proof Section Component
 * Owner: Scenario 7 - Social Proof Section
 *
 * Displays social proof elements (placeholder):
 * - Usage statistics (e.g., "10,000+ links shortened")
 * - Customer testimonials placeholder
 * - Partner logos placeholder
 *
 * Requirements: REQ-7
 */
import { UserGroupIcon, GlobeAltIcon, ClockIcon } from '@heroicons/react/24/outline'
import type { Statistic, Testimonial } from '../../types/homepage'

const statistics: (Statistic & { icon: React.ReactNode })[] = [
  {
    value: '10,000+',
    label: 'Links Shortened',
    icon: <GlobeAltIcon className="w-8 h-8" />,
  },
  {
    value: '5,000+',
    label: 'Happy Users',
    icon: <UserGroupIcon className="w-8 h-8" />,
  },
  {
    value: '99.9%',
    label: 'Uptime',
    icon: <ClockIcon className="w-8 h-8" />,
  },
]

const testimonials: Testimonial[] = [
  {
    quote:
      'This URL shortener has completely transformed how we share links with our customers. The analytics are incredibly insightful.',
    author: 'Sarah Johnson',
    role: 'Marketing Manager',
  },
  {
    quote:
      'Simple, fast, and reliable. We switched from another service and never looked back. Highly recommended!',
    author: 'Michael Chen',
    role: 'Product Lead',
  },
  {
    quote:
      'The dashboard makes it so easy to track all our campaign links in one place. A must-have tool for any marketer.',
    author: 'Emily Rodriguez',
    role: 'Growth Specialist',
  },
]

export function SocialProofSection() {
  return (
    <section
      className="py-16 px-4 md:px-8 lg:px-16 bg-base-200"
      data-testid="social-proof-section"
      id="social-proof"
    >
      <div className="max-w-6xl mx-auto">
        {/* Section Header */}
        <div className="text-center mb-12">
          <h2 className="text-3xl font-bold mb-4">Trusted by Thousands</h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Join thousands of users who trust our platform to shorten and track their links.
          </p>
        </div>

        {/* Statistics Section */}
        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-16"
          data-testid="statistics-section"
        >
          {statistics.map((stat, index) => (
            <div
              key={index}
              className="text-center p-6 bg-base-100 rounded-lg shadow-md"
              data-testid="statistic-item"
            >
              <div className="flex justify-center mb-4 text-primary">{stat.icon}</div>
              <div className="text-4xl font-bold text-primary mb-2">{stat.value}</div>
              <div className="text-base-content/70">{stat.label}</div>
            </div>
          ))}
        </div>

        {/* Testimonials Section */}
        <div data-testid="testimonials-section">
          <h3 className="text-2xl font-semibold text-center mb-8">What Our Users Say</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {testimonials.map((testimonial, index) => (
              <div
                key={index}
                className="p-6 bg-base-100 rounded-lg shadow-md border border-base-300 hover:shadow-lg transition-shadow"
                data-testid="testimonial-card"
              >
                <blockquote
                  className="text-base-content/80 italic mb-4"
                  data-testid="testimonial-quote"
                >
                  "{testimonial.quote}"
                </blockquote>
                <div data-testid="testimonial-author">
                  <div className="font-semibold">{testimonial.author}</div>
                  {testimonial.role && (
                    <div className="text-sm text-base-content/60">{testimonial.role}</div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
