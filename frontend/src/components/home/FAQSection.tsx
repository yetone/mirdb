/**
 * FAQ Section Component
 * Owner: Scenario 4 - FAQ Section Functionality
 *
 * Accordion-style FAQ section with common questions and answers.
 * Uses DaisyUI collapse components for expand/collapse functionality.
 *
 * Topics covered: authentication, analytics, pricing, security
 */

import { useState } from 'react'
import { motion } from 'framer-motion'
import { FAQItem } from '../../types/home'

const faqItems: FAQItem[] = [
  {
    id: 1,
    question: 'Do I need to create an account to shorten URLs?',
    answer: 'Yes, our service requires authentication to create shortened URLs. This allows us to provide you with detailed analytics, link management, and ensures the security of your shortened links. Creating an account is free and takes less than a minute.',
  },
  {
    id: 2,
    question: 'What analytics and tracking capabilities are available?',
    answer: 'We provide comprehensive analytics including click counts, geographic location of visitors, device types, browsers, referral sources, and time-based statistics. All data is displayed in an easy-to-understand dashboard with charts and exportable reports.',
  },
  {
    id: 3,
    question: 'Is the service free to use?',
    answer: 'Yes, our core URL shortening and analytics features are completely free. We offer all essential features including unlimited link creation, detailed analytics, and a modern dashboard at no cost.',
  },
  {
    id: 4,
    question: 'How secure are my shortened URLs and data?',
    answer: 'Security is our top priority. All data is encrypted using enterprise-grade security protocols. We use secure authentication methods, and your analytics data is protected with role-based access controls. You can also share stats via secure, revocable tokens.',
  },
  {
    id: 5,
    question: 'Can I customize my shortened URLs?',
    answer: 'Currently, our system generates unique, memorable short codes automatically. This ensures optimal performance and avoids conflicts. Each generated code is designed to be easy to share and remember.',
  },
  {
    id: 6,
    question: 'How long do shortened URLs remain active?',
    answer: 'Your shortened URLs remain active indefinitely as long as your account is in good standing. There are no expiration dates on links, so you can confidently use them in long-term campaigns and materials.',
  },
  {
    id: 7,
    question: 'Can I edit or delete my shortened URLs?',
    answer: 'Yes, you have full control over your shortened URLs through the dashboard. You can view, manage, and delete any of your links at any time. Deleted links will return a 404 error to visitors.',
  },
]

export interface FAQSectionProps {
  className?: string
}

export function FAQSection({ className = '' }: FAQSectionProps) {
  const [openItems, setOpenItems] = useState<Set<number>>(new Set())

  const toggleItem = (id: number) => {
    setOpenItems((prev) => {
      const newSet = new Set(prev)
      if (newSet.has(id)) {
        newSet.delete(id)
      } else {
        newSet.add(id)
      }
      return newSet
    })
  }

  const handleKeyDown = (e: React.KeyboardEvent, id: number) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      toggleItem(id)
    }
  }

  return (
    <section
      id="faq"
      data-testid="faq-section"
      className={`py-16 px-4 md:px-8 ${className}`}
      aria-labelledby="faq-heading"
    >
      <div className="max-w-3xl mx-auto">
        <motion.h2
          id="faq-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Frequently Asked Questions
        </motion.h2>

        <div className="space-y-4" role="region" aria-label="FAQ accordion">
          {faqItems.map((item, index) => {
            const isOpen = openItems.has(item.id)
            return (
              <motion.div
                key={item.id}
                data-testid={`faq-item-${item.id}`}
                className="collapse collapse-arrow bg-base-200 rounded-lg"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.3, delay: index * 0.1 }}
              >
                <div
                  role="button"
                  tabIndex={0}
                  className={`collapse-title text-lg font-medium cursor-pointer ${isOpen ? 'collapse-open' : ''}`}
                  data-testid={`faq-question-${item.id}`}
                  aria-expanded={isOpen}
                  aria-controls={`faq-answer-${item.id}`}
                  onClick={() => toggleItem(item.id)}
                  onKeyDown={(e) => handleKeyDown(e, item.id)}
                >
                  {item.question}
                </div>
                <div
                  id={`faq-answer-${item.id}`}
                  data-testid={`faq-answer-${item.id}`}
                  className={`collapse-content ${isOpen ? 'collapse-open' : ''}`}
                  role="region"
                  aria-labelledby={`faq-question-${item.id}`}
                  style={{
                    display: isOpen ? 'block' : 'none',
                    paddingBottom: isOpen ? '1rem' : '0',
                  }}
                >
                  <p className="text-base-content/80 pt-2">{item.answer}</p>
                </div>
              </motion.div>
            )
          })}
        </div>
      </div>
    </section>
  )
}

export default FAQSection
