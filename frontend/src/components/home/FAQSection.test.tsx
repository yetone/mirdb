/**
 * FAQSection Component Tests
 *
 * Tests for the FAQ Section Functionality scenario (Scenario 4)
 * Validates accordion behavior, content, and accessibility
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { FAQSection } from './FAQSection'

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
  HTMLMotionProps: {},
}))

describe('FAQSection', () => {
  // Test Case 1: Query FAQ section and count FAQ items (5-8)
  describe('FAQ Section and Item Count', () => {
    it('renders the FAQ section with between 5-8 FAQ items', () => {
      render(<FAQSection />)

      const section = screen.getByTestId('faq-section')
      expect(section).toBeInTheDocument()

      // Get all FAQ items
      const faqItems = screen.getAllByTestId(/^faq-item-/)
      expect(faqItems.length).toBeGreaterThanOrEqual(5)
      expect(faqItems.length).toBeLessThanOrEqual(8)
    })

    it('renders exactly 7 FAQ items', () => {
      render(<FAQSection />)

      const faqItems = screen.getAllByTestId(/^faq-item-/)
      expect(faqItems).toHaveLength(7)
    })

    it('renders the FAQ section heading', () => {
      render(<FAQSection />)

      const heading = screen.getByRole('heading', { name: /frequently asked questions/i })
      expect(heading).toBeInTheDocument()
    })
  })

  // Test Case 2: Click on first FAQ question to expand
  describe('Expand FAQ Item', () => {
    it('FAQ answer becomes visible when question is clicked', async () => {
      const user = userEvent.setup()
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')
      const firstAnswer = screen.getByTestId('faq-answer-1')

      // Initially, answer should be hidden
      expect(firstAnswer).toHaveStyle({ display: 'none' })

      // Click to expand
      await user.click(firstQuestion)

      // Answer should now be visible
      expect(firstAnswer).toHaveStyle({ display: 'block' })

      // Question should still be visible
      expect(firstQuestion).toBeInTheDocument()
      expect(firstQuestion).toBeVisible()
    })

    it('question remains visible after expanding', async () => {
      const user = userEvent.setup()
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')

      await user.click(firstQuestion)

      // Question should still be visible and accessible
      expect(firstQuestion).toBeVisible()
      expect(firstQuestion).toHaveTextContent(/account|authentication/i)
    })

    it('sets aria-expanded to true when expanded', async () => {
      const user = userEvent.setup()
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')

      expect(firstQuestion).toHaveAttribute('aria-expanded', 'false')

      await user.click(firstQuestion)

      expect(firstQuestion).toHaveAttribute('aria-expanded', 'true')
    })
  })

  // Test Case 3: Click on expanded FAQ question to collapse
  describe('Collapse FAQ Item', () => {
    it('FAQ answer is hidden when expanded question is clicked again', async () => {
      const user = userEvent.setup()
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')
      const firstAnswer = screen.getByTestId('faq-answer-1')

      // Click to expand
      await user.click(firstQuestion)
      expect(firstAnswer).toHaveStyle({ display: 'block' })

      // Click again to collapse
      await user.click(firstQuestion)
      expect(firstAnswer).toHaveStyle({ display: 'none' })
    })

    it('sets aria-expanded to false when collapsed', async () => {
      const user = userEvent.setup()
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')

      // Expand
      await user.click(firstQuestion)
      expect(firstQuestion).toHaveAttribute('aria-expanded', 'true')

      // Collapse
      await user.click(firstQuestion)
      expect(firstQuestion).toHaveAttribute('aria-expanded', 'false')
    })
  })

  // Test Case 4: Search for authentication-related FAQ
  describe('Authentication-related FAQ', () => {
    it('at least one FAQ addresses authentication requirements', () => {
      render(<FAQSection />)

      // Get all FAQ questions
      const questions = screen.getAllByTestId(/^faq-question-/)

      // Check if any question contains authentication-related keywords
      const authRelatedQuestion = questions.some((q) =>
        /account|authentication|login|sign|secure/i.test(q.textContent || '')
      )
      expect(authRelatedQuestion).toBe(true)
    })

    it('first FAQ addresses authentication requirement', () => {
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')
      expect(firstQuestion).toHaveTextContent(/account/i)
    })

    it('authentication FAQ answer explains the requirement', () => {
      render(<FAQSection />)

      const firstAnswer = screen.getByTestId('faq-answer-1')
      expect(firstAnswer).toHaveTextContent(/authentication/i)
    })
  })

  // Test Case 5: Search for analytics-related FAQ
  describe('Analytics-related FAQ', () => {
    it('at least one FAQ addresses analytics/tracking capabilities', () => {
      render(<FAQSection />)

      // Get all FAQ questions and answers
      const questions = screen.getAllByTestId(/^faq-question-/)
      const answers = screen.getAllByTestId(/^faq-answer-/)

      // Check questions for analytics keywords
      const analyticsInQuestion = questions.some((q) =>
        /analytics|tracking|statistics|clicks|data/i.test(q.textContent || '')
      )

      // Check answers for analytics keywords
      const analyticsInAnswer = answers.some((a) =>
        /analytics|tracking|statistics|clicks|geographic|device/i.test(a.textContent || '')
      )

      expect(analyticsInQuestion || analyticsInAnswer).toBe(true)
    })

    it('analytics FAQ explains tracking capabilities', () => {
      render(<FAQSection />)

      const secondQuestion = screen.getByTestId('faq-question-2')
      expect(secondQuestion).toHaveTextContent(/analytics|tracking/i)

      const secondAnswer = screen.getByTestId('faq-answer-2')
      expect(secondAnswer).toHaveTextContent(/click|geographic|device|browser/i)
    })
  })

  // Test Case 6: Verify FAQ uses DaisyUI accordion component
  describe('DaisyUI Accordion Component', () => {
    it('FAQ items use collapse classes from DaisyUI', () => {
      render(<FAQSection />)

      const faqItems = screen.getAllByTestId(/^faq-item-/)

      faqItems.forEach((item) => {
        expect(item).toHaveClass('collapse')
        expect(item).toHaveClass('collapse-arrow')
      })
    })

    it('FAQ questions use collapse-title class', () => {
      render(<FAQSection />)

      const questions = screen.getAllByTestId(/^faq-question-/)

      questions.forEach((question) => {
        expect(question).toHaveClass('collapse-title')
      })
    })

    it('FAQ answers use collapse-content class', () => {
      render(<FAQSection />)

      const answers = screen.getAllByTestId(/^faq-answer-/)

      answers.forEach((answer) => {
        expect(answer).toHaveClass('collapse-content')
      })
    })

    it('FAQ items have proper background styling', () => {
      render(<FAQSection />)

      const faqItems = screen.getAllByTestId(/^faq-item-/)

      faqItems.forEach((item) => {
        expect(item).toHaveClass('bg-base-200')
        expect(item).toHaveClass('rounded-lg')
      })
    })
  })

  // Additional tests for accessibility and keyboard navigation
  describe('Accessibility', () => {
    it('has proper aria-labelledby for the section', () => {
      render(<FAQSection />)

      const section = screen.getByTestId('faq-section')
      expect(section).toHaveAttribute('aria-labelledby', 'faq-heading')

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveAttribute('id', 'faq-heading')
    })

    it('questions have role="button" for accessibility', () => {
      render(<FAQSection />)

      const questions = screen.getAllByTestId(/^faq-question-/)

      questions.forEach((question) => {
        expect(question).toHaveAttribute('role', 'button')
      })
    })

    it('questions are keyboard accessible with tabindex', () => {
      render(<FAQSection />)

      const questions = screen.getAllByTestId(/^faq-question-/)

      questions.forEach((question) => {
        expect(question).toHaveAttribute('tabindex', '0')
      })
    })

    it('can toggle FAQ with Enter key', async () => {
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')
      const firstAnswer = screen.getByTestId('faq-answer-1')

      // Initially hidden
      expect(firstAnswer).toHaveStyle({ display: 'none' })

      // Focus and press Enter
      firstQuestion.focus()
      fireEvent.keyDown(firstQuestion, { key: 'Enter' })

      // Should be expanded
      expect(firstAnswer).toHaveStyle({ display: 'block' })
    })

    it('can toggle FAQ with Space key', async () => {
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')
      const firstAnswer = screen.getByTestId('faq-answer-1')

      // Initially hidden
      expect(firstAnswer).toHaveStyle({ display: 'none' })

      // Focus and press Space
      firstQuestion.focus()
      fireEvent.keyDown(firstQuestion, { key: ' ' })

      // Should be expanded
      expect(firstAnswer).toHaveStyle({ display: 'block' })
    })

    it('answers have proper aria-labelledby linking to question', () => {
      render(<FAQSection />)

      for (let i = 1; i <= 7; i++) {
        const answer = screen.getByTestId(`faq-answer-${i}`)
        expect(answer).toHaveAttribute('aria-labelledby', `faq-question-${i}`)
      }
    })
  })

  // Test multiple items can be expanded
  describe('Multiple Item Expansion', () => {
    it('multiple FAQ items can be expanded simultaneously', async () => {
      const user = userEvent.setup()
      render(<FAQSection />)

      const firstQuestion = screen.getByTestId('faq-question-1')
      const secondQuestion = screen.getByTestId('faq-question-2')
      const firstAnswer = screen.getByTestId('faq-answer-1')
      const secondAnswer = screen.getByTestId('faq-answer-2')

      // Expand first item
      await user.click(firstQuestion)
      expect(firstAnswer).toHaveStyle({ display: 'block' })

      // Expand second item
      await user.click(secondQuestion)
      expect(secondAnswer).toHaveStyle({ display: 'block' })

      // Both should still be expanded
      expect(firstAnswer).toHaveStyle({ display: 'block' })
      expect(secondAnswer).toHaveStyle({ display: 'block' })
    })
  })

  // Test content topics
  describe('Content Topics', () => {
    it('covers security topic in FAQ content', () => {
      render(<FAQSection />)

      const allContent = screen.getAllByTestId(/^faq-(question|answer)-/)
      const securityContent = allContent.some((el) =>
        /security|secure|encrypt|protect/i.test(el.textContent || '')
      )
      expect(securityContent).toBe(true)
    })

    it('covers pricing topic in FAQ content', () => {
      render(<FAQSection />)

      const allContent = screen.getAllByTestId(/^faq-(question|answer)-/)
      const pricingContent = allContent.some((el) =>
        /free|price|cost|subscription/i.test(el.textContent || '')
      )
      expect(pricingContent).toBe(true)
    })
  })
})
