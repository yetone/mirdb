/**
 * HowItWorksSection Unit Tests
 * Owner: Scenario 4 - How It Works Section
 *
 * Tests for the HowItWorksSection component verifying:
 * - Section heading displays "How It Works"
 * - 3-step workflow displays correctly
 * - Step 1: Paste URL
 * - Step 2: Get Link
 * - Step 3: Track Clicks
 *
 * Requirements: REQ-4
 */

import { describe, it, expect } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import { HowItWorksSection } from '../../../src/components/landing/HowItWorksSection'

describe('HowItWorksSection', () => {
  describe('Test Case 1: Section exists with heading "How It Works" or similar', () => {
    it('renders the how it works section', () => {
      renderWithProviders(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()
    })

    it('displays "How It Works" heading', () => {
      renderWithProviders(<HowItWorksSection />)

      const heading = screen.getByTestId('how-it-works-heading')
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('How It Works')
    })

    it('heading is an h2 element for proper document hierarchy', () => {
      renderWithProviders(<HowItWorksSection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('How It Works')
    })

    it('section has proper aria-labelledby for accessibility', () => {
      renderWithProviders(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')
    })
  })

  describe('Test Case 2: Step 1 displays "Paste your long URL" or equivalent text', () => {
    it('renders step 1 with Paste URL title', () => {
      renderWithProviders(<HowItWorksSection />)

      const step1 = screen.getByTestId('workflow-step-1')
      expect(step1).toBeInTheDocument()

      const title = screen.getByTestId('workflow-step-1-title')
      expect(title).toHaveTextContent('Paste URL')
    })

    it('step 1 description mentions pasting URL', () => {
      renderWithProviders(<HowItWorksSection />)

      const description = screen.getByTestId('workflow-step-1-description')
      expect(description).toBeInTheDocument()

      const text = description.textContent?.toLowerCase() || ''
      expect(text).toContain('paste')
      expect(text).toContain('url')
    })

    it('step 1 displays number 1', () => {
      renderWithProviders(<HowItWorksSection />)

      const step1 = screen.getByTestId('workflow-step-1')
      expect(step1).toHaveTextContent('1')
    })
  })

  describe('Test Case 3: Step 2 displays "Get a short, trackable link" or equivalent text', () => {
    it('renders step 2 with Get Link title', () => {
      renderWithProviders(<HowItWorksSection />)

      const step2 = screen.getByTestId('workflow-step-2')
      expect(step2).toBeInTheDocument()

      const title = screen.getByTestId('workflow-step-2-title')
      expect(title).toHaveTextContent('Get Link')
    })

    it('step 2 description mentions getting a short trackable link', () => {
      renderWithProviders(<HowItWorksSection />)

      const description = screen.getByTestId('workflow-step-2-description')
      expect(description).toBeInTheDocument()

      const text = description.textContent?.toLowerCase() || ''
      expect(text).toContain('short')
      expect(text).toContain('trackable')
      expect(text).toContain('link')
    })

    it('step 2 displays number 2', () => {
      renderWithProviders(<HowItWorksSection />)

      const step2 = screen.getByTestId('workflow-step-2')
      expect(step2).toHaveTextContent('2')
    })
  })

  describe('Test Case 4: Step 3 displays "Monitor performance with analytics" or equivalent text', () => {
    it('renders step 3 with Track Clicks title', () => {
      renderWithProviders(<HowItWorksSection />)

      const step3 = screen.getByTestId('workflow-step-3')
      expect(step3).toBeInTheDocument()

      const title = screen.getByTestId('workflow-step-3-title')
      expect(title).toHaveTextContent('Track Clicks')
    })

    it('step 3 description mentions monitoring with analytics', () => {
      renderWithProviders(<HowItWorksSection />)

      const description = screen.getByTestId('workflow-step-3-description')
      expect(description).toBeInTheDocument()

      const text = description.textContent?.toLowerCase() || ''
      expect(text).toContain('monitor')
      expect(text).toContain('analytics')
    })

    it('step 3 displays number 3', () => {
      renderWithProviders(<HowItWorksSection />)

      const step3 = screen.getByTestId('workflow-step-3')
      expect(step3).toHaveTextContent('3')
    })
  })

  describe('Test Case 5: Exactly 3 workflow steps are displayed', () => {
    it('renders exactly 3 workflow steps', () => {
      renderWithProviders(<HowItWorksSection />)

      const step1 = screen.getByTestId('workflow-step-1')
      const step2 = screen.getByTestId('workflow-step-2')
      const step3 = screen.getByTestId('workflow-step-3')

      expect(step1).toBeInTheDocument()
      expect(step2).toBeInTheDocument()
      expect(step3).toBeInTheDocument()

      // Verify there is no step 4
      const step4 = screen.queryByTestId('workflow-step-4')
      expect(step4).not.toBeInTheDocument()
    })

    it('workflow list has exactly 3 items', () => {
      renderWithProviders(<HowItWorksSection />)

      const list = screen.getByRole('list', { name: /workflow steps/i })
      const items = within(list).getAllByRole('listitem')

      expect(items).toHaveLength(3)
    })

    it('steps are numbered 1, 2, 3 in order', () => {
      renderWithProviders(<HowItWorksSection />)

      const step1Title = screen.getByTestId('workflow-step-1-title')
      const step2Title = screen.getByTestId('workflow-step-2-title')
      const step3Title = screen.getByTestId('workflow-step-3-title')

      expect(step1Title.closest('article')).toHaveTextContent('1')
      expect(step2Title.closest('article')).toHaveTextContent('2')
      expect(step3Title.closest('article')).toHaveTextContent('3')
    })
  })

  describe('Custom steps prop', () => {
    it('renders custom steps when provided', () => {
      const customSteps = [
        { number: 1, title: 'Custom Step 1', description: 'Description 1' },
        { number: 2, title: 'Custom Step 2', description: 'Description 2' },
        { number: 3, title: 'Custom Step 3', description: 'Description 3' },
      ]

      renderWithProviders(<HowItWorksSection steps={customSteps} />)

      expect(screen.getByTestId('workflow-step-1-title')).toHaveTextContent('Custom Step 1')
      expect(screen.getByTestId('workflow-step-2-title')).toHaveTextContent('Custom Step 2')
      expect(screen.getByTestId('workflow-step-3-title')).toHaveTextContent('Custom Step 3')
    })
  })

  describe('Accessibility', () => {
    it('section has semantic section element', () => {
      renderWithProviders(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section.tagName).toBe('SECTION')
    })

    it('steps are wrapped in article elements', () => {
      renderWithProviders(<HowItWorksSection />)

      const step1 = screen.getByTestId('workflow-step-1')
      const step2 = screen.getByTestId('workflow-step-2')
      const step3 = screen.getByTestId('workflow-step-3')

      expect(step1.tagName).toBe('ARTICLE')
      expect(step2.tagName).toBe('ARTICLE')
      expect(step3.tagName).toBe('ARTICLE')
    })

    it('steps have proper heading hierarchy with h3 elements', () => {
      renderWithProviders(<HowItWorksSection />)

      const h3Elements = screen.getAllByRole('heading', { level: 3 })
      expect(h3Elements).toHaveLength(3)
    })
  })
})
