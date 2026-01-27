/**
 * HowItWorksSection Component Tests
 * Owner: Scenario 4 - How It Works Section
 *
 * Tests for:
 * - Step 1: Paste your long URL (with number 1)
 * - Step 2: Get your short link instantly (with number 2)
 * - Step 3: Track clicks and analyze performance (with number 3)
 * - All steps have visible step numbers
 * - Responsive layout (horizontal on desktop, vertical on mobile)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '../../../utils/test-utils'
import { HowItWorksSection } from '../../../../src/components/homepage/HowItWorksSection'

describe('HowItWorksSection', () => {
  describe('Step Content Display', () => {
    it('renders Step 1 with "Paste your long URL" title and number 1', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText('Paste your long URL')).toBeInTheDocument()
      expect(screen.getByText('1')).toBeInTheDocument()
    })

    it('renders Step 2 with "Get your short link instantly" title and number 2', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText('Get your short link instantly')).toBeInTheDocument()
      expect(screen.getByText('2')).toBeInTheDocument()
    })

    it('renders Step 3 with "Track clicks and analyze performance" title and number 3', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText('Track clicks and analyze performance')).toBeInTheDocument()
      expect(screen.getByText('3')).toBeInTheDocument()
    })

    it('renders all three steps with visible step numbers', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('how-it-works-steps')

      expect(within(stepsContainer).getByText('1')).toBeInTheDocument()
      expect(within(stepsContainer).getByText('2')).toBeInTheDocument()
      expect(within(stepsContainer).getByText('3')).toBeInTheDocument()
    })

    it('renders three step cards total', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('how-it-works-steps')
      const stepNumbers = ['1', '2', '3']

      stepNumbers.forEach(num => {
        expect(within(stepsContainer).getByText(num)).toBeInTheDocument()
      })
    })
  })

  describe('Responsive Layout', () => {
    beforeEach(() => {
      vi.clearAllMocks()
    })

    it('has horizontal flow layout class for desktop (lg:flex-row)', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('how-it-works-steps')
      expect(stepsContainer).toHaveClass('lg:flex-row')
    })

    it('has vertical flow layout class for mobile (flex-col)', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('how-it-works-steps')
      expect(stepsContainer).toHaveClass('flex-col')
    })
  })

  describe('Section Structure', () => {
    it('renders section with proper id for navigation', () => {
      render(<HowItWorksSection />)

      const section = screen.getByRole('region', { name: 'How It Works' })
      expect(section).toHaveAttribute('id', 'how-it-works')
    })

    it('renders section heading', () => {
      render(<HowItWorksSection />)

      expect(screen.getByRole('heading', { level: 2, name: 'How It Works' })).toBeInTheDocument()
    })

    it('renders step titles as h3 headings', () => {
      render(<HowItWorksSection />)

      const headings = screen.getAllByRole('heading', { level: 3 })
      expect(headings).toHaveLength(3)
      expect(headings[0]).toHaveTextContent('Paste your long URL')
      expect(headings[1]).toHaveTextContent('Get your short link instantly')
      expect(headings[2]).toHaveTextContent('Track clicks and analyze performance')
    })
  })

  describe('Step Numbers Visibility', () => {
    it('step numbers have visible styling with distinct background', () => {
      render(<HowItWorksSection />)

      const stepOne = screen.getByText('1')
      // Check that the step number has styling that makes it visible
      expect(stepOne.closest('[class*="rounded"]')).toBeInTheDocument()
    })

    it('all step numbers are rendered with consistent styling', () => {
      render(<HowItWorksSection />)

      const stepNumbers = ['1', '2', '3']
      stepNumbers.forEach(num => {
        const stepNumber = screen.getByText(num)
        expect(stepNumber).toBeInTheDocument()
        // Step numbers should be in elements with font-bold for visibility
        expect(stepNumber).toHaveClass('font-bold')
      })
    })
  })

  describe('Visual Flow', () => {
    it('renders connectors between steps for visual flow', () => {
      render(<HowItWorksSection />)

      // Check for connector elements between steps
      const connectors = screen.getAllByTestId('step-connector')
      // There should be 2 connectors (between step 1-2 and step 2-3)
      expect(connectors).toHaveLength(2)
    })
  })

  describe('Accessibility', () => {
    it('section has aria-label for screen readers', () => {
      render(<HowItWorksSection />)

      expect(screen.getByRole('region', { name: 'How It Works' })).toBeInTheDocument()
    })

    it('step numbers have aria labels indicating their position', () => {
      render(<HowItWorksSection />)

      expect(screen.getByLabelText('Step 1')).toBeInTheDocument()
      expect(screen.getByLabelText('Step 2')).toBeInTheDocument()
      expect(screen.getByLabelText('Step 3')).toBeInTheDocument()
    })
  })

  describe('Step Descriptions', () => {
    it('each step has a description', () => {
      render(<HowItWorksSection />)

      // Step 1 description
      expect(screen.getByText(/Copy and paste any long URL/)).toBeInTheDocument()
      // Step 2 description
      expect(screen.getByText(/Instantly receive a short/)).toBeInTheDocument()
      // Step 3 description
      expect(screen.getByText(/Monitor your link performance/)).toBeInTheDocument()
    })
  })
})
