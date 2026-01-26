/**
 * How It Works Section Tests
 * Owner: Scenario 3 - How It Works Section
 *
 * Test coverage:
 * - Three workflow steps displayed
 * - Step numbering visible
 * - Step content accuracy
 * - Layout and ordering
 */
import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import { HowItWorksSection } from '../../components/home/HowItWorksSection'

describe('HowItWorksSection', () => {
  describe('Test Case 1: Section renders with 3 workflow steps', () => {
    it('should render the section with exactly 3 workflow steps', () => {
      renderWithProviders(<HowItWorksSection />)

      // Check that the section heading exists
      expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument()

      // Check for 3 step number indicators
      expect(screen.getByTestId('step-number-1')).toBeInTheDocument()
      expect(screen.getByTestId('step-number-2')).toBeInTheDocument()
      expect(screen.getByTestId('step-number-3')).toBeInTheDocument()

      // Verify we have exactly 3 step titles (h3 elements within the section)
      const stepTitles = screen.getAllByRole('heading', { level: 3 })
      expect(stepTitles).toHaveLength(3)
    })
  })

  describe('Test Case 2: Step 1 content verification', () => {
    it('should display Step 1 with "Paste your long URL" text and number indicator', () => {
      renderWithProviders(<HowItWorksSection />)

      // Check step 1 number indicator
      const step1Number = screen.getByTestId('step-number-1')
      expect(step1Number).toBeInTheDocument()
      expect(step1Number).toHaveTextContent('1')
      expect(step1Number).toHaveAttribute('aria-label', 'Step 1')

      // Check step 1 title text
      expect(screen.getByText(/paste your long url/i)).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Step 2 content verification', () => {
    it('should display Step 2 with "Get a short, memorable link" text', () => {
      renderWithProviders(<HowItWorksSection />)

      // Check step 2 number indicator
      const step2Number = screen.getByTestId('step-number-2')
      expect(step2Number).toBeInTheDocument()
      expect(step2Number).toHaveTextContent('2')
      expect(step2Number).toHaveAttribute('aria-label', 'Step 2')

      // Check step 2 title text
      expect(screen.getByText(/get a short, memorable link/i)).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Step 3 content verification', () => {
    it('should display Step 3 with "Track clicks and analytics" text', () => {
      renderWithProviders(<HowItWorksSection />)

      // Check step 3 number indicator
      const step3Number = screen.getByTestId('step-number-3')
      expect(step3Number).toBeInTheDocument()
      expect(step3Number).toHaveTextContent('3')
      expect(step3Number).toHaveAttribute('aria-label', 'Step 3')

      // Check step 3 title text
      expect(screen.getByText(/track clicks and analytics/i)).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Step numbering verification', () => {
    it('should display visible number indicators (1, 2, 3) for each step', () => {
      renderWithProviders(<HowItWorksSection />)

      // Get all step number elements
      const step1 = screen.getByTestId('step-number-1')
      const step2 = screen.getByTestId('step-number-2')
      const step3 = screen.getByTestId('step-number-3')

      // Verify numbers are displayed
      expect(step1).toHaveTextContent('1')
      expect(step2).toHaveTextContent('2')
      expect(step3).toHaveTextContent('3')

      // Verify they are in the document and not hidden via aria-hidden
      expect(step1).toBeInTheDocument()
      expect(step2).toBeInTheDocument()
      expect(step3).toBeInTheDocument()
      expect(step1).not.toHaveAttribute('aria-hidden', 'true')
      expect(step2).not.toHaveAttribute('aria-hidden', 'true')
      expect(step3).not.toHaveAttribute('aria-hidden', 'true')

      // Verify proper aria labels for accessibility
      expect(step1).toHaveAttribute('aria-label', 'Step 1')
      expect(step2).toHaveAttribute('aria-label', 'Step 2')
      expect(step3).toHaveAttribute('aria-label', 'Step 3')
    })
  })

  describe('Additional coverage: Section accessibility', () => {
    it('should have proper heading hierarchy and section labeling', () => {
      renderWithProviders(<HowItWorksSection />)

      // Check section has aria-labelledby pointing to the heading
      const section = screen.getByRole('region', { name: /how it works/i })
      expect(section).toBeInTheDocument()

      // Check h2 heading exists for proper hierarchy
      const mainHeading = screen.getByRole('heading', { level: 2, name: /how it works/i })
      expect(mainHeading).toBeInTheDocument()
    })

    it('should render step descriptions', () => {
      renderWithProviders(<HowItWorksSection />)

      // Check that descriptions are present
      expect(screen.getByText(/enter any long url you want to shorten/i)).toBeInTheDocument()
      expect(screen.getByText(/instantly receive a compact/i)).toBeInTheDocument()
      expect(screen.getByText(/monitor your link performance/i)).toBeInTheDocument()
    })
  })
})
