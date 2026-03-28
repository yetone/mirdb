import { describe, it, expect } from 'vitest'
import { render, screen } from '../../../setup'
import HowItWorksSection from '@/components/homepage/HowItWorksSection'

describe('HowItWorksSection', () => {
  describe('Test Case 1: Section heading is present', () => {
    it('should render the How It Works section with a section heading', () => {
      render(<HowItWorksSection />)

      // Verify section exists
      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()

      // Verify heading exists with correct text
      const heading = screen.getByRole('heading', { level: 2, name: /how it works/i })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('How It Works')
    })
  })

  describe('Test Case 2: Exactly 3 step elements are rendered', () => {
    it('should render exactly 3 step elements', () => {
      render(<HowItWorksSection />)

      // Find all step elements by test ID pattern
      const step1 = screen.getByTestId('step-1')
      const step2 = screen.getByTestId('step-2')
      const step3 = screen.getByTestId('step-3')

      expect(step1).toBeInTheDocument()
      expect(step2).toBeInTheDocument()
      expect(step3).toBeInTheDocument()

      // Verify exactly 3 steps (no step 4)
      expect(screen.queryByTestId('step-4')).not.toBeInTheDocument()

      // Verify container exists
      const container = screen.getByTestId('steps-container')
      expect(container).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Step 1 contains text about pasting/entering a URL', () => {
    it('should have Step 1 with text about pasting or entering a URL', () => {
      render(<HowItWorksSection />)

      // Check step 1 title
      const stepTitle = screen.getByTestId('step-title-1')
      expect(stepTitle).toBeInTheDocument()
      expect(stepTitle).toHaveTextContent(/paste.*url/i)

      // Check step 1 description contains URL-related text
      const stepDescription = screen.getByTestId('step-description-1')
      expect(stepDescription).toBeInTheDocument()
      expect(stepDescription.textContent?.toLowerCase()).toMatch(/enter|paste|url/i)
    })
  })

  describe('Test Case 4: Step 2 contains text about getting a short link', () => {
    it('should have Step 2 with text about getting a short link', () => {
      render(<HowItWorksSection />)

      // Check step 2 title
      const stepTitle = screen.getByTestId('step-title-2')
      expect(stepTitle).toBeInTheDocument()
      expect(stepTitle).toHaveTextContent(/short.*link/i)

      // Check step 2 description
      const stepDescription = screen.getByTestId('step-description-2')
      expect(stepDescription).toBeInTheDocument()
      expect(stepDescription.textContent?.toLowerCase()).toMatch(/short|link/i)
    })
  })

  describe('Test Case 5: Step 3 contains text about tracking performance/analytics', () => {
    it('should have Step 3 with text about tracking performance or analytics', () => {
      render(<HowItWorksSection />)

      // Check step 3 title
      const stepTitle = screen.getByTestId('step-title-3')
      expect(stepTitle).toBeInTheDocument()
      expect(stepTitle).toHaveTextContent(/track.*performance/i)

      // Check step 3 description contains analytics/tracking text
      const stepDescription = screen.getByTestId('step-description-3')
      expect(stepDescription).toBeInTheDocument()
      expect(stepDescription.textContent?.toLowerCase()).toMatch(/track|analytics|monitor|performance/i)
    })
  })

  describe('Test Case 6: Steps have visual progression indicators', () => {
    it('should have numbered step indicators and connector arrows', () => {
      render(<HowItWorksSection />)

      // Check that each step has a number indicator
      const stepNumber1 = screen.getByTestId('step-number-1')
      const stepNumber2 = screen.getByTestId('step-number-2')
      const stepNumber3 = screen.getByTestId('step-number-3')

      expect(stepNumber1).toBeInTheDocument()
      expect(stepNumber1).toHaveTextContent('1')
      expect(stepNumber2).toBeInTheDocument()
      expect(stepNumber2).toHaveTextContent('2')
      expect(stepNumber3).toBeInTheDocument()
      expect(stepNumber3).toHaveTextContent('3')

      // Check that connector arrows exist between steps (but not after the last one)
      const connector1 = screen.getByTestId('step-connector-1')
      const connector2 = screen.getByTestId('step-connector-2')

      expect(connector1).toBeInTheDocument()
      expect(connector2).toBeInTheDocument()

      // Last step should not have a connector
      expect(screen.queryByTestId('step-connector-3')).not.toBeInTheDocument()
    })

    it('should have mobile connector arrows for responsive display', () => {
      render(<HowItWorksSection />)

      // Check mobile connectors exist
      const mobileConnector1 = screen.getByTestId('step-connector-mobile-1')
      const mobileConnector2 = screen.getByTestId('step-connector-mobile-2')

      expect(mobileConnector1).toBeInTheDocument()
      expect(mobileConnector2).toBeInTheDocument()

      // Last step should not have mobile connector
      expect(screen.queryByTestId('step-connector-mobile-3')).not.toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('should have proper aria attributes', () => {
      render(<HowItWorksSection />)

      // Section should have aria-labelledby
      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')

      // Heading should have the referenced id
      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveAttribute('id', 'how-it-works-heading')
    })
  })
})
