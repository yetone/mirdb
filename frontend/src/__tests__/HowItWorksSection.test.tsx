import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import HowItWorksSection from '../components/HowItWorksSection'

describe('HowItWorksSection', () => {
  // Test Case 1: Component renders with exactly 3 step cards
  describe('Component Rendering', () => {
    it('should render the HowItWorksSection component with exactly 3 step cards', () => {
      render(<HowItWorksSection />)

      // Find the section
      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()

      // Find all step cards
      const stepCards = screen.getAllByTestId(/^step-card-/)
      expect(stepCards).toHaveLength(3)
    })

    it('should display a section heading', () => {
      render(<HowItWorksSection />)

      const heading = screen.getByRole('heading', { name: /how it works/i })
      expect(heading).toBeInTheDocument()
    })
  })

  // Test Case 2: Step 1 - Paste your long URL
  describe('Step 1 - Paste URL', () => {
    it('should display step 1 with "Paste your long URL" text', () => {
      render(<HowItWorksSection />)

      const step1Card = screen.getByTestId('step-card-1')
      expect(step1Card).toBeInTheDocument()

      // Check for step number
      const stepNumber = within(step1Card).getByTestId('step-number-1')
      expect(stepNumber).toHaveTextContent('1')

      // Check for description text
      expect(within(step1Card).getByText(/paste your long url/i)).toBeInTheDocument()
    })

    it('should display an icon for step 1', () => {
      render(<HowItWorksSection />)

      const step1Card = screen.getByTestId('step-card-1')
      const icon = within(step1Card).getByTestId('step-icon-1')
      expect(icon).toBeInTheDocument()
    })
  })

  // Test Case 3: Step 2 - Get your shortened link
  describe('Step 2 - Get Shortened Link', () => {
    it('should display step 2 with "Get your shortened link" text', () => {
      render(<HowItWorksSection />)

      const step2Card = screen.getByTestId('step-card-2')
      expect(step2Card).toBeInTheDocument()

      // Check for step number
      const stepNumber = within(step2Card).getByTestId('step-number-2')
      expect(stepNumber).toHaveTextContent('2')

      // Check for description text
      expect(within(step2Card).getByText(/get your shortened link/i)).toBeInTheDocument()
    })

    it('should display an icon for step 2', () => {
      render(<HowItWorksSection />)

      const step2Card = screen.getByTestId('step-card-2')
      const icon = within(step2Card).getByTestId('step-icon-2')
      expect(icon).toBeInTheDocument()
    })
  })

  // Test Case 4: Step 3 - Share and track performance
  describe('Step 3 - Share and Track', () => {
    it('should display step 3 with "Share and track performance" text', () => {
      render(<HowItWorksSection />)

      const step3Card = screen.getByTestId('step-card-3')
      expect(step3Card).toBeInTheDocument()

      // Check for step number
      const stepNumber = within(step3Card).getByTestId('step-number-3')
      expect(stepNumber).toHaveTextContent('3')

      // Check for description text
      expect(within(step3Card).getByText(/share and track performance/i)).toBeInTheDocument()
    })

    it('should display an icon for step 3', () => {
      render(<HowItWorksSection />)

      const step3Card = screen.getByTestId('step-card-3')
      const icon = within(step3Card).getByTestId('step-icon-3')
      expect(icon).toBeInTheDocument()
    })
  })

  // Test Case 5: Visual hierarchy - Steps displayed in sequential order
  describe('Visual Hierarchy and Sequential Order', () => {
    it('should display steps in sequential order (1, 2, 3)', () => {
      render(<HowItWorksSection />)

      const stepCards = screen.getAllByTestId(/^step-card-/)
      expect(stepCards).toHaveLength(3)

      // Verify order by checking the DOM order
      const stepNumbers = screen.getAllByTestId(/^step-number-/)
      expect(stepNumbers[0]).toHaveTextContent('1')
      expect(stepNumbers[1]).toHaveTextContent('2')
      expect(stepNumbers[2]).toHaveTextContent('3')
    })

    it('should have visual progression with step numbers styled distinctly', () => {
      render(<HowItWorksSection />)

      const stepNumbers = screen.getAllByTestId(/^step-number-/)

      // Each step number should have styling for visual distinction
      stepNumbers.forEach((stepNumber) => {
        // Check that it has some styling class (we'll use flex, items-center, etc.)
        expect(stepNumber.className).toBeTruthy()
      })
    })

    it('should render steps in a container with proper layout', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toBeInTheDocument()

      // Container should have layout classes
      expect(stepsContainer.className).toContain('grid')
    })
  })
})
