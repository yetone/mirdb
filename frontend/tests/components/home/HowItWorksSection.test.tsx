/**
 * Unit and Integration Tests for HowItWorksSection Component
 * Owner: Scenario 3 - How It Works Section
 *
 * Test Coverage:
 * - Component renders without errors
 * - Exactly 3 steps are displayed
 * - Step 1 (Create) has correct title and description
 * - Step 2 (Share) has correct title and description
 * - Step 3 (Track) has correct title and description
 * - Each step displays a step number indicator
 * - Each step has an associated icon
 * - Responsive layout tests
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HowItWorksSection from '../../../src/components/home/HowItWorksSection'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'

// Helper to render component with providers
const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <ThemeProvider defaultTheme="dark">
      <BrowserRouter>
        {component}
      </BrowserRouter>
    </ThemeProvider>
  )
}

describe('HowItWorksSection', () => {
  // Test Case 1: Component renders without errors
  describe('Test Case 1: Component renders without errors', () => {
    it('should render the HowItWorksSection component successfully', () => {
      renderWithProviders(<HowItWorksSection />)

      // Check that the section exists
      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()

      // Check that the main heading exists
      const heading = screen.getByRole('heading', { name: /how it works/i })
      expect(heading).toBeInTheDocument()
    })

    it('should have proper accessibility attributes', () => {
      renderWithProviders(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-title')

      const heading = screen.getByRole('heading', { name: /how it works/i })
      expect(heading).toHaveAttribute('id', 'how-it-works-title')
    })
  })

  // Test Case 2: Exactly 3 steps are displayed
  describe('Test Case 2: Exactly 3 steps are displayed', () => {
    it('should display exactly 3 step cards', () => {
      renderWithProviders(<HowItWorksSection />)

      const stepCard1 = screen.getByTestId('step-card-1')
      const stepCard2 = screen.getByTestId('step-card-2')
      const stepCard3 = screen.getByTestId('step-card-3')

      expect(stepCard1).toBeInTheDocument()
      expect(stepCard2).toBeInTheDocument()
      expect(stepCard3).toBeInTheDocument()
    })

    it('should have a container with all 3 steps', () => {
      renderWithProviders(<HowItWorksSection />)

      const container = screen.getByTestId('steps-container')
      expect(container).toBeInTheDocument()

      // Verify 3 children within the container (step cards)
      const stepCards = within(container).getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)
    })
  })

  // Test Case 3: Step 1 (Create) has title and description about pasting URL
  describe('Test Case 3: Step 1 (Create) content', () => {
    it('should display Step 1 with title "Create"', () => {
      renderWithProviders(<HowItWorksSection />)

      const title = screen.getByTestId('step-title-1')
      expect(title).toHaveTextContent('Create')
    })

    it('should display Step 1 description about pasting URL', () => {
      renderWithProviders(<HowItWorksSection />)

      const description = screen.getByTestId('step-description-1')
      expect(description.textContent?.toLowerCase()).toContain('url')
      expect(description.textContent?.toLowerCase()).toContain('short link')
    })
  })

  // Test Case 4: Step 2 (Share) has title and description about sharing links
  describe('Test Case 4: Step 2 (Share) content', () => {
    it('should display Step 2 with title "Share"', () => {
      renderWithProviders(<HowItWorksSection />)

      const title = screen.getByTestId('step-title-2')
      expect(title).toHaveTextContent('Share')
    })

    it('should display Step 2 description about sharing links', () => {
      renderWithProviders(<HowItWorksSection />)

      const description = screen.getByTestId('step-description-2')
      expect(description.textContent?.toLowerCase()).toContain('share')
    })
  })

  // Test Case 5: Step 3 (Track) has title and description about monitoring analytics
  describe('Test Case 5: Step 3 (Track) content', () => {
    it('should display Step 3 with title "Track"', () => {
      renderWithProviders(<HowItWorksSection />)

      const title = screen.getByTestId('step-title-3')
      expect(title).toHaveTextContent('Track')
    })

    it('should display Step 3 description about monitoring analytics', () => {
      renderWithProviders(<HowItWorksSection />)

      const description = screen.getByTestId('step-description-3')
      expect(description.textContent?.toLowerCase()).toContain('analytics')
    })
  })

  // Test Case 6: Each step displays a step number indicator
  describe('Test Case 6: Step number indicators', () => {
    it('should display step number 1', () => {
      renderWithProviders(<HowItWorksSection />)

      const numberIndicator = screen.getByTestId('step-number-1')
      expect(numberIndicator).toHaveTextContent('1')
      expect(numberIndicator).toHaveAttribute('aria-label', 'Step 1')
    })

    it('should display step number 2', () => {
      renderWithProviders(<HowItWorksSection />)

      const numberIndicator = screen.getByTestId('step-number-2')
      expect(numberIndicator).toHaveTextContent('2')
      expect(numberIndicator).toHaveAttribute('aria-label', 'Step 2')
    })

    it('should display step number 3', () => {
      renderWithProviders(<HowItWorksSection />)

      const numberIndicator = screen.getByTestId('step-number-3')
      expect(numberIndicator).toHaveTextContent('3')
      expect(numberIndicator).toHaveAttribute('aria-label', 'Step 3')
    })
  })

  // Test Case 7: Each step has an associated icon or illustration
  describe('Test Case 7: Step icons', () => {
    it('should display an icon for Step 1', () => {
      renderWithProviders(<HowItWorksSection />)

      const iconContainer = screen.getByTestId('step-icon-1')
      expect(iconContainer).toBeInTheDocument()
      expect(iconContainer).toHaveAttribute('role', 'img')
      expect(iconContainer).toHaveAttribute('aria-label', 'Create icon')
    })

    it('should display an icon for Step 2', () => {
      renderWithProviders(<HowItWorksSection />)

      const iconContainer = screen.getByTestId('step-icon-2')
      expect(iconContainer).toBeInTheDocument()
      expect(iconContainer).toHaveAttribute('role', 'img')
      expect(iconContainer).toHaveAttribute('aria-label', 'Share icon')
    })

    it('should display an icon for Step 3', () => {
      renderWithProviders(<HowItWorksSection />)

      const iconContainer = screen.getByTestId('step-icon-3')
      expect(iconContainer).toBeInTheDocument()
      expect(iconContainer).toHaveAttribute('role', 'img')
      expect(iconContainer).toHaveAttribute('aria-label', 'Track icon')
    })
  })

  // Test Case 8: Steps display in horizontal layout on desktop
  describe('Test Case 8: Desktop viewport layout', () => {
    beforeEach(() => {
      // Set viewport to desktop size
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 1024,
      })
      window.dispatchEvent(new Event('resize'))
    })

    afterEach(() => {
      // Reset viewport
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 1024,
      })
    })

    it('should have flex-row class for horizontal layout on desktop', () => {
      renderWithProviders(<HowItWorksSection />)

      const container = screen.getByTestId('steps-container')
      // md:flex-row means horizontal layout on desktop
      expect(container.className).toContain('md:flex-row')
    })

    it('should render all steps present in the document on desktop', () => {
      renderWithProviders(<HowItWorksSection />)

      const steps = [
        screen.getByTestId('step-card-1'),
        screen.getByTestId('step-card-2'),
        screen.getByTestId('step-card-3'),
      ]

      steps.forEach(step => {
        expect(step).toBeInTheDocument()
      })
    })
  })

  // Test Case 9: Steps display in vertical/stacked layout on mobile
  describe('Test Case 9: Mobile viewport layout', () => {
    beforeEach(() => {
      // Set viewport to mobile size
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      })
      window.dispatchEvent(new Event('resize'))
    })

    afterEach(() => {
      // Reset viewport
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 1024,
      })
    })

    it('should have flex-col class for vertical layout', () => {
      renderWithProviders(<HowItWorksSection />)

      const container = screen.getByTestId('steps-container')
      // flex-col is the default (mobile-first), md:flex-row applies on larger screens
      expect(container.className).toContain('flex-col')
    })

    it('should render all steps in stacked order on mobile', () => {
      renderWithProviders(<HowItWorksSection />)

      const container = screen.getByTestId('steps-container')
      const stepCards = within(container).getAllByTestId(/step-card-\d/)

      // All 3 steps should be present in document
      expect(stepCards).toHaveLength(3)
      stepCards.forEach(card => {
        expect(card).toBeInTheDocument()
      })
    })
  })
})
