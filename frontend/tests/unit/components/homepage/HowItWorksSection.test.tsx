/**
 * HowItWorksSection Component Tests
 * Owner: Scenario 4 - How It Works Section Display
 *
 * Test cases:
 * 1. Three steps are displayed: 'Paste Your URL', 'Get Short Link', 'Track & Share'
 * 2. Each step has a number indicator (1, 2, 3) and corresponding icon
 * 3. Steps are connected visually (arrows or lines between steps)
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '../../../test-utils'
import HowItWorksSection from '../../../../src/components/homepage/HowItWorksSection'

describe('HowItWorksSection', () => {
  describe('Test Case 1: Three steps with correct titles', () => {
    it('renders three step cards', () => {
      render(<HowItWorksSection />)

      const stepCards = screen.getAllByTestId('step-card')
      expect(stepCards).toHaveLength(3)
    })

    it('displays "Paste Your URL" as the first step', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText('Paste Your URL')).toBeInTheDocument()
    })

    it('displays "Get Short Link" as the second step', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText('Get Short Link')).toBeInTheDocument()
    })

    it('displays "Track & Share" as the third step', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText('Track & Share')).toBeInTheDocument()
    })

    it('renders all three step titles in the correct order', () => {
      render(<HowItWorksSection />)

      const stepTitles = screen.getAllByTestId('step-title')
      expect(stepTitles).toHaveLength(3)

      expect(stepTitles[0]).toHaveTextContent('Paste Your URL')
      expect(stepTitles[1]).toHaveTextContent('Get Short Link')
      expect(stepTitles[2]).toHaveTextContent('Track & Share')
    })
  })

  describe('Test Case 2: Each step has number indicator and icon', () => {
    it('renders three number indicators with values 1, 2, 3', () => {
      render(<HowItWorksSection />)

      const stepNumbers = screen.getAllByTestId('step-number')
      expect(stepNumbers).toHaveLength(3)

      expect(stepNumbers[0]).toHaveTextContent('1')
      expect(stepNumbers[1]).toHaveTextContent('2')
      expect(stepNumbers[2]).toHaveTextContent('3')
    })

    it('each step number has an aria-label for accessibility', () => {
      render(<HowItWorksSection />)

      const stepNumbers = screen.getAllByTestId('step-number')

      expect(stepNumbers[0]).toHaveAttribute('aria-label', 'Step 1')
      expect(stepNumbers[1]).toHaveAttribute('aria-label', 'Step 2')
      expect(stepNumbers[2]).toHaveAttribute('aria-label', 'Step 3')
    })

    it('renders three step icons', () => {
      render(<HowItWorksSection />)

      const stepIcons = screen.getAllByTestId('step-icon')
      expect(stepIcons).toHaveLength(3)
    })

    it('each step icon contains an SVG element', () => {
      render(<HowItWorksSection />)

      const stepIcons = screen.getAllByTestId('step-icon')

      stepIcons.forEach((icon) => {
        const svg = icon.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('each step has a complete structure with number, icon, and title', () => {
      render(<HowItWorksSection />)

      const stepCards = screen.getAllByTestId('step-card')

      stepCards.forEach((card) => {
        const cardScope = within(card)

        expect(cardScope.getByTestId('step-number')).toBeInTheDocument()
        expect(cardScope.getByTestId('step-icon')).toBeInTheDocument()
        expect(cardScope.getByTestId('step-title')).toBeInTheDocument()
        expect(cardScope.getByTestId('step-description')).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 3: Steps are connected visually', () => {
    it('renders connectors between steps (not after the last step)', () => {
      render(<HowItWorksSection />)

      const connectors = screen.getAllByTestId('step-connector')
      // There should be 2 connectors (between step 1-2 and step 2-3)
      expect(connectors).toHaveLength(2)
    })

    it('renders arrow connectors for desktop view', () => {
      render(<HowItWorksSection />)

      const arrowConnectors = screen.getAllByTestId('step-connector-arrow')
      // Should have 2 arrow connectors
      expect(arrowConnectors).toHaveLength(2)
    })

    it('renders line connectors for mobile view', () => {
      render(<HowItWorksSection />)

      const lineConnectors = screen.getAllByTestId('step-connector-line')
      // Should have 2 line connectors
      expect(lineConnectors).toHaveLength(2)
    })

    it('arrow connectors have aria-hidden for accessibility', () => {
      render(<HowItWorksSection />)

      const arrowConnectors = screen.getAllByTestId('step-connector-arrow')

      arrowConnectors.forEach((arrow) => {
        expect(arrow).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('line connectors have aria-hidden for accessibility', () => {
      render(<HowItWorksSection />)

      const lineConnectors = screen.getAllByTestId('step-connector-line')

      lineConnectors.forEach((line) => {
        expect(line).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('Layout and Responsiveness', () => {
    it('renders the section with proper test ID', () => {
      render(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()
    })

    it('steps container has flex layout classes for responsiveness', () => {
      render(<HowItWorksSection />)

      const container = screen.getByTestId('steps-container')
      // Mobile: vertical (flex-col), Desktop: horizontal (md:flex-row)
      expect(container).toHaveClass('flex')
      expect(container).toHaveClass('flex-col')
      expect(container).toHaveClass('md:flex-row')
    })

    it('section has proper padding for mobile', () => {
      render(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveClass('px-4')
    })

    it('arrow connectors are hidden on mobile (hidden md:block)', () => {
      render(<HowItWorksSection />)

      const arrowConnectors = screen.getAllByTestId('step-connector-arrow')

      arrowConnectors.forEach((arrow) => {
        expect(arrow).toHaveClass('hidden')
        expect(arrow).toHaveClass('md:block')
      })
    })

    it('line connectors are hidden on desktop (md:hidden)', () => {
      render(<HowItWorksSection />)

      const lineConnectors = screen.getAllByTestId('step-connector-line')

      lineConnectors.forEach((line) => {
        expect(line).toHaveClass('md:hidden')
      })
    })
  })

  describe('Accessibility', () => {
    it('has a proper heading for the section', () => {
      render(<HowItWorksSection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveTextContent('How It Works')
    })

    it('section has aria-labelledby pointing to the heading', () => {
      render(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')

      const heading = document.getElementById('how-it-works-heading')
      expect(heading).toBeInTheDocument()
    })

    it('step icons have aria-hidden attribute', () => {
      render(<HowItWorksSection />)

      const stepIcons = screen.getAllByTestId('step-icon')

      stepIcons.forEach((icon) => {
        const svg = icon.querySelector('svg')
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('Step content verification', () => {
    it('Paste Your URL step has a description', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText(/Copy any long URL and paste it/)).toBeInTheDocument()
    })

    it('Get Short Link step has a description', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText(/Click the shorten button and instantly/)).toBeInTheDocument()
    })

    it('Track & Share step has a description', () => {
      render(<HowItWorksSection />)

      expect(screen.getByText(/Share your link anywhere and track/)).toBeInTheDocument()
    })
  })
})
