/**
 * HowItWorksSection Tests
 * Owner: Scenario 4 - How It Works Section
 *
 * Tests for the How It Works Section component:
 * - Section with 'How It Works' heading exists (TC1)
 * - Exactly 3 step elements are rendered (TC2)
 * - Step 1 displays number indicator (TC3)
 * - All 3 steps contain icon elements (TC4)
 * - Steps describe: pasting URL, getting short link, sharing/tracking (TC5)
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { HowItWorksSection } from '@/components/homepage/HowItWorksSection'
import { StepCard } from '@/components/homepage/StepCard'
import { Link2 } from 'lucide-react'

describe('HowItWorksSection', () => {
  // Test Case 1: Section with 'How It Works' heading exists
  describe('How It Works section container', () => {
    it('renders the section with How It Works heading', () => {
      render(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()
    })

    it('displays "How It Works" heading text', () => {
      render(<HowItWorksSection />)

      const heading = screen.getByTestId('how-it-works-heading')
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent(/how it works/i)
    })

    it('heading is an h2 element', () => {
      render(<HowItWorksSection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveTextContent(/how it works/i)
    })

    it('has proper accessibility attributes', () => {
      render(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveAttribute('id', 'how-it-works-heading')
    })
  })

  // Test Case 2: Exactly 3 step elements are rendered
  describe('Step count', () => {
    it('renders exactly 3 step elements', () => {
      render(<HowItWorksSection />)

      const stepCards = screen.getAllByTestId('step-card')
      expect(stepCards).toHaveLength(3)
    })

    it('steps are contained within a grid container', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toBeInTheDocument()

      const stepCards = within(stepsContainer).getAllByTestId('step-card')
      expect(stepCards).toHaveLength(3)
    })
  })

  // Test Case 3: Step 1 displays '1' or 'Step 1' number indicator
  describe('Step number indicators', () => {
    it('Step 1 displays number indicator "1"', () => {
      render(<HowItWorksSection />)

      const stepNumbers = screen.getAllByTestId('step-number')
      expect(stepNumbers[0]).toHaveTextContent('1')
    })

    it('all steps display their respective number indicators', () => {
      render(<HowItWorksSection />)

      const stepNumbers = screen.getAllByTestId('step-number')
      expect(stepNumbers).toHaveLength(3)
      expect(stepNumbers[0]).toHaveTextContent('1')
      expect(stepNumbers[1]).toHaveTextContent('2')
      expect(stepNumbers[2]).toHaveTextContent('3')
    })

    it('step numbers have aria-label for accessibility', () => {
      render(<HowItWorksSection />)

      const stepNumbers = screen.getAllByTestId('step-number')
      expect(stepNumbers[0]).toHaveAttribute('aria-label', 'Step 1')
      expect(stepNumbers[1]).toHaveAttribute('aria-label', 'Step 2')
      expect(stepNumbers[2]).toHaveAttribute('aria-label', 'Step 3')
    })
  })

  // Test Case 4: All 3 steps contain icon elements
  describe('Step icons', () => {
    it('all 3 steps contain icon elements', () => {
      render(<HowItWorksSection />)

      const stepCards = screen.getAllByTestId('step-card')
      expect(stepCards).toHaveLength(3)

      stepCards.forEach((card) => {
        const iconContainer = within(card).getByTestId('step-icon')
        expect(iconContainer).toBeInTheDocument()

        // Check that icon container has an SVG child (from lucide-react)
        const svg = iconContainer.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('icons are properly hidden from screen readers', () => {
      render(<HowItWorksSection />)

      const iconContainers = screen.getAllByTestId('step-icon')

      iconContainers.forEach((iconContainer) => {
        expect(iconContainer).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  // Test Case 5: Steps describe: pasting URL, getting short link, sharing/tracking
  describe('Step descriptions', () => {
    it('steps describe pasting URL', () => {
      render(<HowItWorksSection />)

      const pasteStep = screen.getByText(/paste/i)
      expect(pasteStep).toBeInTheDocument()
    })

    it('steps describe getting short link', () => {
      render(<HowItWorksSection />)

      const shortLinkStep = screen.getByText(/short link/i)
      expect(shortLinkStep).toBeInTheDocument()
    })

    it('steps describe sharing and tracking', () => {
      render(<HowItWorksSection />)

      // Use getAllByText since "share" appears in multiple places (title and description)
      const shareElements = screen.getAllByText(/share/i)
      expect(shareElements.length).toBeGreaterThanOrEqual(1)

      const trackElements = screen.getAllByText(/track/i)
      expect(trackElements.length).toBeGreaterThanOrEqual(1)
    })

    it('all three step concepts are present', () => {
      render(<HowItWorksSection />)

      // Step 1: Paste URL
      expect(screen.getByText(/paste.*url/i)).toBeInTheDocument()
      // Step 2: Get short link
      expect(screen.getByText(/short link/i)).toBeInTheDocument()
      // Step 3: Share and track
      expect(screen.getByText(/share.*track/i)).toBeInTheDocument()
    })

    it('each step has a title element', () => {
      render(<HowItWorksSection />)

      const stepTitles = screen.getAllByTestId('step-title')
      expect(stepTitles).toHaveLength(3)

      stepTitles.forEach((title) => {
        expect(title.tagName).toBe('H3')
        expect(title.textContent).not.toBe('')
      })
    })

    it('each step has a description element', () => {
      render(<HowItWorksSection />)

      const stepDescriptions = screen.getAllByTestId('step-description')
      expect(stepDescriptions).toHaveLength(3)

      stepDescriptions.forEach((description) => {
        expect(description.textContent).not.toBe('')
        // Description should have meaningful content
        expect(description.textContent!.length).toBeGreaterThan(20)
      })
    })
  })

  // Additional integration tests
  describe('HowItWorksSection integration', () => {
    it('accepts className prop', () => {
      render(<HowItWorksSection className="custom-class" />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveClass('custom-class')
    })

    it('renders with proper grid layout classes for responsive design', () => {
      render(<HowItWorksSection />)

      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toHaveClass('grid')
      expect(stepsContainer).toHaveClass('grid-cols-1')
      expect(stepsContainer).toHaveClass('md:grid-cols-3')
    })

    it('has section subtitle', () => {
      render(<HowItWorksSection />)

      const subtitle = screen.getByText(/shorten your links in three simple steps/i)
      expect(subtitle).toBeInTheDocument()
    })
  })
})

// StepCard component tests
describe('StepCard', () => {
  const defaultProps = {
    stepNumber: 1,
    icon: <Link2 className="w-8 h-8" data-testid="test-icon" />,
    title: 'Test Step',
    description: 'This is a test step description with enough content.'
  }

  it('renders step number', () => {
    render(<StepCard {...defaultProps} />)

    const stepNumber = screen.getByTestId('step-number')
    expect(stepNumber).toHaveTextContent('1')
  })

  it('renders icon', () => {
    render(<StepCard {...defaultProps} />)

    const iconContainer = screen.getByTestId('step-icon')
    expect(iconContainer).toBeInTheDocument()
  })

  it('renders title', () => {
    render(<StepCard {...defaultProps} />)

    const title = screen.getByTestId('step-title')
    expect(title).toHaveTextContent('Test Step')
  })

  it('renders description', () => {
    render(<StepCard {...defaultProps} />)

    const description = screen.getByTestId('step-description')
    expect(description).toHaveTextContent('This is a test step description with enough content.')
  })
})
