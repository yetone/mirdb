import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { HowItWorksSection, Step } from '../../../src/components/homepage/HowItWorksSection'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({
      children,
      whileHover,
      whileTap,
      initial,
      animate,
      transition,
      whileInView,
      viewport,
      variants,
      ...props
    }: React.PropsWithChildren<Record<string, unknown>>) => <div {...props}>{children}</div>,
  },
}))

const renderHowItWorksSection = (props: { steps?: Step[] } = {}) => {
  return render(<HowItWorksSection {...props} />)
}

describe('HowItWorksSection', () => {
  // Test Case 1: Component renders without errors
  describe('Rendering', () => {
    it('should render the component without errors', () => {
      renderHowItWorksSection()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('should render the section with correct structure', () => {
      renderHowItWorksSection()
      const section = screen.getByTestId('how-it-works-section')
      expect(section.tagName).toBe('SECTION')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')
    })

    it('should have correct section id for navigation', () => {
      renderHowItWorksSection()
      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('id', 'how-it-works')
    })
  })

  // Test Case 2: Check for section heading
  describe('Section Heading', () => {
    it('should render heading with text "How It Works"', () => {
      renderHowItWorksSection()
      const heading = screen.getByTestId('how-it-works-heading')
      expect(heading).toBeInTheDocument()
      expect(heading.textContent).toBe('How It Works')
    })

    it('should have h2 element for proper heading hierarchy', () => {
      renderHowItWorksSection()
      const heading = screen.getByTestId('how-it-works-heading')
      expect(heading.tagName).toBe('H2')
    })

    it('should have proper heading id for accessibility', () => {
      renderHowItWorksSection()
      const heading = screen.getByTestId('how-it-works-heading')
      expect(heading).toHaveAttribute('id', 'how-it-works-heading')
    })
  })

  // Test Case 3: Count displayed steps
  describe('Step Count', () => {
    it('should render exactly 3 steps', () => {
      renderHowItWorksSection()
      const steps = screen.getAllByTestId('step-item')
      expect(steps).toHaveLength(3)
    })

    it('should render 3 step numbers', () => {
      renderHowItWorksSection()
      const stepNumbers = screen.getAllByTestId('step-number')
      expect(stepNumbers).toHaveLength(3)
    })

    it('should render 3 step titles', () => {
      renderHowItWorksSection()
      const stepTitles = screen.getAllByTestId('step-title')
      expect(stepTitles).toHaveLength(3)
    })

    it('should render custom steps when provided', () => {
      const customSteps: Step[] = [
        {
          id: 'custom-1',
          number: 1,
          title: 'Custom Step 1',
          description: 'Custom description 1',
          icon: <svg data-testid="custom-icon-1" />,
        },
        {
          id: 'custom-2',
          number: 2,
          title: 'Custom Step 2',
          description: 'Custom description 2',
          icon: <svg data-testid="custom-icon-2" />,
        },
        {
          id: 'custom-3',
          number: 3,
          title: 'Custom Step 3',
          description: 'Custom description 3',
          icon: <svg data-testid="custom-icon-3" />,
        },
      ]
      renderHowItWorksSection({ steps: customSteps })
      const stepItems = screen.getAllByTestId('step-item')
      expect(stepItems).toHaveLength(3)
    })
  })

  // Test Case 4: Verify steps have visual numbering
  describe('Step Numbering', () => {
    it('should have number indicators 1, 2, 3 for each step', () => {
      renderHowItWorksSection()
      const stepNumbers = screen.getAllByTestId('step-number')

      expect(stepNumbers[0].textContent).toBe('1')
      expect(stepNumbers[1].textContent).toBe('2')
      expect(stepNumbers[2].textContent).toBe('3')
    })

    it('should have aria-label for step numbers for accessibility', () => {
      renderHowItWorksSection()
      const stepNumbers = screen.getAllByTestId('step-number')

      expect(stepNumbers[0]).toHaveAttribute('aria-label', 'Step 1')
      expect(stepNumbers[1]).toHaveAttribute('aria-label', 'Step 2')
      expect(stepNumbers[2]).toHaveAttribute('aria-label', 'Step 3')
    })

    it('should style step numbers prominently', () => {
      renderHowItWorksSection()
      const stepNumbers = screen.getAllByTestId('step-number')

      stepNumbers.forEach((stepNumber) => {
        expect(stepNumber).toHaveClass('rounded-full')
        expect(stepNumber).toHaveClass('bg-primary')
      })
    })
  })

  // Test Case 5: Check Step 1 content (signup/account creation)
  describe('Step 1 Content', () => {
    it('should have Step 1 mentioning signup or account creation', () => {
      renderHowItWorksSection()
      const stepItems = screen.getAllByTestId('step-item')
      const firstStep = within(stepItems[0])
      const title = firstStep.getByTestId('step-title')
      const description = firstStep.getByTestId('step-description')

      const titleText = title.textContent?.toLowerCase() || ''
      const descriptionText = description.textContent?.toLowerCase() || ''
      const step1Content = titleText + ' ' + descriptionText

      expect(step1Content).toMatch(/sign\s*up|account|create|register/i)
    })

    it('should display Step 1 with proper title', () => {
      renderHowItWorksSection()
      const stepTitles = screen.getAllByTestId('step-title')
      expect(stepTitles[0].textContent).toContain('Sign Up')
    })
  })

  // Test Case 6: Check Step 2 content (URL shortening)
  describe('Step 2 Content', () => {
    it('should have Step 2 mentioning URL shortening or pasting URL', () => {
      renderHowItWorksSection()
      const stepItems = screen.getAllByTestId('step-item')
      const secondStep = within(stepItems[1])
      const title = secondStep.getByTestId('step-title')
      const description = secondStep.getByTestId('step-description')

      const titleText = title.textContent?.toLowerCase() || ''
      const descriptionText = description.textContent?.toLowerCase() || ''
      const step2Content = titleText + ' ' + descriptionText

      expect(step2Content).toMatch(/url|shorten|paste|link/i)
    })

    it('should display Step 2 with URL-related title', () => {
      renderHowItWorksSection()
      const stepTitles = screen.getAllByTestId('step-title')
      expect(stepTitles[1].textContent).toMatch(/url|paste/i)
    })
  })

  // Test Case 7: Check Step 3 content (sharing/analytics tracking)
  describe('Step 3 Content', () => {
    it('should have Step 3 mentioning sharing or analytics tracking', () => {
      renderHowItWorksSection()
      const stepItems = screen.getAllByTestId('step-item')
      const thirdStep = within(stepItems[2])
      const title = thirdStep.getByTestId('step-title')
      const description = thirdStep.getByTestId('step-description')

      const titleText = title.textContent?.toLowerCase() || ''
      const descriptionText = description.textContent?.toLowerCase() || ''
      const step3Content = titleText + ' ' + descriptionText

      expect(step3Content).toMatch(/share|track|analytics/i)
    })

    it('should display Step 3 with sharing/tracking title', () => {
      renderHowItWorksSection()
      const stepTitles = screen.getAllByTestId('step-title')
      expect(stepTitles[2].textContent).toMatch(/share|track|analytics/i)
    })
  })

  // Test Case 8: Verify visual flow/sequence
  describe('Visual Flow and Sequence', () => {
    it('should have a visual connector between steps', () => {
      renderHowItWorksSection()
      const connector = screen.getByTestId('step-connector')
      expect(connector).toBeInTheDocument()
    })

    it('should have mobile connectors between steps', () => {
      renderHowItWorksSection()
      const mobileConnectors = screen.getAllByTestId('mobile-connector')
      // There should be 2 connectors (between step 1-2 and step 2-3)
      expect(mobileConnectors).toHaveLength(2)
    })

    it('should render steps in a grid layout container', () => {
      renderHowItWorksSection()
      const container = screen.getByTestId('steps-container')
      expect(container).toBeInTheDocument()
    })

    it('should mark connectors as aria-hidden for accessibility', () => {
      renderHowItWorksSection()
      const connector = screen.getByTestId('step-connector')
      expect(connector).toHaveAttribute('aria-hidden', 'true')
    })

    it('should display steps in logical order from 1 to 3', () => {
      renderHowItWorksSection()
      const stepNumbers = screen.getAllByTestId('step-number')

      // Verify order by checking DOM position and numbers
      expect(stepNumbers[0].textContent).toBe('1')
      expect(stepNumbers[1].textContent).toBe('2')
      expect(stepNumbers[2].textContent).toBe('3')
    })

    it('should render steps in a grid container for horizontal layout', () => {
      renderHowItWorksSection()
      const stepsGrid = screen.getByTestId('steps-grid')
      expect(stepsGrid).toBeInTheDocument()
      expect(stepsGrid).toHaveClass('grid')
      expect(stepsGrid).toHaveClass('md:grid-cols-3')
    })

    it('should have visual indicators that are aria-hidden for accessibility', () => {
      renderHowItWorksSection()
      const connector = screen.getByTestId('step-connector')
      expect(connector).toHaveAttribute('aria-hidden', 'true')

      const mobileConnectors = screen.getAllByTestId('mobile-connector')
      mobileConnectors.forEach((connector) => {
        expect(connector).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  // Additional tests for step structure
  describe('Step Structure', () => {
    it('should have an icon in each step', () => {
      renderHowItWorksSection()
      const stepIcons = screen.getAllByTestId('step-icon')
      expect(stepIcons).toHaveLength(3)

      stepIcons.forEach((iconContainer) => {
        const svg = iconContainer.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('should have a title for each step', () => {
      renderHowItWorksSection()
      const stepTitles = screen.getAllByTestId('step-title')
      expect(stepTitles).toHaveLength(3)

      stepTitles.forEach((title) => {
        expect(title.textContent?.trim()).not.toBe('')
      })
    })

    it('should have a description for each step', () => {
      renderHowItWorksSection()
      const stepDescriptions = screen.getAllByTestId('step-description')
      expect(stepDescriptions).toHaveLength(3)

      stepDescriptions.forEach((description) => {
        expect(description.textContent?.trim()).not.toBe('')
        // Description should be meaningful
        expect(description.textContent!.length).toBeGreaterThan(20)
      })
    })
  })

  // Accessibility tests
  describe('Accessibility', () => {
    it('should have proper heading hierarchy', () => {
      renderHowItWorksSection()
      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      expect(h2Elements).toHaveLength(1)
    })

    it('should have accessible section with aria-labelledby', () => {
      renderHowItWorksSection()
      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')
    })

    it('should have step icons marked as aria-hidden', () => {
      renderHowItWorksSection()
      const stepIcons = screen.getAllByTestId('step-icon')
      stepIcons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('should have h3 headings for step titles', () => {
      renderHowItWorksSection()
      const stepTitles = screen.getAllByTestId('step-title')
      stepTitles.forEach((title) => {
        expect(title.tagName).toBe('H3')
      })
    })

    it('should have proper text contrast classes', () => {
      renderHowItWorksSection()
      const titles = screen.getAllByTestId('step-title')
      const descriptions = screen.getAllByTestId('step-description')

      titles.forEach((title) => {
        expect(title).toHaveClass('text-base-content')
      })

      descriptions.forEach((desc) => {
        expect(desc).toHaveClass('text-base-content/70')
      })
    })
  })

  // Layout tests
  describe('Layout', () => {
    it('should render steps in a grid container', () => {
      renderHowItWorksSection()
      const grid = screen.getByTestId('steps-grid')
      expect(grid).toBeInTheDocument()
      expect(grid).toHaveClass('grid')
    })

    it('should have responsive grid classes', () => {
      renderHowItWorksSection()
      const grid = screen.getByTestId('steps-grid')
      expect(grid).toHaveClass('grid-cols-1')
      expect(grid).toHaveClass('md:grid-cols-3')
    })

    it('should have a steps container with relative positioning', () => {
      renderHowItWorksSection()
      const container = screen.getByTestId('steps-container')
      expect(container).toBeInTheDocument()
      expect(container).toHaveClass('relative')
    })
  })

  // Test with custom steps prop
  describe('Custom Steps', () => {
    it('should accept custom steps through props', () => {
      const customSteps: Step[] = [
        {
          id: 'custom-1',
          number: 1,
          title: 'Custom Step 1',
          description: 'Custom description 1',
          icon: <span>Icon1</span>,
        },
        {
          id: 'custom-2',
          number: 2,
          title: 'Custom Step 2',
          description: 'Custom description 2',
          icon: <span>Icon2</span>,
        },
        {
          id: 'custom-3',
          number: 3,
          title: 'Custom Step 3',
          description: 'Custom description 3',
          icon: <span>Icon3</span>,
        },
      ]

      renderHowItWorksSection({ steps: customSteps })

      const stepTitles = screen.getAllByTestId('step-title')
      expect(stepTitles[0].textContent).toBe('Custom Step 1')
      expect(stepTitles[1].textContent).toBe('Custom Step 2')
      expect(stepTitles[2].textContent).toBe('Custom Step 3')
    })
  })
})
