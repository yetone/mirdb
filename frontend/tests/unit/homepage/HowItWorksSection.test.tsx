/**
 * HowItWorksSection Unit Tests
 * Owner: Scenario 5 - How It Works Section
 *
 * Tests for the HowItWorksSection component to verify:
 * - Section renders with heading
 * - Three steps are displayed
 * - Each step has correct content (Create, Share, Track)
 * - Accessibility requirements are met
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from './setup'
import { HowItWorksSection } from '@/components/homepage/HowItWorksSection'

describe('HowItWorksSection', () => {
  // Test Case 1: Section exists with heading 'How It Works'
  it('renders the section with "How It Works" heading', () => {
    render(<HowItWorksSection />)

    const section = screen.getByTestId('how-it-works-section')
    expect(section).toBeInTheDocument()

    const heading = screen.getByTestId('how-it-works-heading')
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent('How It Works')
  })

  // Test Case 2: Exactly 3 step elements are rendered
  it('renders exactly 3 step elements', () => {
    render(<HowItWorksSection />)

    const stepsContainer = screen.getByTestId('how-it-works-steps')
    expect(stepsContainer).toBeInTheDocument()

    const step1 = screen.getByTestId('step-1')
    const step2 = screen.getByTestId('step-2')
    const step3 = screen.getByTestId('step-3')

    expect(step1).toBeInTheDocument()
    expect(step2).toBeInTheDocument()
    expect(step3).toBeInTheDocument()

    // Verify only 3 steps exist
    const allSteps = [step1, step2, step3]
    expect(allSteps).toHaveLength(3)
  })

  // Test Case 3: First step mentions creating/pasting URL
  it('first step mentions creating/pasting URL', () => {
    render(<HowItWorksSection />)

    const step1 = screen.getByTestId('step-1')
    const title = within(step1).getByTestId('step-1-title')
    const description = within(step1).getByTestId('step-1-description')

    expect(title).toHaveTextContent('Create')
    expect(description).toHaveTextContent(/paste.*url|url.*paste/i)
  })

  // Test Case 4: Second step mentions sharing the short link
  it('second step mentions sharing the short link', () => {
    render(<HowItWorksSection />)

    const step2 = screen.getByTestId('step-2')
    const title = within(step2).getByTestId('step-2-title')
    const description = within(step2).getByTestId('step-2-description')

    expect(title).toHaveTextContent('Share')
    expect(description).toHaveTextContent(/share.*link|short.*link/i)
  })

  // Test Case 5: Third step mentions tracking/analytics
  it('third step mentions tracking/analytics', () => {
    render(<HowItWorksSection />)

    const step3 = screen.getByTestId('step-3')
    const title = within(step3).getByTestId('step-3-title')
    const description = within(step3).getByTestId('step-3-description')

    expect(title).toHaveTextContent('Track')
    expect(description).toHaveTextContent(/track|analytic|clicks|monitor|performance/i)
  })

  // Additional accessibility tests
  it('has accessible heading structure', () => {
    render(<HowItWorksSection />)

    const heading = screen.getByRole('heading', { level: 2, name: /how it works/i })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveAttribute('id', 'how-it-works-heading')
  })

  it('section has proper aria-labelledby attribute', () => {
    render(<HowItWorksSection />)

    const section = screen.getByTestId('how-it-works-section')
    expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')
  })

  it('each step has an icon', () => {
    render(<HowItWorksSection />)

    const step1Icon = screen.getByTestId('step-1-icon')
    const step2Icon = screen.getByTestId('step-2-icon')
    const step3Icon = screen.getByTestId('step-3-icon')

    expect(step1Icon).toBeInTheDocument()
    expect(step2Icon).toBeInTheDocument()
    expect(step3Icon).toBeInTheDocument()
  })

  it('each step has a step number displayed', () => {
    render(<HowItWorksSection />)

    const step1Number = screen.getByTestId('step-1-number')
    const step2Number = screen.getByTestId('step-2-number')
    const step3Number = screen.getByTestId('step-3-number')

    expect(step1Number).toHaveTextContent('1')
    expect(step2Number).toHaveTextContent('2')
    expect(step3Number).toHaveTextContent('3')
  })

  it('section has id for anchor navigation', () => {
    render(<HowItWorksSection />)

    const section = screen.getByTestId('how-it-works-section')
    expect(section).toHaveAttribute('id', 'how-it-works')
  })

  it('step titles are rendered as h3 elements', () => {
    render(<HowItWorksSection />)

    const step1Title = screen.getByTestId('step-1-title')
    const step2Title = screen.getByTestId('step-2-title')
    const step3Title = screen.getByTestId('step-3-title')

    expect(step1Title.tagName).toBe('H3')
    expect(step2Title.tagName).toBe('H3')
    expect(step3Title.tagName).toBe('H3')
  })
})
