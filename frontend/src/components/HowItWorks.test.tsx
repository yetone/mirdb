import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import HowItWorks from './HowItWorks'

describe('HowItWorks Section', () => {
  // Test Case 1: Section with heading 'How It Works' exists
  it('should render the How It Works section with correct heading', () => {
    render(<HowItWorks />)

    const section = screen.getByRole('region', { name: /how it works/i })
    expect(section).toBeInTheDocument()

    const heading = screen.getByRole('heading', { name: /how it works/i, level: 2 })
    expect(heading).toBeInTheDocument()
  })

  // Test Case 2: At least 3-4 step elements are rendered with step numbers or icons
  it('should render at least 3-4 step elements with step numbers', () => {
    render(<HowItWorks />)

    const stepsContainer = screen.getByTestId('how-it-works-steps')
    expect(stepsContainer).toBeInTheDocument()

    // Check for step badges
    const step1 = screen.getByText('Step 1')
    const step2 = screen.getByText('Step 2')
    const step3 = screen.getByText('Step 3')
    const step4 = screen.getByText('Step 4')

    expect(step1).toBeInTheDocument()
    expect(step2).toBeInTheDocument()
    expect(step3).toBeInTheDocument()
    expect(step4).toBeInTheDocument()

    // Verify step test IDs exist
    expect(screen.getByTestId('step-1')).toBeInTheDocument()
    expect(screen.getByTestId('step-2')).toBeInTheDocument()
    expect(screen.getByTestId('step-3')).toBeInTheDocument()
    expect(screen.getByTestId('step-4')).toBeInTheDocument()
  })

  // Test Case 3: First step mentions creating an account or signing up
  it('should have first step mentioning account creation or signing up', () => {
    render(<HowItWorks />)

    const step1Element = screen.getByTestId('step-1')
    expect(step1Element).toBeInTheDocument()

    // Check for account creation content
    const accountHeading = screen.getByRole('heading', { name: /create your account/i })
    expect(accountHeading).toBeInTheDocument()

    // Check for sign up mention in description
    const signUpText = screen.getByText(/sign up/i)
    expect(signUpText).toBeInTheDocument()
  })

  // Test Case 4: Step exists mentioning pasting or entering a long URL
  it('should have a step mentioning pasting or entering a long URL', () => {
    render(<HowItWorks />)

    // Check for URL pasting step
    const urlHeading = screen.getByRole('heading', { name: /paste your long url/i })
    expect(urlHeading).toBeInTheDocument()

    // Check for enter/paste URL mention in description
    const urlDescription = screen.getByText(/enter any long url/i)
    expect(urlDescription).toBeInTheDocument()
  })

  // Test Case 5: Step exists mentioning sharing the shortened link
  it('should have a step mentioning sharing the shortened link', () => {
    render(<HowItWorks />)

    // Check for sharing step
    const shareHeading = screen.getByRole('heading', { name: /share your short link/i })
    expect(shareHeading).toBeInTheDocument()

    // Check for share mention in description
    const shareDescription = screen.getByText(/share it anywhere/i)
    expect(shareDescription).toBeInTheDocument()
  })

  // Test Case 6: Step exists mentioning tracking performance or analytics
  it('should have a step mentioning tracking performance or analytics', () => {
    render(<HowItWorks />)

    // Check for tracking step
    const trackHeading = screen.getByRole('heading', { name: /track performance/i })
    expect(trackHeading).toBeInTheDocument()

    // Check for analytics mention in description
    const analyticsDescription = screen.getByText(/analytics/i)
    expect(analyticsDescription).toBeInTheDocument()

    // Check for clicks tracking mention
    const clicksDescription = screen.getByText(/monitor clicks/i)
    expect(clicksDescription).toBeInTheDocument()
  })

  // Additional accessibility tests
  it('should have proper accessibility attributes', () => {
    render(<HowItWorks />)

    // Section should have aria-labelledby
    const section = screen.getByRole('region', { name: /how it works/i })
    expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')

    // Heading should have correct id
    const heading = screen.getByRole('heading', { name: /how it works/i, level: 2 })
    expect(heading).toHaveAttribute('id', 'how-it-works-heading')
  })

  // Test steps are in correct order
  it('should display steps in the correct order', () => {
    render(<HowItWorks />)

    const headings = screen.getAllByRole('heading', { level: 3 })
    const stepTitles = headings.map(h => h.textContent)

    expect(stepTitles).toEqual([
      'Create Your Account',
      'Paste Your Long URL',
      'Share Your Short Link',
      'Track Performance',
    ])
  })
})
