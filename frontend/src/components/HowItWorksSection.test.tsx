import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import HowItWorksSection from './HowItWorksSection'

const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      {component}
    </MemoryRouter>
  )
}

describe('HowItWorksSection', () => {
  // Test Case 1: Section exists with heading 'How It Works'
  it('renders section with How It Works heading', () => {
    renderWithRouter(<HowItWorksSection />)

    const section = screen.getByTestId('how-it-works-section')
    expect(section).toBeInTheDocument()

    const heading = screen.getByRole('heading', { name: /how it works/i })
    expect(heading).toBeInTheDocument()
  })

  // Test Case 2: Step 1 describes pasting/entering a long URL
  it('renders Step 1 describing pasting or entering a long URL', () => {
    renderWithRouter(<HowItWorksSection />)

    const step1 = screen.getByTestId('step-1')
    expect(step1).toBeInTheDocument()

    // Step 1 should describe pasting/entering a URL
    const step1Content = step1.textContent?.toLowerCase() || ''
    expect(step1Content).toMatch(/paste|enter/)
    expect(step1Content).toMatch(/url/)
  })

  // Test Case 3: Step 2 describes getting the shortened link
  it('renders Step 2 describing getting the shortened link', () => {
    renderWithRouter(<HowItWorksSection />)

    const step2 = screen.getByTestId('step-2')
    expect(step2).toBeInTheDocument()

    // Step 2 should describe getting the short link
    const step2Content = step2.textContent?.toLowerCase() || ''
    expect(step2Content).toMatch(/get|receive|copy/)
    expect(step2Content).toMatch(/short|link/)
  })

  // Test Case 4: Step 3 describes tracking clicks and analyzing performance
  it('renders Step 3 describing tracking clicks and analyzing performance', () => {
    renderWithRouter(<HowItWorksSection />)

    const step3 = screen.getByTestId('step-3')
    expect(step3).toBeInTheDocument()

    // Step 3 should describe tracking and analytics
    const step3Content = step3.textContent?.toLowerCase() || ''
    expect(step3Content).toMatch(/track|analyz|monitor/)
    expect(step3Content).toMatch(/click|performance|stats/)
  })

  // Test Case 5: Exactly 3 steps are displayed
  it('displays exactly 3 steps', () => {
    renderWithRouter(<HowItWorksSection />)

    const section = screen.getByTestId('how-it-works-section')

    // Query for all step elements
    const step1 = within(section).getByTestId('step-1')
    const step2 = within(section).getByTestId('step-2')
    const step3 = within(section).getByTestId('step-3')

    expect(step1).toBeInTheDocument()
    expect(step2).toBeInTheDocument()
    expect(step3).toBeInTheDocument()

    // Verify only 3 steps exist (no step-4)
    const step4 = within(section).queryByTestId('step-4')
    expect(step4).not.toBeInTheDocument()

    // Additional check: count elements with step- testid pattern
    const allSteps = section.querySelectorAll('[data-testid^="step-"]')
    expect(allSteps).toHaveLength(3)
  })
})
