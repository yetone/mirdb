import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import userEvent from '@testing-library/user-event'
import FinalCTASection from './FinalCTASection'

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
  removeItem: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

const renderWithRouter = (component: React.ReactElement, { initialEntries = ['/'] } = {}) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route path="/" element={component} />
        <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
      </Routes>
    </MemoryRouter>
  )
}

describe('FinalCTASection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
  })

  // Test Case 1: Section exists with compelling message about getting started
  it('renders final CTA section with compelling message about getting started', () => {
    renderWithRouter(<FinalCTASection />)

    const finalCtaSection = screen.getByTestId('final-cta-section')
    expect(finalCtaSection).toBeInTheDocument()

    // Check for compelling message that reinforces the value proposition
    const sectionText = finalCtaSection.textContent?.toLowerCase() || ''
    expect(sectionText).toMatch(/ready|start|shorten|track|today|now/i)
  })

  // Test Case 2: Click 'Start Shortening URLs Today' button navigates to /register
  it('navigates to /register when clicking Start Shortening URLs Today button', async () => {
    const user = userEvent.setup()
    renderWithRouter(<FinalCTASection />)

    const ctaButton = screen.getByTestId('final-cta-button')
    expect(ctaButton).toBeInTheDocument()
    expect(ctaButton.textContent).toContain('Start Shortening URLs Today')
    expect(ctaButton).toHaveAttribute('href', '/register')

    // Click and verify navigation
    await user.click(ctaButton)
    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })

  // Test Case 3: Button is prominently styled and clearly visible
  it('renders a prominently styled and clearly visible CTA button', () => {
    renderWithRouter(<FinalCTASection />)

    const ctaButton = screen.getByTestId('final-cta-button')
    expect(ctaButton).toBeInTheDocument()

    // Verify the button has prominent styling (btn-primary and btn-lg classes from DaisyUI)
    expect(ctaButton).toHaveClass('btn')
    expect(ctaButton).toHaveClass('btn-primary')
    expect(ctaButton).toHaveClass('btn-lg')
  })

  // Additional test: Section contains a value proposition heading
  it('displays a value proposition heading', () => {
    renderWithRouter(<FinalCTASection />)

    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
    // The heading should contain a compelling message about starting
    expect(heading.textContent?.toLowerCase()).toMatch(/ready|start|track|links/i)
  })

  // Additional test: Section has proper section ID for anchor navigation
  it('has proper section ID for anchor navigation', () => {
    renderWithRouter(<FinalCTASection />)

    const section = screen.getByTestId('final-cta-section')
    expect(section).toHaveAttribute('id', 'final-cta')
  })

  // Additional test: Subheadline reinforces value proposition
  it('has a subheadline that reinforces the value proposition', () => {
    renderWithRouter(<FinalCTASection />)

    const subheadline = screen.getByTestId('final-cta-subheadline')
    expect(subheadline).toBeInTheDocument()
    // Should mention benefits like analytics, free, or getting started
    const subText = subheadline.textContent?.toLowerCase() || ''
    expect(subText).toMatch(/analytic|free|account|click|track/i)
  })
})
