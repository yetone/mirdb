import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { HeroSection } from '../../src/components/homepage/HeroSection'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, whileHover, whileTap, initial, animate, transition, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    button: ({ children, whileHover, whileTap, initial, animate, transition, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
  },
}))

// Mock Register page component for integration testing
const MockRegisterPage = () => (
  <div data-testid="register-page">
    <h1>Register</h1>
    <p>Create your account</p>
  </div>
)

describe('Homepage Navigation Integration', () => {
  describe('CTA Button Navigation', () => {
    it('should navigate to /register page when primary CTA is clicked', async () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<HeroSection />} />
            <Route path="/register" element={<MockRegisterPage />} />
          </Routes>
        </MemoryRouter>
      )

      // Verify we start on the homepage with hero section
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Click the primary CTA button
      const ctaButton = screen.getByTestId('hero-cta-primary')
      fireEvent.click(ctaButton)

      // Verify navigation to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })

    it('should navigate correctly with custom CTA text', async () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<HeroSection ctaText="Sign Up Now" />} />
            <Route path="/register" element={<MockRegisterPage />} />
          </Routes>
        </MemoryRouter>
      )

      const ctaButton = screen.getByText('Sign Up Now')
      expect(ctaButton).toBeInTheDocument()

      fireEvent.click(ctaButton)

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })

    it('should maintain accessible navigation flow', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Routes>
            <Route path="/" element={<HeroSection />} />
            <Route path="/register" element={<MockRegisterPage />} />
          </Routes>
        </MemoryRouter>
      )

      const ctaButton = screen.getByTestId('hero-cta-primary')

      // Button should be keyboard accessible
      expect(ctaButton.getAttribute('type') || 'button').toBe('button')
      expect(ctaButton).not.toBeDisabled()

      // Button should have visible text for accessibility
      expect(ctaButton).toHaveTextContent('Get Started')
    })
  })

  describe('Secondary CTA Behavior', () => {
    it('should scroll to features section when secondary CTA is clicked', () => {
      // Create a mock features section
      const mockScrollIntoView = vi.fn()
      const featuresSection = document.createElement('div')
      featuresSection.id = 'features'
      featuresSection.scrollIntoView = mockScrollIntoView
      document.body.appendChild(featuresSection)

      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const secondaryCta = screen.getByTestId('hero-cta-secondary')
      fireEvent.click(secondaryCta)

      expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })

      // Cleanup
      document.body.removeChild(featuresSection)
    })
  })
})
