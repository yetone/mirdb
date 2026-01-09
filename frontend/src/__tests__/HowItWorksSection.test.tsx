import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HowItWorksSection from '../components/HowItWorksSection'
import Home from '../pages/Home'

describe('HowItWorksSection', () => {
  // Test Case 1: Component renders with exactly 3 steps displayed
  describe('Test Case 1: Component renders with exactly 3 steps', () => {
    it('renders the HowItWorksSection component with exactly 3 steps displayed', () => {
      render(<HowItWorksSection />)

      // Check for section heading
      expect(screen.getByText('How It Works')).toBeInTheDocument()

      // Check for exactly 3 steps
      const step1 = screen.getByTestId('step-1')
      const step2 = screen.getByTestId('step-2')
      const step3 = screen.getByTestId('step-3')

      expect(step1).toBeInTheDocument()
      expect(step2).toBeInTheDocument()
      expect(step3).toBeInTheDocument()

      // Verify no 4th step exists
      expect(screen.queryByTestId('step-4')).not.toBeInTheDocument()
    })

    it('renders all steps with role="listitem"', () => {
      render(<HowItWorksSection />)

      const listItems = screen.getAllByRole('listitem')
      expect(listItems).toHaveLength(3)
    })
  })

  // Test Case 2: Step 1 displays 'Paste your long URL' with visual icon
  describe('Test Case 2: Step 1 content', () => {
    it('displays Step 1 with "Paste Your Long URL" instruction', () => {
      render(<HowItWorksSection />)

      const step1Title = screen.getByTestId('step-title-1')
      expect(step1Title).toHaveTextContent(/paste your long url/i)
    })

    it('has a visual icon for Step 1', () => {
      render(<HowItWorksSection />)

      const step1Icon = screen.getByTestId('step-icon-1')
      expect(step1Icon).toBeInTheDocument()

      // Check icon contains SVG element
      const svg = step1Icon.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })

    it('has a description for Step 1', () => {
      render(<HowItWorksSection />)

      const step1Description = screen.getByTestId('step-description-1')
      expect(step1Description).toHaveTextContent(/copy any long url/i)
    })
  })

  // Test Case 3: Step 2 displays 'Get a short, shareable link' with visual icon
  describe('Test Case 3: Step 2 content', () => {
    it('displays Step 2 with "Get a Short, Shareable Link" instruction', () => {
      render(<HowItWorksSection />)

      const step2Title = screen.getByTestId('step-title-2')
      expect(step2Title).toHaveTextContent(/get a short.*shareable.*link/i)
    })

    it('has a visual icon for Step 2', () => {
      render(<HowItWorksSection />)

      const step2Icon = screen.getByTestId('step-icon-2')
      expect(step2Icon).toBeInTheDocument()

      // Check icon contains SVG element
      const svg = step2Icon.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })

    it('has a description for Step 2', () => {
      render(<HowItWorksSection />)

      const step2Description = screen.getByTestId('step-description-2')
      expect(step2Description).toHaveTextContent(/shortened url/i)
    })
  })

  // Test Case 4: Step 3 displays 'Track clicks and analytics' with visual icon
  describe('Test Case 4: Step 3 content', () => {
    it('displays Step 3 with "Track Clicks and Analytics" instruction', () => {
      render(<HowItWorksSection />)

      const step3Title = screen.getByTestId('step-title-3')
      expect(step3Title).toHaveTextContent(/track.*clicks.*analytics/i)
    })

    it('has a visual icon for Step 3', () => {
      render(<HowItWorksSection />)

      const step3Icon = screen.getByTestId('step-icon-3')
      expect(step3Icon).toBeInTheDocument()

      // Check icon contains SVG element
      const svg = step3Icon.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })

    it('has a description for Step 3', () => {
      render(<HowItWorksSection />)

      const step3Description = screen.getByTestId('step-description-3')
      expect(step3Description).toHaveTextContent(/monitor.*link.*analytics/i)
    })
  })

  // Test Case 5: Each step shows a number (1, 2, 3) or clear sequential indicator
  describe('Test Case 5: Step numbers are displayed', () => {
    it('displays step number 1', () => {
      render(<HowItWorksSection />)

      const stepNumber1 = screen.getByTestId('step-number-1')
      expect(stepNumber1).toHaveTextContent('1')
    })

    it('displays step number 2', () => {
      render(<HowItWorksSection />)

      const stepNumber2 = screen.getByTestId('step-number-2')
      expect(stepNumber2).toHaveTextContent('2')
    })

    it('displays step number 3', () => {
      render(<HowItWorksSection />)

      const stepNumber3 = screen.getByTestId('step-number-3')
      expect(stepNumber3).toHaveTextContent('3')
    })

    it('all step numbers are visible in sequential order', () => {
      render(<HowItWorksSection />)

      const stepNumbers = [
        screen.getByTestId('step-number-1'),
        screen.getByTestId('step-number-2'),
        screen.getByTestId('step-number-3'),
      ]

      stepNumbers.forEach((stepNumber, index) => {
        expect(stepNumber).toHaveTextContent(String(index + 1))
        expect(stepNumber).toHaveAttribute('aria-label', `Step ${index + 1}`)
      })
    })
  })

  // Test Case 6: Navigate to How It Works section via anchor link (E2E-style test)
  describe('Test Case 6: Navigation to How It Works section', () => {
    it('has the section with correct id for anchor navigation', () => {
      render(<HowItWorksSection />)

      const section = screen.getByRole('region', { name: /how it works/i })
      expect(section).toHaveAttribute('id', 'how-it-works')
    })

    it('section is accessible via anchor link when rendered in Home page', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // Check the navigation link exists
      const navLink = screen.getByTestId('nav-how-it-works')
      expect(navLink).toBeInTheDocument()
      expect(navLink).toHaveTextContent(/how it works/i)

      // Check the section exists with the correct id
      const section = document.getElementById('how-it-works')
      expect(section).toBeInTheDocument()
    })

    it('clicking the nav link calls scrollIntoView for smooth scrolling', () => {
      // Mock scrollIntoView
      const scrollIntoViewMock = vi.fn()
      Element.prototype.scrollIntoView = scrollIntoViewMock

      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      const navLink = screen.getByTestId('nav-how-it-works')
      fireEvent.click(navLink)

      // Verify scrollIntoView was called with smooth behavior
      expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' })
    })
  })

  // Additional accessibility tests
  describe('Accessibility', () => {
    it('has proper heading structure', () => {
      render(<HowItWorksSection />)

      const heading = screen.getByRole('heading', { name: /how it works/i })
      expect(heading).toBeInTheDocument()
      expect(heading.tagName).toBe('H2')
    })

    it('section has proper aria-labelledby', () => {
      render(<HowItWorksSection />)

      const section = screen.getByRole('region')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')
    })

    it('icons have aria-hidden attribute', () => {
      render(<HowItWorksSection />)

      const icons = document.querySelectorAll('[data-testid^="step-icon-"] svg')
      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })
})
