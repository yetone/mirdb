import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Footer from './Footer'

const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <MemoryRouter>
      {component}
    </MemoryRouter>
  )
}

describe('Footer Section Display', () => {
  // Test Case 1: Footer element exists in the DOM
  it('renders footer element in the DOM', () => {
    renderWithRouter(<Footer />)

    const footer = screen.getByTestId('footer-section')
    expect(footer).toBeInTheDocument()
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  // Test Case 2: Logo is displayed in footer
  it('displays logo in footer', () => {
    renderWithRouter(<Footer />)

    const logo = screen.getByTestId('footer-logo')
    expect(logo).toBeInTheDocument()
  })

  // Test Case 3: Copyright notice with current year is displayed
  it('displays copyright notice with current year', () => {
    renderWithRouter(<Footer />)

    const copyright = screen.getByTestId('footer-copyright')
    expect(copyright).toBeInTheDocument()

    const currentYear = new Date().getFullYear().toString()
    expect(copyright.textContent).toContain(currentYear)
    expect(copyright.textContent?.toLowerCase()).toContain('copyright')
  })

  // Test Case 4: Footer contains navigation links
  it('contains navigation links in footer', () => {
    renderWithRouter(<Footer />)

    const linksContainer = screen.getByTestId('footer-links')
    expect(linksContainer).toBeInTheDocument()

    // Check for at least one link
    const links = linksContainer.querySelectorAll('a')
    expect(links.length).toBeGreaterThan(0)
  })
})
