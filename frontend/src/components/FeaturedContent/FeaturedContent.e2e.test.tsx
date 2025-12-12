import { describe, it, expect } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { FeaturedContent } from './FeaturedContent'
import { DetailPage } from '../../pages/DetailPage'
import type { FeaturedItem } from '../../types/FeaturedContent.types'

// Sample test data for E2E tests
const featuredItems: FeaturedItem[] = [
  {
    id: '1',
    title: 'Getting Started Guide',
    description: 'Learn how to get started with our platform',
    imageUrl: '/images/getting-started.jpg',
    imageAlt: 'Getting started illustration',
    detailUrl: '/detail/1',
  },
  {
    id: '2',
    title: 'Best Practices',
    description: 'Discover best practices and tips',
    imageUrl: '/images/best-practices.jpg',
    imageAlt: 'Best practices illustration',
    detailUrl: '/detail/2',
  },
]

// Helper to render the full application with routing
const renderApp = (initialPath = '/') => {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <Routes>
        <Route
          path="/"
          element={
            <main>
              <section className="hero-section" data-testid="hero-section">
                <h1>Welcome to Our Platform</h1>
              </section>
              <FeaturedContent
                items={featuredItems}
                sectionTitle="Featured Highlights"
              />
            </main>
          }
        />
        <Route path="/detail/:id" element={<DetailPage />} />
      </Routes>
    </MemoryRouter>
  )
}

/**
 * Test Case 2 (E2E): Click on a featured content item
 * Expected: User is navigated to the detail page for that item
 */
describe('Featured Content E2E - User Navigation Flow', () => {
  it('navigates to detail page when clicking featured item', async () => {
    const user = userEvent.setup()
    renderApp()

    // Step 1: Verify homepage loads with hero section
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()

    // Step 2: Verify featured content section is visible below hero
    expect(screen.getByTestId('featured-content-section')).toBeInTheDocument()
    expect(screen.getByText('Featured Highlights')).toBeInTheDocument()

    // Step 3: Verify featured content cards are displayed
    expect(screen.getByTestId('featured-card-1')).toBeInTheDocument()
    expect(screen.getByTestId('featured-card-2')).toBeInTheDocument()

    // Step 4: Click on the first featured item
    const firstItemLink = screen.getByRole('link', { name: /View details for Getting Started Guide/i })
    await user.click(firstItemLink)

    // Verify navigation to detail page
    await waitFor(() => {
      expect(screen.getByTestId('detail-page')).toBeInTheDocument()
    })

    // Verify the correct item detail is displayed
    expect(screen.getByTestId('detail-page-id')).toHaveTextContent('Item ID: 1')
  })

  it('navigates to correct detail page for second featured item', async () => {
    const user = userEvent.setup()
    renderApp()

    // Click on the second featured item
    const secondItemLink = screen.getByRole('link', { name: /View details for Best Practices/i })
    await user.click(secondItemLink)

    // Verify navigation to correct detail page
    await waitFor(() => {
      expect(screen.getByTestId('detail-page')).toBeInTheDocument()
    })

    expect(screen.getByTestId('detail-page-id')).toHaveTextContent('Item ID: 2')
  })

  it('can navigate back to homepage from detail page', async () => {
    const user = userEvent.setup()
    renderApp()

    // Navigate to detail page
    const firstItemLink = screen.getByRole('link', { name: /View details for Getting Started Guide/i })
    await user.click(firstItemLink)

    // Wait for detail page to load
    await waitFor(() => {
      expect(screen.getByTestId('detail-page')).toBeInTheDocument()
    })

    // Click back to home link
    const backLink = screen.getByTestId('back-to-home')
    await user.click(backLink)

    // Verify we're back on homepage
    await waitFor(() => {
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    expect(screen.getByTestId('featured-content-section')).toBeInTheDocument()
  })

  it('featured content displays engaging visuals (images)', () => {
    renderApp()

    // Verify images are present with proper attributes
    const images = screen.getAllByRole('img')
    expect(images).toHaveLength(featuredItems.length)

    images.forEach((img, index) => {
      expect(img).toHaveAttribute('src', featuredItems[index].imageUrl)
      expect(img).toHaveAttribute('alt', featuredItems[index].imageAlt)
    })
  })

  it('featured content displays descriptions', () => {
    renderApp()

    // Verify descriptions are displayed
    featuredItems.forEach((item) => {
      expect(screen.getByText(item.description)).toBeInTheDocument()
    })
  })

  it('featured content section is visible after hero section', () => {
    renderApp()

    // Both sections should be rendered in the correct order
    const heroSection = screen.getByTestId('hero-section')
    const featuredSection = screen.getByTestId('featured-content-section')

    expect(heroSection).toBeInTheDocument()
    expect(featuredSection).toBeInTheDocument()

    // Check DOM order (featured comes after hero)
    const main = heroSection.parentElement
    const children = Array.from(main?.children || [])
    const heroIndex = children.indexOf(heroSection)
    const featuredIndex = children.indexOf(featuredSection)

    expect(featuredIndex).toBeGreaterThan(heroIndex)
  })
})

/**
 * Complete User Journey Test
 * Tests the full scenario from PRD US-5: Discover Featured Content
 */
describe('US-5: Discover Featured Content - Complete User Journey', () => {
  it('completes the full discovery flow as defined in the scenario steps', async () => {
    const user = userEvent.setup()
    renderApp()

    // Step 1: Load homepage - User is on the homepage
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()

    // Step 2: Scroll past hero section - Hero section is visible at top of page
    // (In unit tests, we verify the structure; actual scrolling tested in browser)
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Step 3: Verify featured content section - Check that featured content cards/grid are displayed
    const featuredSection = screen.getByTestId('featured-content-section')
    expect(featuredSection).toBeInTheDocument()

    const featuredGrid = screen.getByTestId('featured-content-grid')
    expect(featuredGrid).toBeInTheDocument()

    // Verify cards are in the grid
    const cards = screen.getAllByTestId(/^featured-card-/)
    expect(cards.length).toBe(2)

    // Step 4: Test featured item interaction - Click on a featured item to verify it links to more details
    const featuredCard = screen.getByTestId('featured-card-1')
    const link = featuredCard.querySelector('a')
    expect(link).toHaveAttribute('href', '/detail/1')

    await user.click(link!)

    // Verify navigation to detail page
    await waitFor(() => {
      expect(screen.getByTestId('detail-page')).toBeInTheDocument()
    })

    // The user has successfully discovered featured content and navigated to details
  })
})
