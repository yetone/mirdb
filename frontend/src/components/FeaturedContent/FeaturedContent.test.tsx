import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { FeaturedContent } from './FeaturedContent'
import { FeaturedCard } from './FeaturedCard'
import type { FeaturedItem } from '../../types/FeaturedContent.types'

// Sample test data
const sampleFeaturedItems: FeaturedItem[] = [
  {
    id: '1',
    title: 'Test Item 1',
    description: 'This is the first test item description',
    imageUrl: '/images/test1.jpg',
    imageAlt: 'Test image 1',
    detailUrl: '/detail/1',
  },
  {
    id: '2',
    title: 'Test Item 2',
    description: 'This is the second test item description',
    imageUrl: '/images/test2.jpg',
    imageAlt: 'Test image 2',
    detailUrl: '/detail/2',
  },
  {
    id: '3',
    title: 'Test Item 3',
    description: 'This is the third test item description',
    imageUrl: '/images/test3.jpg',
    imageAlt: 'Test image 3',
    detailUrl: '/detail/3',
  },
]

// Helper to render with Router
const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <BrowserRouter>{component}</BrowserRouter>
  )
}

/**
 * Test Case 1: Render FeaturedContent component with sample data
 * Expected: Featured content cards are rendered with images and descriptions
 */
describe('FeaturedContent Component - Rendering with Data', () => {
  it('renders featured content section with correct structure', () => {
    renderWithRouter(<FeaturedContent items={sampleFeaturedItems} />)

    // Verify section is rendered
    const section = screen.getByTestId('featured-content-section')
    expect(section).toBeInTheDocument()

    // Verify grid is rendered
    const grid = screen.getByTestId('featured-content-grid')
    expect(grid).toBeInTheDocument()
  })

  it('renders all featured content cards with images', () => {
    renderWithRouter(<FeaturedContent items={sampleFeaturedItems} />)

    // Verify all cards are rendered
    sampleFeaturedItems.forEach((item) => {
      const card = screen.getByTestId(`featured-card-${item.id}`)
      expect(card).toBeInTheDocument()

      // Verify image is rendered with correct alt text
      const image = screen.getByAltText(item.imageAlt)
      expect(image).toBeInTheDocument()
      expect(image).toHaveAttribute('src', item.imageUrl)
    })
  })

  it('renders all featured content cards with descriptions', () => {
    renderWithRouter(<FeaturedContent items={sampleFeaturedItems} />)

    // Verify descriptions are rendered
    sampleFeaturedItems.forEach((item) => {
      expect(screen.getByText(item.description)).toBeInTheDocument()
    })
  })

  it('renders all featured content cards with titles', () => {
    renderWithRouter(<FeaturedContent items={sampleFeaturedItems} />)

    // Verify titles are rendered
    sampleFeaturedItems.forEach((item) => {
      expect(screen.getByText(item.title)).toBeInTheDocument()
    })
  })

  it('renders custom section title when provided', () => {
    const customTitle = 'Our Featured Highlights'
    renderWithRouter(
      <FeaturedContent items={sampleFeaturedItems} sectionTitle={customTitle} />
    )

    expect(screen.getByText(customTitle)).toBeInTheDocument()
  })

  it('renders default section title when not provided', () => {
    renderWithRouter(<FeaturedContent items={sampleFeaturedItems} />)

    expect(screen.getByText('Featured Content')).toBeInTheDocument()
  })

  it('renders cards with proper accessibility attributes', () => {
    renderWithRouter(<FeaturedContent items={sampleFeaturedItems} />)

    // Verify section has aria-label
    const section = screen.getByTestId('featured-content-section')
    expect(section).toHaveAttribute('aria-label')

    // Verify grid has role="list"
    const grid = screen.getByTestId('featured-content-grid')
    expect(grid).toHaveAttribute('role', 'list')

    // Verify cards have proper roles
    sampleFeaturedItems.forEach((item) => {
      const card = screen.getByTestId(`featured-card-${item.id}`)
      expect(card).toHaveAttribute('role', 'article')
    })
  })
})

/**
 * Test Case 3: Render FeaturedContent with empty data array
 * Expected: Component handles empty state gracefully without errors
 */
describe('FeaturedContent Component - Empty State Handling', () => {
  it('handles empty array gracefully without errors', () => {
    // Should not throw any errors
    expect(() => {
      renderWithRouter(<FeaturedContent items={[]} />)
    }).not.toThrow()
  })

  it('renders section even with empty items', () => {
    renderWithRouter(<FeaturedContent items={[]} />)

    const section = screen.getByTestId('featured-content-section')
    expect(section).toBeInTheDocument()
  })

  it('displays empty state message when items array is empty', () => {
    renderWithRouter(<FeaturedContent items={[]} />)

    const emptyState = screen.getByTestId('featured-content-empty')
    expect(emptyState).toBeInTheDocument()
    expect(screen.getByText('No featured content available at this time.')).toBeInTheDocument()
  })

  it('does not render grid when items array is empty', () => {
    renderWithRouter(<FeaturedContent items={[]} />)

    const grid = screen.queryByTestId('featured-content-grid')
    expect(grid).not.toBeInTheDocument()
  })

  it('still renders section title with empty items', () => {
    renderWithRouter(<FeaturedContent items={[]} sectionTitle="Featured Content" />)

    expect(screen.getByText('Featured Content')).toBeInTheDocument()
  })

  it('empty state has proper accessibility attributes', () => {
    renderWithRouter(<FeaturedContent items={[]} />)

    const emptyState = screen.getByTestId('featured-content-empty')
    expect(emptyState).toHaveAttribute('role', 'status')
    expect(emptyState).toHaveAttribute('aria-live', 'polite')
  })
})

/**
 * FeaturedCard Component Tests
 */
describe('FeaturedCard Component', () => {
  const testItem: FeaturedItem = {
    id: 'test-1',
    title: 'Test Featured Item',
    description: 'A description for the test featured item',
    imageUrl: '/images/test.jpg',
    imageAlt: 'Test image alt text',
    detailUrl: '/detail/test-1',
  }

  it('renders card with all required elements', () => {
    renderWithRouter(<FeaturedCard item={testItem} />)

    expect(screen.getByText(testItem.title)).toBeInTheDocument()
    expect(screen.getByText(testItem.description)).toBeInTheDocument()
    expect(screen.getByAltText(testItem.imageAlt)).toBeInTheDocument()
  })

  it('contains link to detail page', () => {
    renderWithRouter(<FeaturedCard item={testItem} />)

    const link = screen.getByRole('link')
    expect(link).toHaveAttribute('href', testItem.detailUrl)
  })

  it('calls onClick callback when provided', async () => {
    const handleClick = vi.fn()
    const user = userEvent.setup()

    renderWithRouter(<FeaturedCard item={testItem} onClick={handleClick} />)

    const link = screen.getByRole('link')
    await user.click(link)

    expect(handleClick).toHaveBeenCalledWith(testItem)
  })

  it('image has lazy loading attribute', () => {
    renderWithRouter(<FeaturedCard item={testItem} />)

    const image = screen.getByAltText(testItem.imageAlt)
    expect(image).toHaveAttribute('loading', 'lazy')
  })
})

/**
 * Integration Tests - FeaturedContent with Navigation
 */
describe('FeaturedContent Integration - Navigation', () => {
  it('featured card links to correct detail URL', () => {
    renderWithRouter(<FeaturedContent items={sampleFeaturedItems} />)

    const links = screen.getAllByRole('link')

    // Verify each link has the correct href
    sampleFeaturedItems.forEach((item, index) => {
      expect(links[index]).toHaveAttribute('href', item.detailUrl)
    })
  })

  it('clicking featured item triggers onItemClick callback', async () => {
    const handleItemClick = vi.fn()
    const user = userEvent.setup()

    renderWithRouter(
      <FeaturedContent items={sampleFeaturedItems} onItemClick={handleItemClick} />
    )

    const firstCardLink = screen.getAllByRole('link')[0]
    await user.click(firstCardLink)

    expect(handleItemClick).toHaveBeenCalledWith(sampleFeaturedItems[0])
  })
})
