import { FeaturedCard } from './FeaturedCard'
import type { FeaturedContentProps } from '../../types/FeaturedContent.types'
import './FeaturedContent.css'

/**
 * FeaturedContent component displays a section of featured content items
 * as a grid of cards. Handles empty state gracefully.
 *
 * REQ-3: Homepage shall display featured content or highlights relevant to users
 * US-5: Discover Featured Content - user sees featured content sections with
 *       engaging visuals and descriptions, can click through to learn more
 */
export const FeaturedContent: React.FC<FeaturedContentProps> = ({
  items,
  sectionTitle = 'Featured Content',
  onItemClick,
}) => {
  // Handle empty state gracefully - render section but show helpful message
  if (!items || items.length === 0) {
    return (
      <section
        className="featured-content-section"
        data-testid="featured-content-section"
        aria-label={sectionTitle}
      >
        <h2 className="featured-content-title">{sectionTitle}</h2>
        <div
          className="featured-content-empty"
          data-testid="featured-content-empty"
          role="status"
          aria-live="polite"
        >
          <p>No featured content available at this time.</p>
        </div>
      </section>
    )
  }

  return (
    <section
      className="featured-content-section"
      data-testid="featured-content-section"
      aria-label={sectionTitle}
    >
      <h2 className="featured-content-title">{sectionTitle}</h2>
      <div
        className="featured-content-grid"
        data-testid="featured-content-grid"
        role="list"
      >
        {items.map((item) => (
          <div key={item.id} role="listitem">
            <FeaturedCard item={item} onClick={onItemClick} />
          </div>
        ))}
      </div>
    </section>
  )
}
