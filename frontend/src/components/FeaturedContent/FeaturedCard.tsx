import { Link } from 'react-router-dom'
import type { FeaturedCardProps, FeaturedItem } from '../../types/FeaturedContent.types'

/**
 * FeaturedCard component displays a single featured content item
 * with an image, title, description, and link to the detail page.
 *
 * REQ-3: Homepage shall display featured content or highlights relevant to users
 * US-5: Discover Featured Content - engaging visuals and descriptions
 */
export const FeaturedCard: React.FC<FeaturedCardProps> = ({ item, onClick }) => {
  const handleClick = (e: React.MouseEvent) => {
    if (onClick) {
      e.preventDefault()
      onClick(item)
    }
  }

  return (
    <article
      className="featured-card"
      data-testid={`featured-card-${item.id}`}
      role="article"
      aria-labelledby={`featured-title-${item.id}`}
    >
      <Link
        to={item.detailUrl}
        onClick={handleClick}
        className="featured-card-link"
        aria-label={`View details for ${item.title}`}
      >
        <div className="featured-card-image-container">
          <img
            src={item.imageUrl}
            alt={item.imageAlt}
            className="featured-card-image"
            loading="lazy"
          />
        </div>
        <div className="featured-card-content">
          <h3
            id={`featured-title-${item.id}`}
            className="featured-card-title"
          >
            {item.title}
          </h3>
          <p className="featured-card-description">
            {item.description}
          </p>
        </div>
      </Link>
    </article>
  )
}
