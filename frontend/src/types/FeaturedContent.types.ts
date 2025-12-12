/**
 * Type definitions for Featured Content functionality
 * REQ-3: Homepage shall display featured content or highlights relevant to users
 */

/**
 * Represents a single featured content item
 */
export interface FeaturedItem {
  /** Unique identifier for the featured item */
  id: string
  /** Title of the featured content */
  title: string
  /** Description or summary of the content */
  description: string
  /** URL to the image for the featured item */
  imageUrl: string
  /** Alt text for the image for accessibility */
  imageAlt: string
  /** URL to the detail page for this item */
  detailUrl: string
}

/**
 * Props for the FeaturedContent component
 */
export interface FeaturedContentProps {
  /** Array of featured items to display */
  items: FeaturedItem[]
  /** Optional title for the featured content section */
  sectionTitle?: string
  /** Optional callback when an item is clicked */
  onItemClick?: (item: FeaturedItem) => void
}

/**
 * Props for individual FeaturedCard component
 */
export interface FeaturedCardProps {
  /** The featured item data */
  item: FeaturedItem
  /** Callback when the card is clicked */
  onClick?: (item: FeaturedItem) => void
}
