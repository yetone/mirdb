import { useParams, Link } from 'react-router-dom'

/**
 * DetailPage component displays the detail view for a featured item.
 * This component is used as the navigation target when clicking
 * on featured content items.
 */
export const DetailPage: React.FC = () => {
  const { id } = useParams<{ id: string }>()

  return (
    <main
      className="detail-page"
      data-testid="detail-page"
    >
      <nav aria-label="Breadcrumb">
        <Link to="/" data-testid="back-to-home">
          ← Back to Home
        </Link>
      </nav>
      <h1 data-testid="detail-page-title">Featured Item Details</h1>
      <p data-testid="detail-page-id">Item ID: {id}</p>
    </main>
  )
}
