import { Link } from 'react-router-dom';
import { ROUTES } from '../utils/constants';

export default function NotFound() {
  return (
    <div data-testid="not-found-page">
      <h1>404 - Page Not Found</h1>
      <p>The page you are looking for does not exist.</p>
      <Link to={ROUTES.HOME} data-testid="not-found-home-link">
        Return to homepage
      </Link>
    </div>
  );
}
