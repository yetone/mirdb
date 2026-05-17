import { Link } from 'react-router-dom';
import { ROUTES } from '../../utils/constants';

/**
 * Hero CTA buttons.
 * Owner: Scenario 2 - Call-to-Action Buttons and Navigation Routing.
 *
 * Renders a primary 'Register' link and a secondary 'Login' link
 * using react-router-dom <Link> for client-side navigation.
 */
export default function HeroCTA() {
  return (
    <div className="flex flex-col sm:flex-row gap-4 items-center justify-center">
      <Link
        to={ROUTES.REGISTER}
        className="btn btn-primary btn-lg"
        data-testid="hero-cta-primary"
      >
        Register
      </Link>
      <Link
        to={ROUTES.LOGIN}
        className="btn btn-outline btn-lg"
        data-testid="hero-cta-secondary"
      >
        Login
      </Link>
    </div>
  );
}
