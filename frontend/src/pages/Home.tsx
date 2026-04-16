/**
 * Homepage Page Component.
 * Owner: Scenario 1 - Homepage Basic Display and Navigation
 *
 * Composes all homepage sections and integrates with AuthContext.
 */
import { useAuth } from '../contexts/AuthContext';
import { HeroSection } from '../components/homepage';

export default function Home() {
  const { isAuthenticated, username } = useAuth();

  return (
    <main className="relative">
      <HeroSection
        isAuthenticated={isAuthenticated}
        username={username ?? undefined}
      />
      {/* Additional sections will be added by other scenarios */}
      <div id="features" />
    </main>
  );
}
