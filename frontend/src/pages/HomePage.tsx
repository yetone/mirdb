/**
 * Main Homepage Component.
 * Owner: Scenario 6 - Main HomePage Assembly
 *
 * This is a placeholder that will be fully implemented by Scenario 6.
 * Currently includes HeroSection for Scenario 1 testing.
 * PublicNavbar added by Scenario 2 for navigation testing.
 */

import HeroSection from '../components/homepage/HeroSection';
import PublicNavbar from '../components/homepage/PublicNavbar';

function HomePage() {
  return (
    <div className="min-h-screen">
      <PublicNavbar />
      <main>
        <HeroSection />
      </main>
    </div>
  );
}

export default HomePage;
