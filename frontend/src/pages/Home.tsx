/**
 * Homepage Component
 * Owner: Primary - Scenario 1 (Hero Section), Secondary - Scenarios 15, 17
 *
 * Main landing page for the URL Shortening Service.
 * Composes HeroSection, FeaturesSection, HowItWorksSection, and Footer.
 *
 * Authentication-aware features (Scenario 15):
 * - Displays personalized greeting for authenticated users
 * - Shows Dashboard/Logout buttons instead of Sign Up/Log In
 */

import HeroSection from '../components/homepage/HeroSection'
import FeaturesSection from '../components/homepage/FeaturesSection'
import HowItWorksSection from '../components/homepage/HowItWorksSection'
import Footer from '../components/Footer'
import { useAuth } from '../contexts/AuthContext'

export default function Home() {
  const { user, isAuthenticated, logout } = useAuth()

  return (
    <div className="min-h-screen bg-base-100">
      <main>
        {/* Hero Section - Scenario 1, with auth props - Scenario 15 */}
        <HeroSection
          isAuthenticated={isAuthenticated}
          username={user?.username}
          onLogout={logout}
        />

        {/* Features Section - Scenario 3 */}
        <FeaturesSection />

        {/* How It Works Section - Scenario 4 */}
        <HowItWorksSection />
      </main>

      {/* Footer - Scenario 6 */}
      <Footer />
    </div>
  )
}
