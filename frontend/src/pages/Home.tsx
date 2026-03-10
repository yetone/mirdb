/**
 * Home Page Component
 * Owner: Scenario 13 - Semantic HTML and SEO
 *
 * The main landing page with proper semantic HTML structure.
 * Uses HTML5 semantic elements for accessibility and SEO:
 * - <header> for site navigation
 * - <main> for primary content
 * - <footer> for page footer (via Footer component)
 *
 * Contains sections:
 * - Hero section with URL shortening form
 * - Features section highlighting capabilities
 * - How It Works section with step-by-step guide
 * - CTA section encouraging registration
 * - Footer with links and copyright
 */

import { Navbar } from '../components/Navbar';
import {
  HeroSection,
  FeaturesSection,
  HowItWorksSection,
  CTASection,
  Footer,
} from '../components/landing';

/**
 * Home page component with semantic HTML structure.
 * Provides proper document outline with header, main, and footer landmarks.
 */
function Home() {
  return (
    <>
      {/* Header landmark containing site navigation */}
      <header>
        <Navbar />
      </header>

      {/* Main content landmark */}
      <main id="main-content" className="min-h-screen">
        {/* Hero section with H1 headline and URL shortener */}
        <HeroSection />

        {/* Features section showcasing product capabilities */}
        <FeaturesSection />

        {/* How It Works step-by-step guide */}
        <HowItWorksSection />

        {/* Call-to-action for user registration */}
        <CTASection />
      </main>

      {/* Footer landmark with links and copyright */}
      <Footer />
    </>
  );
}

export default Home;
