/**
 * Landing Page / Home Page
 * Owner: Multiple scenarios (main page composition)
 *
 * Main landing page that composes all section components:
 * - Navigation header with Login/Register links
 * - HeroSection
 * - FeaturesSection (with id="features" for scroll anchor)
 * - HowItWorksSection
 * - CTASection
 * - Footer
 *
 * Uses:
 * - BackgroundEffect for visual enhancement
 * - ThemeContext for theme integration
 * - React Router for navigation
 * - Semantic HTML (header, main, section, footer)
 *
 * SEO:
 * - Proper heading hierarchy (one h1)
 * - Meta tags for description
 *
 * Accessibility:
 * - Semantic HTML structure
 * - Skip links for keyboard navigation
 * - Proper focus management
 */
import { Link } from 'react-router-dom';
import BackgroundEffect from '../components/BackgroundEffect';
import { HeroSection, FeaturesSection, HowItWorksSection, CTASection } from '../components/landing';
import ThemeToggle from '../components/ThemeToggle';

export default function Home() {
  return (
    <div className="min-h-screen relative">
      <BackgroundEffect />

      {/* Navigation Header */}
      <header className="navbar bg-base-100/80 backdrop-blur-md sticky top-0 z-50 border-b border-base-300">
        <div className="navbar-start">
          <Link to="/" className="btn btn-ghost text-xl font-bold">
            URL Shortener
          </Link>
        </div>
        <div className="navbar-end gap-2">
          <ThemeToggle />
          <Link to="/login" className="btn btn-ghost">
            Login
          </Link>
          <Link to="/register" className="btn btn-primary">
            Get Started
          </Link>
        </div>
      </header>

      {/* Main Content */}
      <main>
        <HeroSection />

        {/* Features section with id for scroll anchor */}
        <section id="features" aria-label="Features">
          <FeaturesSection />
        </section>

        {/* How It Works section */}
        <HowItWorksSection />

        {/* Final CTA section before footer */}
        <CTASection />
      </main>

      {/* Footer placeholder */}
      <footer className="footer footer-center p-4 bg-base-200 text-base-content border-t border-base-300">
        <p>&copy; {new Date().getFullYear()} URL Shortener. All rights reserved.</p>
      </footer>
    </div>
  );
}
