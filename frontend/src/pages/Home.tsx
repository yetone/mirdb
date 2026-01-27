/**
 * Landing Page / Home Page
 * Owner: Multiple scenarios (main page composition)
 *
 * Main landing page that composes all section components.
 */
import { Link } from 'react-router-dom';
import BackgroundEffect from '../components/BackgroundEffect';
import { HeroSection } from '../components/landing';
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

        {/* Features section placeholder with id for scroll anchor */}
        <section id="features" className="py-16 px-4">
          <div className="max-w-6xl mx-auto text-center">
            <h2 className="text-3xl font-bold mb-8">Features</h2>
            <p className="text-base-content/70">
              Feature cards coming soon...
            </p>
          </div>
        </section>
      </main>

      {/* Footer placeholder */}
      <footer className="footer footer-center p-4 bg-base-200 text-base-content border-t border-base-300">
        <p>&copy; {new Date().getFullYear()} URL Shortener. All rights reserved.</p>
      </footer>
    </div>
  );
}
