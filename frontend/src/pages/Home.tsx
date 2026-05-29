/**
 * Home Page Component
 * Owner: Scenario 1 - Homepage Hero Section Rendering (primary)
 *        All other scenarios may extend this file
 *
 * Main homepage composing all sections: Navbar, HeroSection,
 * FeaturesSection, Footer, and BackgroundEffect.
 *
 * Expected exports:
 * - Home: React.FC component
 */
import UrlShortenerForm from '../components/Home/UrlShortenerForm';

export default function Home() {
  return (
    <main>
      <h1>URL Shortener</h1>
      <UrlShortenerForm />
    </main>
  );
}
