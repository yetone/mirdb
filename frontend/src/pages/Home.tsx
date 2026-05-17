import HomeNavbar from '../components/homepage/HomeNavbar';
import Hero from '../components/homepage/Hero';
import FeaturesSection from '../components/homepage/FeaturesSection';

/**
 * Home page composition.
 * Owner: Scenario 1 - Page Branding and Hero Display.
 */
export default function Home() {
  return (
    <main id="main-content">
      <HomeNavbar />
      <Hero />
      <FeaturesSection />
    </main>
  );
}
