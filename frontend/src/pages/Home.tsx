import { useEffect } from 'react';
import HomeNavbar from '../components/homepage/HomeNavbar';
import Hero from '../components/homepage/Hero';
import FeaturesSection from '../components/homepage/FeaturesSection';
import SEOTags from '../components/homepage/SEOTags';
import BackgroundEffect from '../components/homepage/BackgroundEffect';
import SkipToContent from '../components/homepage/SkipToContent';
import { BRAND } from '../utils/constants';

const HOMEPAGE_TITLE = `${BRAND.name} - URL Shortening Service`;

/**
 * Home page composition.
 * Owner: Scenario 1 - Page Branding and Hero Display.
 *
 * Wraps the public homepage in a semantic <main> and composes the
 * sub-components owned by sibling scenarios. The useEffect title update is a
 * deliberate fallback so the document title is correct even when SEOTags
 * (Scenario 8) has not yet been wired up; the head manager, once active,
 * will simply re-assert the same value.
 */
export default function Home() {
  useEffect(() => {
    document.title = HOMEPAGE_TITLE;
  }, []);

  return (
    <>
      <SkipToContent />
      <SEOTags />
      <BackgroundEffect />
      <HomeNavbar />
      <main id="main-content" data-testid="home-main" tabIndex={-1}>
        <Hero />
        <FeaturesSection />
      </main>
    </>
  );
}
