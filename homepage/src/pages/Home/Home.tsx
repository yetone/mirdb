/**
 * Home page component.
 * Owner: Scenario 10 - User Flow (page composition)
 *
 * Composes all homepage sections for the complete first-time visitor experience.
 *
 * Composition:
 * - Hero (from Scenario 2)
 * - Features (from Scenario 3)
 * - SocialProof (from Scenario 4)
 * - Footer (from Scenario 5)
 *
 * User flow validated:
 * - US-1: First-time visitor understands product value
 * - US-2: Easy access to sign-up CTA
 * - US-4: Social proof for trust building
 */

import { Hero } from '../../components/Hero';
import { Features } from '../../components/Features';
import { SocialProof } from '../../components/SocialProof';
import { Footer } from '../../components/Footer';
import { TESTIMONIALS, TRUSTED_COMPANIES, USER_STATISTICS } from '../../utils/constants';
import styles from './Home.module.css';

export function Home() {
  return (
    <>
      <main id="main-content" className={styles.main} tabIndex={-1}>
        <Hero />
        <Features />
        <SocialProof
          testimonials={TESTIMONIALS}
          companies={TRUSTED_COMPANIES}
          statistics={USER_STATISTICS}
        />
      </main>
      <Footer />
    </>
  );
}

export default Home;
