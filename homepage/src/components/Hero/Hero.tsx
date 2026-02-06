import { Button } from '../common/Button';
import { HERO_CONTENT } from '../../utils/constants';
import styles from './Hero.module.css';

/**
 * Hero section component with headline, subheadline, and CTA buttons.
 *
 * Requirements covered:
 * - REQ-2: Prominent hero section with headline and subheadline
 * - REQ-3: Primary CTA button (Get Started/Sign Up)
 * - REQ-4: Secondary CTA button (Learn More/View Demo)
 */
export function Hero() {
  return (
    <section
      className={styles.hero}
      aria-labelledby="hero-headline"
      data-testid="hero-section"
    >
      <div className={styles.container}>
        <h1 id="hero-headline" className={styles.headline}>
          {HERO_CONTENT.headline}
        </h1>
        <p className={styles.subheadline}>
          {HERO_CONTENT.subheadline}
        </p>
        <div className={styles.ctaContainer}>
          <Button
            variant="primary"
            href={HERO_CONTENT.primaryCTA.href}
            aria-label={`${HERO_CONTENT.primaryCTA.text} - Create your account`}
          >
            {HERO_CONTENT.primaryCTA.text}
          </Button>
          <Button
            variant="secondary"
            href={HERO_CONTENT.secondaryCTA.href}
            aria-label={`${HERO_CONTENT.secondaryCTA.text} - Explore features`}
          >
            {HERO_CONTENT.secondaryCTA.text}
          </Button>
        </div>
      </div>
    </section>
  );
}
