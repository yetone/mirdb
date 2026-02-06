/**
 * Testimonial component for displaying individual testimonials.
 * Owner: Scenario 4 - Social Proof Section
 *
 * Uses proper blockquote semantics with cite attributes for WCAG compliance.
 */

import type { Testimonial as TestimonialType } from '../../types';
import styles from './SocialProof.module.css';

interface TestimonialProps {
  testimonial: TestimonialType;
}

export function Testimonial({ testimonial }: TestimonialProps) {
  const { quote, author, role, company } = testimonial;
  const attribution = role && company ? `${role}, ${company}` : role || company || '';

  return (
    <article className={styles.testimonialCard}>
      <blockquote
        className={styles.testimonialQuote}
        cite={company ? `https://${company.toLowerCase().replace(/\s+/g, '')}.com` : undefined}
      >
        <p>"{quote}"</p>
      </blockquote>
      <footer className={styles.testimonialAttribution}>
        <cite className={styles.testimonialAuthor}>{author}</cite>
        {attribution && (
          <span className={styles.testimonialRole}>{attribution}</span>
        )}
      </footer>
    </article>
  );
}

export default Testimonial;
