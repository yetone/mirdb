/**
 * SocialProof section component.
 * Owner: Scenario 4 - Social Proof Section
 *
 * Displays testimonials, user statistics, and trusted company logos
 * to build trust with potential users.
 *
 * Requirements covered:
 * - REQ-7: Social proof elements (testimonials, user count, trusted logos)
 * - NFR-2: WCAG 2.1 Level AA accessibility standards
 */

import type { Testimonial as TestimonialType, TrustedCompany, UserStatistics } from '../../types';
import { Testimonial } from './Testimonial';
import { TrustLogos } from './TrustLogos';
import styles from './SocialProof.module.css';

interface SocialProofProps {
  testimonials: TestimonialType[];
  companies?: TrustedCompany[];
  statistics?: UserStatistics;
  sectionTitle?: string;
}

export function SocialProof({
  testimonials,
  companies,
  statistics,
  sectionTitle = 'What Our Users Say',
}: SocialProofProps) {
  return (
    <section
      id="social-proof"
      className={styles.socialProof}
      aria-labelledby="social-proof-title"
    >
      <div className={styles.container}>
        <h2 id="social-proof-title" className={styles.sectionTitle}>
          {sectionTitle}
        </h2>

        {/* User Statistics */}
        {statistics && (
          <div className={styles.statistics} role="group" aria-label="User statistics">
            <div className={styles.statItem}>
              <div className={styles.statValue} aria-label={`${statistics.userCount} ${statistics.userLabel}`}>
                {statistics.userCount}
              </div>
              <div className={styles.statLabel}>{statistics.userLabel}</div>
            </div>
            <div className={styles.statItem}>
              <div className={styles.statValue} aria-label={`${statistics.projectCount} ${statistics.projectLabel}`}>
                {statistics.projectCount}
              </div>
              <div className={styles.statLabel}>{statistics.projectLabel}</div>
            </div>
            <div className={styles.statItem}>
              <div className={styles.statValue} aria-label={`${statistics.uptimePercent} ${statistics.uptimeLabel}`}>
                {statistics.uptimePercent}
              </div>
              <div className={styles.statLabel}>{statistics.uptimeLabel}</div>
            </div>
          </div>
        )}

        {/* Testimonials */}
        {testimonials.length > 0 && (
          <div className={styles.testimonials}>
            {testimonials.map((testimonial, index) => (
              <Testimonial key={`testimonial-${index}`} testimonial={testimonial} />
            ))}
          </div>
        )}

        {/* Trust Logos */}
        {companies && companies.length > 0 && <TrustLogos companies={companies} />}
      </div>
    </section>
  );
}

export default SocialProof;
