/**
 * TrustLogos component for displaying trusted company logos.
 * Owner: Scenario 4 - Social Proof Section
 *
 * Displays company logos with proper alt text for accessibility.
 */

import type { TrustedCompany } from '../../types';
import styles from './SocialProof.module.css';

interface TrustLogosProps {
  companies: TrustedCompany[];
  title?: string;
}

export function TrustLogos({ companies, title = 'Trusted by industry leaders' }: TrustLogosProps) {
  return (
    <section className={styles.trustSection} aria-labelledby="trust-logos-title">
      <h3 id="trust-logos-title" className={styles.trustTitle}>
        {title}
      </h3>
      <div className={styles.trustLogos} role="list" aria-label="Trusted companies">
        {companies.map((company) => (
          <div key={company.name} role="listitem">
            <img
              src={company.logo}
              alt={`${company.name} logo`}
              className={styles.companyLogo}
              loading="lazy"
            />
          </div>
        ))}
      </div>
    </section>
  );
}

export default TrustLogos;
