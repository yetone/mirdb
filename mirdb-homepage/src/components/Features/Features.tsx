import { FEATURES } from '../../utils/constants';
import { FeatureCard } from './FeatureCard';
import styles from './Features.module.css';

export function Features() {
  return (
    <section
      id="features"
      className={styles.features}
      aria-labelledby="features-heading"
      role="region"
    >
      <h2 id="features-heading" className={styles.heading}>
        Key Features
      </h2>
      <div className={styles.grid}>
        {FEATURES.map((feature) => (
          <FeatureCard key={feature.title} feature={feature} />
        ))}
      </div>
    </section>
  );
}
