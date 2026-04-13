/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays key MirDB features in a grid layout:
 * - Memcached protocol compatibility
 * - Disk persistence (SSTable-based)
 * - LSM-tree architecture
 * - Skip list memtables
 * - Minor and major compaction
 * - Written in Rust
 */
import { FEATURES } from '../../utils/constants';
import { FeatureCard } from './FeatureCard';
import './Features.css';

export function Features() {
  return (
    <section id="features" className="features-section section" aria-labelledby="features-heading">
      <div className="container">
        <h2 id="features-heading" className="features-title">Features</h2>
        <p className="features-subtitle">
          Built with performance and reliability in mind
        </p>
        <div className="features-grid" role="list">
          {FEATURES.map((feature, index) => (
            <div key={index} role="listitem">
              <FeatureCard feature={feature} />
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
