/**
 * Dashboard Preview Section Component
 * Owner: Scenario 7 - Dashboard Preview Section
 *
 * Displays a visual preview/mockup of the analytics dashboard
 * to entice users to sign up and explore the capabilities.
 *
 * Contents:
 * - Screenshot or mockup image of dashboard
 * - Feature callouts/annotations
 * - CTA button: "Start Tracking Your Links"
 *
 * Requirements covered:
 * - REQ-4: Visual demonstration of dashboard capabilities
 */

import { motion } from 'framer-motion';
import { FuturisticButton } from '../FuturisticButton';
import { GlassMorphismCard } from '../GlassMorphismCard';

export function DashboardPreview() {
  return (
    <section
      className="py-20 px-4"
      data-testid="dashboard-preview-section"
      aria-labelledby="dashboard-preview-title"
    >
      <div className="max-w-6xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-12"
        >
          <h2
            id="dashboard-preview-title"
            className="text-3xl md:text-4xl font-bold mb-4 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          >
            Powerful Analytics Dashboard
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Get real-time insights into your link performance with our intuitive dashboard
          </p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          <GlassMorphismCard className="overflow-hidden">
            <div className="relative">
              {/* Dashboard Preview Image */}
              <img
                src="/assets/images/dashboard-preview.svg"
                alt="Analytics dashboard showing click statistics, geographic data, and referrer insights"
                className="w-full h-auto rounded-lg shadow-lg"
                data-testid="dashboard-preview-image"
              />

              {/* Feature Callouts */}
              <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4">
                <div
                  className="text-center p-4"
                  data-testid="callout-click-tracking"
                >
                  <div className="text-primary font-semibold mb-1">Click Tracking</div>
                  <p className="text-sm text-base-content/70">
                    Monitor every click in real-time
                  </p>
                </div>
                <div
                  className="text-center p-4"
                  data-testid="callout-geographic-data"
                >
                  <div className="text-primary font-semibold mb-1">Geographic Data</div>
                  <p className="text-sm text-base-content/70">
                    See where your audience is located
                  </p>
                </div>
                <div
                  className="text-center p-4"
                  data-testid="callout-referrer-insights"
                >
                  <div className="text-primary font-semibold mb-1">Referrer Insights</div>
                  <p className="text-sm text-base-content/70">
                    Know where your traffic comes from
                  </p>
                </div>
              </div>
            </div>

            {/* CTA Section */}
            <div className="text-center mt-8">
              <FuturisticButton
                to="/register"
                variant="primary"
                size="lg"
                data-testid="dashboard-preview-cta"
              >
                Start Tracking Your Links
              </FuturisticButton>
            </div>
          </GlassMorphismCard>
        </motion.div>
      </div>
    </section>
  );
}
