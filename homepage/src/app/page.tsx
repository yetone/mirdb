import { Hero } from '@/components/sections/Hero';
import { Features } from '@/components/sections/Features';
import { HowItWorks } from '@/components/sections/HowItWorks';
import { ProjectStatus } from '@/components/sections/ProjectStatus';
import { Resources } from '@/components/sections/Resources';

export default function Home() {
  return (
    <main className="min-h-screen">
      {/* Hero Section - Scenario 1 */}
      <Hero />

      {/* Features Section - Scenario 2 */}
      <Features />

      {/* How It Works Section - Scenario 3 */}
      <HowItWorks />

      {/* Status Section - Scenario 4 */}
      <ProjectStatus />

      {/* Quick Start Section - Scenario 5 */}
      <section id="quick-start" className="py-16 px-4">
        {/* QuickStart - Scenario 5 */}
      </section>

      {/* Resources Section - Scenario 6 */}
      <Resources />
    </main>
  );
}
