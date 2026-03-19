import { Hero } from '@/components/sections/Hero';
import { Features } from '@/components/sections/Features';
import { HowItWorks } from '@/components/sections/HowItWorks';

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
      <section id="status" className="py-16 px-4 bg-gray-50 dark:bg-slate-800">
        {/* ProjectStatus - Scenario 4 */}
      </section>

      {/* Quick Start Section - Scenario 5 */}
      <section id="quick-start" className="py-16 px-4">
        {/* QuickStart - Scenario 5 */}
      </section>

      {/* Resources Section - Scenario 6 */}
      <section id="resources" className="py-16 px-4 bg-gray-50 dark:bg-slate-800">
        {/* Resources - Scenario 6 */}
      </section>
    </main>
  );
}
