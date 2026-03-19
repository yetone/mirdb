import { Header } from '@/components/layout/Header';
import { Hero } from '@/components/sections/Hero';
import { Features } from '@/components/sections/Features';
import { HowItWorks } from '@/components/sections/HowItWorks';
import { ProjectStatus } from '@/components/sections/ProjectStatus';
import { QuickStart } from '@/components/sections/QuickStart';
import { Resources } from '@/components/sections/Resources';
import { Footer } from '@/components/layout/Footer';

export default function Home() {
  return (
    <>
      {/* Navigation Header - Scenario 8 */}
      <Header />

      <main className="min-h-screen pt-20">
        {/* Hero Section - Scenario 1 */}
        <Hero />

        {/* Features Section - Scenario 2 */}
        <Features />

        {/* How It Works Section - Scenario 3 */}
        <HowItWorks />

        {/* Status Section - Scenario 4 */}
        <ProjectStatus />

        {/* Quick Start Section - Scenario 5 */}
        <QuickStart />

        {/* Resources Section - Scenario 6 */}
        <Resources />
      </main>

      {/* Footer Section - Scenario 7 */}
      <Footer />
    </>
  );
}
