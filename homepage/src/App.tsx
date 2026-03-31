import { HeroSection } from './components/sections/HeroSection';
import { FeaturesSection } from './components/sections/FeaturesSection';
import { QuickStartSection } from './components/sections/QuickStartSection';
import { DemoSection } from './components/sections/DemoSection';
import { Header } from './components/layout/Header';
import { features } from './constants/features';

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900">
      <Header />
      <main>
        <HeroSection />
        <FeaturesSection features={features} />
        <QuickStartSection />
        <DemoSection />
      </main>
    </div>
  );
}

export default App;
