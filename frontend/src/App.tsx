import HeroSection from './components/hero/HeroSection';
import FeaturesSection from './components/features/FeaturesSection';
import PlannedFeatures from './components/features/PlannedFeatures';

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900 text-gray-900 dark:text-white">
      <main>
        <HeroSection />
        <FeaturesSection />
        <PlannedFeatures />
      </main>
    </div>
  );
}

export default App;
