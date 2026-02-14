import { ThemeProvider } from './context/ThemeContext';
import { Navbar } from './components/layout/Navbar';
import { Footer } from './components/layout/Footer';
import { Hero } from './components/sections/Hero';
import { ThemeToggle } from './components/common/ThemeToggle';

function App() {
  return (
    <ThemeProvider>
      <div className="min-h-screen flex flex-col">
        <Navbar />
        {/* Theme toggle - positioned at top right for demo visibility */}
        <div className="fixed top-4 right-4 z-50">
          <ThemeToggle />
        </div>
        <main className="flex-grow">
          <section id="home">
            <Hero />
          </section>

          <section id="features" className="py-20 px-4 bg-gray-50 dark:bg-gray-800">
            <div className="max-w-4xl mx-auto">
              <h2 className="text-3xl font-bold text-center mb-8">Features</h2>
              <p className="text-center text-gray-600 dark:text-gray-400">
                Features section content will be provided by Scenario 4.
              </p>
            </div>
          </section>
        </main>
        <Footer />
      </div>
    </ThemeProvider>
  );
}

export default App;
