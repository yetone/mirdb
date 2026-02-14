import { ThemeProvider } from './context/ThemeContext';
import { Navbar } from './components/layout/Navbar';
import { Footer } from './components/layout/Footer';
import { Hero } from './components/sections/Hero';
import { Features } from './components/sections/Features';
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

          <Features />
        </main>
        <Footer />
      </div>
    </ThemeProvider>
  );
}

export default App;
