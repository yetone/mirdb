import { Hero, Features, Documentation, Performance, QuickStart, Contributing } from './components/sections';
import { Footer } from './components/layout';

function App() {
  return (
    <>
      <a href="#main-content" className="skip-link">
        Skip to main content
      </a>
      <main id="main-content" tabIndex={-1}>
        <Hero />
        <Features />
        <QuickStart />
        <Performance />
        <Documentation />
        <Contributing />
      </main>
      <Footer />
    </>
  );
}

export default App;
