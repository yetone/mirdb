import { Hero, Features, Documentation, Performance, QuickStart, Contributing } from './components/sections';
import { Footer } from './components/layout';
import { ThemeToggle } from './components/ui';
import './App.css';

function App() {
  return (
    <>
      <a href="#main-content" className="skip-link">
        Skip to main content
      </a>
      <header className="app-header" role="banner">
        <div className="container app-header__content">
          <span className="app-header__logo">MirDB</span>
          <ThemeToggle />
        </div>
      </header>
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
