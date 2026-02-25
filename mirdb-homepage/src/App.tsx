import { Header } from './components/Header';
import { Features } from './components/Features';
import { ProjectStatus } from './components/ProjectStatus';
import { TechHighlights } from './components/TechHighlights';
import { ThemeToggle } from './components/ThemeToggle';
import { Footer } from './components/Footer';
import styles from './App.module.css';

function App() {
  return (
    <div className={styles.app}>
      <Header themeToggle={<ThemeToggle />} />
      <main className={styles.main}>
        <section id="features">
          <Features />
        </section>
        <section id="usage" className={styles.section}>
          <h2>Usage</h2>
          <p>Usage demonstration section placeholder.</p>
        </section>
        <ProjectStatus />
        <TechHighlights />
      </main>
      <Footer />
    </div>
  );
}

export default App;
