import { Header } from './components/Header';
import { Features } from './components/Features';
import styles from './App.module.css';

function App() {
  return (
    <div className={styles.app}>
      <Header />
      <main className={styles.main}>
        <section id="features">
          <Features />
        </section>
        <section id="usage" className={styles.section}>
          <h2>Usage</h2>
          <p>Usage demonstration section placeholder.</p>
        </section>
        <section id="architecture" className={styles.section}>
          <h2>Architecture</h2>
          <p>Technical architecture section placeholder.</p>
        </section>
        <section id="resources" className={styles.section}>
          <h2>Resources</h2>
          <p>Resources section placeholder.</p>
        </section>
      </main>
    </div>
  );
}

export default App;
