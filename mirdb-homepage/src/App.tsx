import { Features } from './components/Features';
import styles from './App.module.css';

function App() {
  return (
    <div className={styles.app}>
      <main className={styles.main}>
        <Features />
      </main>
    </div>
  );
}

export default App;
