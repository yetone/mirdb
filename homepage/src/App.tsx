import { QuickStart } from './components/QuickStart/QuickStart';

function App() {
  return (
    <>
      <a href="#main-content" className="skip-link">
        Skip to main content
      </a>
      <main id="main-content">
        <QuickStart />
      </main>
    </>
  );
}

export default App;
