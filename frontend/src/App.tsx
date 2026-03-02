import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { HeroSection } from './components/home';

function Home() {
  return (
    <main id="main-content">
      <HeroSection />
    </main>
  );
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Home />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
