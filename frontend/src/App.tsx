import { Routes, Route } from 'react-router-dom';
import { Home } from './pages/Home';
import { ThemeProvider } from './contexts';

function App() {
  return (
    <ThemeProvider>
      <div className="min-h-screen">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/register" element={<div>Register Page</div>} />
          <Route path="/login" element={<div>Login Page</div>} />
        </Routes>
      </div>
    </ThemeProvider>
  );
}

export default App;
