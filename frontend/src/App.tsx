import { Routes, Route } from 'react-router-dom';
import { Home } from './pages/Home';

function App() {
  return (
    <div data-theme="dark" className="min-h-screen">
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/register" element={<div>Register Page</div>} />
        <Route path="/login" element={<div>Login Page</div>} />
      </Routes>
    </div>
  );
}

export default App;
