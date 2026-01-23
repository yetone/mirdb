import { Routes, Route } from 'react-router-dom';
import Home from './pages/Home';
import Login from './pages/Login';
import Register from './pages/Register';
import Dashboard from './pages/Dashboard';
import UrlStats from './pages/UrlStats';
import Settings from './pages/Settings';
import ProtectedLayout from './components/ProtectedLayout';
import AdminRoute from './components/AdminRoute';

function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/login" element={<Login />} />
      <Route path="/register" element={<Register />} />
      <Route element={<ProtectedLayout />}>
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/stats/:shortCode" element={<UrlStats />} />
        <Route element={<AdminRoute />}>
          <Route path="/settings" element={<Settings />} />
        </Route>
      </Route>
    </Routes>
  );
}

export default App;
