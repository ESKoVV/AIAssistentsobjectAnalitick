import { useQueryClient } from '@tanstack/react-query';
import { Navigate, Route, Routes } from 'react-router-dom';
import { Header } from './components/layout/Header';
import { PageWrapper } from './components/layout/PageWrapper';
import { Sidebar } from './components/layout/Sidebar';
import { Analytics } from './pages/Analytics';
import { Dashboard } from './pages/Dashboard';
import { DocumentDetail } from './pages/DocumentDetail';
import { Feed } from './pages/Feed';
import { Topics } from './pages/Topics';

function App() {
  const queryClient = useQueryClient();

  return (
    <div className="min-h-screen">
      <Header onRefresh={() => queryClient.invalidateQueries()} />
      <div className="flex">
        <Sidebar />
        <main className="flex-1">
          <PageWrapper>
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/feed" element={<Feed />} />
              <Route path="/topics" element={<Topics />} />
              <Route path="/document/:id" element={<DocumentDetail />} />
              <Route path="/analytics" element={<Analytics />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </PageWrapper>
        </main>
      </div>
    </div>
  );
}

export default App;
