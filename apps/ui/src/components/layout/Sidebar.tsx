import { BarChart3, FileText, Home, ListTree, PieChart } from 'lucide-react';
import { NavLink } from 'react-router-dom';

const links = [
  { to: '/', label: 'Обзор', icon: Home },
  { to: '/feed', label: 'Лента', icon: FileText },
  { to: '/topics', label: 'Темы', icon: ListTree },
  { to: '/analytics', label: 'Аналитика', icon: PieChart },
  { to: '/post/doc-2001', label: 'Пост', icon: BarChart3 }
];

export const Sidebar = () => (
  <aside className="w-64 border-r border-slate-700 bg-panel p-4">
    <nav className="space-y-1">
      {links.map(({ to, label, icon: Icon }) => (
        <NavLink
          key={to}
          to={to}
          className={({ isActive }) =>
            `flex items-center gap-2 rounded-md px-3 py-2 text-sm ${
              isActive ? 'bg-blue-600 text-white' : 'text-slate-300 hover:bg-slate-800'
            }`
          }
        >
          <Icon size={16} />
          {label}
        </NavLink>
      ))}
    </nav>
  </aside>
);
