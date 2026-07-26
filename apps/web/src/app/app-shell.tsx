import type { PropsWithChildren } from "react";
import { NavLink } from "react-router-dom";

const NAV_ITEMS = [
  { to: "/", label: "Материалы", end: true },
  { to: "/collections", label: "Группы" },
  { to: "/exports", label: "Экспорт" },
  { to: "/runs", label: "Подборка" },
  { to: "/artifacts", label: "Результаты" },
  { to: "/diagnostics", label: "Проверки" },
];

export function AppShell({ children }: PropsWithChildren): JSX.Element {
  return (
    <div className="app-shell">
      <header className="app-shell__header">
        <div>
          <p className="app-shell__eyebrow">Анализ медиа</p>
          <h1>Материалы</h1>
        </div>
      </header>

      <nav className="app-shell__nav" aria-label="Основная навигация">
        {NAV_ITEMS.map((item) => (
          <NavLink end={item.end} key={item.to} to={item.to}>
            {item.label}
          </NavLink>
        ))}
      </nav>

      <main className="app-shell__content">{children}</main>
    </div>
  );
}
