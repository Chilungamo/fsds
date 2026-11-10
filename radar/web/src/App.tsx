import { useCallback, useEffect, useRef, useState } from "react";
import { api, type CaseSummary, type RadarEvent, type Stats } from "./api";
import { CaseView } from "./components/CaseView";
import { Feed } from "./components/Feed";
import { StatsStrip } from "./components/StatsStrip";
import { useEventStream } from "./live";

const PAGE = 40;

function caseFromPath(): number | null {
  const m = window.location.pathname.match(/^\/cases\/(\d+)/);
  return m ? Number(m[1]) : null;
}

function useTheme(): [string, () => void] {
  const [theme, setTheme] = useState<string>(() => {
    try {
      return localStorage.getItem("radar-theme") ?? "";
    } catch {
      return "";
    }
  });
  useEffect(() => {
    if (theme) document.documentElement.dataset.theme = theme;
    else delete document.documentElement.dataset.theme;
  }, [theme]);
  const toggle = () => {
    const dark = theme ? theme === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
    const next = dark ? "light" : "dark";
    setTheme(next);
    try {
      localStorage.setItem("radar-theme", next);
    } catch {
      /* storage unavailable: theme lasts for this visit */
    }
  };
  return [theme, toggle];
}

export function App() {
  const [selected, setSelected] = useState<number | null>(caseFromPath);
  const [items, setItems] = useState<CaseSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [status, setStatus] = useState("");
  const [sort, setSort] = useState("recent");
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState<Stats | null>(null);
  const [fresh, setFresh] = useState<Set<number>>(new Set());
  const [announce, setAnnounce] = useState("");
  const [, toggleTheme] = useTheme();
  const reqId = useRef(0);

  const loadPage = useCallback(
    (offset: number) => {
      const id = ++reqId.current;
      setLoading(true);
      api
        .cases({ status, q: query.trim(), sort, limit: PAGE, offset })
        .then((r) => {
          if (id !== reqId.current) return; // a newer request superseded this one
          setItems((prev) => (offset === 0 ? r.items : [...prev, ...r.items]));
          setTotal(r.total);
        })
        .finally(() => id === reqId.current && setLoading(false));
    },
    [status, query, sort],
  );

  useEffect(() => {
    const t = setTimeout(() => loadPage(0), query ? 250 : 0);
    return () => clearTimeout(t);
  }, [loadPage, query]);

  useEffect(() => {
    api.stats().then(setStats).catch(() => setStats(null));
  }, []);

  useEffect(() => {
    if (!selected && items.length) setSelected(items[0].id);
  }, [items, selected]);

  useEffect(() => {
    const onPop = () => setSelected(caseFromPath());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const select = (id: number) => {
    setSelected(id);
    window.history.pushState(null, "", `/cases/${id}`);
  };

  // live updates: refresh the first page and stats, flash the card, re-render an open case
  const refresh = useRef<ReturnType<typeof setTimeout>>();
  const connected = useEventStream((e: RadarEvent) => {
    if (e.case_id === null) return;
    const caseId = e.case_id;
    setFresh((s) => new Set(s).add(caseId));
    setTimeout(() => setFresh((s) => { const n = new Set(s); n.delete(caseId); return n; }), 3500);
    window.dispatchEvent(new CustomEvent("radar:case-changed", { detail: caseId }));
    if (e.type === "case.created" || e.type === "case.confirmed") {
      const company = String(e.payload.company ?? "A company");
      setAnnounce(e.type === "case.created" ? `New Item 4.02 alert: ${company}` : `Restated figures filed: ${company}`);
    }
    clearTimeout(refresh.current);
    refresh.current = setTimeout(() => {
      loadPage(0);
      api.stats().then(setStats).catch(() => undefined);
    }, 800);
  });

  return (
    <div className="app">
      <header className="topbar">
        <a className="brand" href="/" onClick={(e) => { e.preventDefault(); setSelected(null); window.history.pushState(null, "", "/"); }}>
          <svg width="22" height="22" viewBox="0 0 32 32" aria-hidden="true">
            <circle cx="16" cy="16" r="13" fill="none" stroke="var(--accent)" strokeWidth="3" />
            <circle cx="16" cy="16" r="5" fill="var(--down)" />
          </svg>
          Restatement Radar
        </a>
        <span className="muted tagline" style={{ fontSize: 12 }}>SEC financial restatements, live from EDGAR</span>
        <span className="spacer" />
        <span className={`live ${connected ? "on" : ""}`} title={connected ? "Receiving live updates" : "Reconnecting to live updates"}>
          <span className="dot" aria-hidden="true" />
          {connected ? "Live" : "Offline"}
        </span>
        <button className="icon-btn" onClick={toggleTheme} aria-label="Switch between light and dark mode" title="Light / dark">
          <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
            <circle cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" strokeWidth="1.5" />
            <path d="M8 1.5a6.5 6.5 0 0 1 0 13z" fill="currentColor" />
          </svg>
        </button>
      </header>
      <StatsStrip stats={stats} />
      <div className="sr-only" aria-live="polite">{announce}</div>
      <main className="main">
        <Feed
          items={items}
          total={total}
          selected={selected}
          fresh={fresh}
          status={status}
          sort={sort}
          query={query}
          loading={loading}
          onStatus={setStatus}
          onSort={setSort}
          onQuery={setQuery}
          onSelect={select}
          onMore={() => loadPage(items.length)}
        />
        <section className="detail" aria-label="Case detail">
          {selected ? <CaseView key={selected} id={selected} /> : <div className="empty">Pick a case from the feed.</div>}
        </section>
      </main>
    </div>
  );
}
