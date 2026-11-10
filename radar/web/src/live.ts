import { useEffect, useRef, useState } from "react";
import type { RadarEvent } from "./api";

const EVENT_TYPES = ["case.created", "case.updated", "case.confirmed", "case.enriched"];

/**
 * Subscribe to /api/stream (Server-Sent Events). The browser reconnects on its own and
 * resends Last-Event-ID, so the server replays anything missed while offline.
 */
export function useEventStream(onEvent: (e: RadarEvent) => void): boolean {
  const [connected, setConnected] = useState(false);
  const handler = useRef(onEvent);
  handler.current = onEvent;

  useEffect(() => {
    const source = new EventSource("/api/stream");
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(source.readyState === EventSource.OPEN);
    const listener = (msg: MessageEvent) => {
      try {
        handler.current(JSON.parse(msg.data) as RadarEvent);
      } catch {
        /* ignore malformed events */
      }
    };
    EVENT_TYPES.forEach((t) => source.addEventListener(t, listener as EventListener));
    return () => source.close();
  }, []);

  return connected;
}
