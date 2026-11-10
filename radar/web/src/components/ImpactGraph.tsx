import { useMemo, useState } from "react";
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { Graph, GraphNode } from "../api";
import { delta, direction } from "../format";

const LANE_X: Record<string, number> = { IS: 0, BS: 290, CF: 580 };
const ROW_H = 74;

// Top-to-bottom order inside each lane follows the statements' own roll-ups, so links
// within a statement run downwards (revenue -> gross profit -> ... -> net income).
const ORDER = [
  "revenue", "cogs", "gross_profit", "sga", "rd", "impairment", "opex", "operating_income",
  "warrant_fv_change", "other_income", "interest_expense", "pretax_income", "tax_expense",
  "income_continuing_ops", "net_income", "nci_income", "comprehensive_income", "eps_basic", "eps_diluted",
  "cash", "accounts_receivable", "inventory", "prepaid_assets", "total_current_assets", "ppe", "goodwill",
  "intangibles", "operating_lease_rou", "deferred_tax_assets", "other_assets_noncurrent", "total_assets",
  "accounts_payable", "accrued_liabilities", "deferred_revenue", "total_current_liabilities",
  "warrant_liabilities", "deferred_tax_liabilities", "other_liabilities_noncurrent", "total_liabilities",
  "temporary_equity", "common_stock", "apic", "retained_earnings", "aoci", "total_equity", "nci",
  "liabilities_and_equity",
  "sbc", "d_and_a", "deferred_tax_expense", "chg_accounts_receivable", "chg_inventory", "chg_prepaid",
  "chg_accounts_payable", "chg_accrued", "chg_deferred_revenue", "cfo", "capex", "cfi", "cff", "fx_effect",
  "net_change_cash",
];
const rank = (id: string) => {
  const i = ORDER.indexOf(id);
  return i === -1 ? ORDER.length : i;
};

// React Flow requires node data to be an open record
type FlowData = Record<string, unknown> & GraphNode & { dim: boolean; onPick: (id: string) => void };

function ItemNode({ data }: NodeProps<Node<FlowData>>) {
  const dir = data.measured ? direction(data.delta) : "flat";
  return (
    <div
      className={`rf-node ${dir} ${data.measured ? "" : "implied"} ${data.dim ? "dim" : ""}`}
      onClick={() => data.onPick(data.id)}
      title={data.measured ? "Reported in the restatement" : "Not reported: the effect is implied by the accounting link"}
    >
      <Handle id="t" type="target" position={Position.Top} style={{ opacity: 0 }} />
      <Handle id="l" type="target" position={Position.Left} style={{ opacity: 0 }} />
      <Handle id="rt" type="target" position={Position.Right} style={{ opacity: 0 }} />
      <div className="n">{data.name}</div>
      <div className="d">
        {data.measured ? <span className={`chg ${dir}`}>{delta(data.delta)}</span> : <span className="muted">implied</span>}
      </div>
      <Handle id="b" type="source" position={Position.Bottom} style={{ opacity: 0 }} />
      <Handle id="r" type="source" position={Position.Right} style={{ opacity: 0 }} />
      <Handle id="ls" type="source" position={Position.Left} style={{ opacity: 0 }} />
    </div>
  );
}

const nodeTypes = { item: ItemNode };

export function ImpactGraph({ graph, onPick, picked }: { graph: Graph; onPick: (id: string | null) => void; picked: string | null }) {
  const [hover, setHover] = useState<string | null>(null);
  const focus = picked ?? hover;

  const connected = useMemo(() => {
    if (!focus) return null;
    const ids = new Set([focus]);
    graph.edges.forEach((e) => {
      if (e.src_item === focus) ids.add(e.dst_item);
      if (e.dst_item === focus) ids.add(e.src_item);
    });
    return ids;
  }, [focus, graph.edges]);

  const nodes: Node<FlowData>[] = useMemo(() => {
    const rows: Record<string, number> = { IS: 0, BS: 0, CF: 0 };
    const order = [...graph.nodes].sort((a, b) => rank(a.id) - rank(b.id));
    return order.map((n) => ({
      id: n.id,
      type: "item",
      position: { x: LANE_X[n.statement] ?? 0, y: ROW_H * rows[n.statement]++ },
      data: { ...n, dim: !!connected && !connected.has(n.id), onPick: (id: string) => onPick(id === picked ? null : id) },
      draggable: true,
    }));
  }, [graph.nodes, connected, onPick, picked]);

  const edges: Edge[] = useMemo(
    () =>
      graph.edges.map((e) => {
        const on = !connected || (connected.has(e.src_item) && connected.has(e.dst_item));
        const lane = (id: string) => LANE_X[graph.nodes.find((n) => n.id === id)?.statement ?? "IS"] ?? 0;
        const [sx, tx] = [lane(e.src_item), lane(e.dst_item)];
        const handles =
          sx === tx ? { sourceHandle: "b", targetHandle: "t" }
          : sx < tx ? { sourceHandle: "r", targetHandle: "l" }
          : { sourceHandle: "ls", targetHandle: "rt" };
        return {
          id: `${e.src_item}-${e.dst_item}`,
          source: e.src_item,
          target: e.dst_item,
          // right angles inside a statement; curves between statements, so a line that
          // crosses the balance-sheet lane never looks as if it starts at a box there
          type: sx === tx ? "smoothstep" : "default",
          ...handles,
          label: on && focus ? e.relationship : undefined,
          animated: on && !!focus,
          style: { stroke: "var(--text-muted)", strokeWidth: 1.5, opacity: on ? 1 : 0.2 },
          markerEnd: { type: MarkerType.ArrowClosed, color: "var(--text-muted)" },
        };
      }),
    [graph.edges, connected, focus],
  );

  if (!graph.nodes.length) return <p className="muted">No linked line items to draw for this period.</p>;
  return (
    <>
      <div className="lane-labels" aria-hidden="true">
        <span>Income statement</span>
        <span>Balance sheet</span>
        <span>Cash flow</span>
      </div>
      <div className="graph-wrap" onMouseLeave={() => setHover(null)}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          fitView
          fitViewOptions={{ padding: 0.15 }}
          minZoom={0.3}
          onNodeMouseEnter={(_, n) => setHover(n.id)}
          onNodeMouseLeave={() => setHover(null)}
          onPaneClick={() => onPick(null)}
          proOptions={{ hideAttribution: true }}
        >
          <Background gap={18} size={1} color="var(--border)" />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
      <p className="hint" style={{ marginTop: 8 }}>
        Solid boxes were reported in the restatement; dashed boxes are where the accounting link says the effect must land.
        Hover or click a box to follow its connections; the matching row is highlighted in the table.
      </p>
    </>
  );
}
