"""Mule account identifier: Investigator Console.

  streamlit run app/streamlit_app.py            # storage from config/mule.yaml
  MULE_STORAGE_BACKEND=parquet streamlit run app/streamlit_app.py
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    ROOT = Path(__file__).resolve().parent.parent
except NameError:
    ROOT = Path(os.getcwd())
sys.path.insert(0, str(ROOT))

import networkx as nx  # noqa: E402
import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from app import data  # noqa: E402
from mule.client import endpoint_configured, score_accounts  # noqa: E402
from mule.config import policy, table  # noqa: E402
from mule.features import FEATURES  # noqa: E402
from mule.reasons import NO_TIER  # noqa: E402
from mule.schema import DECISIONS  # noqa: E402

st.set_page_config(page_title="Mule account identifier", page_icon=":spider_web:", layout="wide")

TIER_ORDER = ["T1_FREEZE_REVIEW", "T2_HOLD_MONITOR", "T3_WATCHLIST"]
TIER_LABEL = {"T1_FREEZE_REVIEW": "T1 · Freeze review", "T2_HOLD_MONITOR": "T2 · Hold & monitor",
              "T3_WATCHLIST": "T3 · Watchlist", NO_TIER: "Not alerted"}
TIER_COLOR = {"T1_FREEZE_REVIEW": "#e4572e", "T2_HOLD_MONITOR": "#f3a712", "T3_WATCHLIST": "#2e86ab",
              NO_TIER: "#9aa5b1"}


def pct(x) -> str:
    return "n/a" if x is None or pd.isna(x) else f"{x:.1%}"


# ---------------------------------------------------------------- ring / identity graph rendering
def _layout(G: nx.Graph) -> dict:
    k = 1.3 / max(len(G.nodes()) ** 0.5, 0.3)
    return nx.spring_layout(G, seed=42, k=k)


def _edge_trace(G: nx.Graph, pos: dict, kind: str, color: str, dash: str | None = None) -> go.Scatter:
    xs, ys = [], []
    for u, v, d in G.edges(data=True):
        if d.get("kind") != kind:
            continue
        xs += [pos[u][0], pos[v][0], None]
        ys += [pos[u][1], pos[v][1], None]
    return go.Scatter(x=xs, y=ys, mode="lines", line=dict(width=1.5, color=color, dash=dash), hoverinfo="none",
                      showlegend=False)


def render_graph(G: nx.Graph) -> go.Figure:
    if not G.nodes():
        return go.Figure()
    pos = _layout(G)
    traces = [_edge_trace(G, pos, "identity", "#9aa5b1"), _edge_trace(G, pos, "money", "#e4572e", "dot"),
             _edge_trace(G, pos, "both", "#1b1b1e")]
    tiers = [G.nodes[n].get("tier", NO_TIER) for n in G.nodes()]
    node_trace = go.Scatter(
        x=[pos[n][0] for n in G.nodes()], y=[pos[n][1] for n in G.nodes()], mode="markers+text",
        text=[G.nodes[n].get("label", n) for n in G.nodes()], textposition="top center",
        marker=dict(size=18, color=[TIER_COLOR.get(t, "#9aa5b1") for t in tiers], line=dict(width=1, color="#1b1b1e")),
        hovertext=[f"{G.nodes[n].get('label', n)}<br>{TIER_LABEL.get(t, 'not alerted')}"
                  for n, t in zip(G.nodes(), tiers)],
        hoverinfo="text", showlegend=False)
    fig = go.Figure(traces + [node_trace])
    fig.update_layout(height=460, margin=dict(l=10, r=10, t=10, b=10), xaxis=dict(visible=False),
                      yaxis=dict(visible=False), plot_bgcolor="rgba(0,0,0,0)")
    return fig


def identity_ego_graph(center_cif: str, edges: pd.DataFrame, tier_by_cif: pd.Series) -> nx.Graph:
    G = nx.Graph()
    G.add_node(center_cif, label=center_cif, tier=tier_by_cif.get(center_cif, NO_TIER))
    for _, r in edges.iterrows():
        other = r["dst_cif"] if r["src_cif"] == center_cif else r["src_cif"]
        G.add_node(other, label=other, tier=tier_by_cif.get(other, NO_TIER))
        G.add_edge(center_cif, other, kind="identity")
    return G


def tier_by_cif(alerts: pd.DataFrame) -> pd.Series:
    """One tier per CIF, keeping the most severe when a customer has more than
    one alerted account (tier codes sort T1 < T2 < T3 lexically)."""
    if alerts.empty:
        return pd.Series(dtype=object)
    return alerts.sort_values("tier").drop_duplicates("cif").set_index("cif")["tier"]


def ring_graph(members: pd.DataFrame, id_edges: pd.DataFrame, money_edges: pd.DataFrame,
              tier_by_cif: pd.Series) -> nx.Graph:
    G = nx.Graph()
    label_by_cif = members.drop_duplicates("cif").set_index("cif")["account_id"]
    for cif in members["cif"]:
        G.add_node(cif, label=label_by_cif.get(cif, cif), tier=tier_by_cif.get(cif, NO_TIER))
    for _, r in id_edges.iterrows():
        if G.has_node(r["src_cif"]) and G.has_node(r["dst_cif"]):
            G.add_edge(r["src_cif"], r["dst_cif"], kind="identity")
    for _, r in money_edges.iterrows():
        if not (G.has_node(r["src_cif"]) and G.has_node(r["dst_cif"])):
            continue
        if G.has_edge(r["src_cif"], r["dst_cif"]):
            G[r["src_cif"]][r["dst_cif"]]["kind"] = "both"
        else:
            G.add_edge(r["src_cif"], r["dst_cif"], kind="money")
    return G


# ---------------------------------------------------------------- sidebar
try:
    runs = data.read("mule_model_run").sort_values("run_date", ascending=False)
except Exception as e:  # no data yet / connection problem
    st.error(f"Could not read {table('mule_model_run')} via {data.storage().name}: {e}")
    st.info("Run the daily job first (cai/jobs/daily_score.py), or set MULE_STORAGE_BACKEND=parquet with "
            "exported tables in data/parquet.")
    st.stop()

if runs.empty:
    st.warning("No scoring run yet. Run cai/jobs/daily_score.py first.")
    st.stop()

st.sidebar.title("Scoring run")
run_date = st.sidebar.selectbox("Run date", runs["run_date"].tolist(), format_func=lambda d: d.strftime("%a %d %b %Y"))
run = runs[runs["run_date"] == run_date].iloc[0]
st.sidebar.caption(f"Run `{run.run_id}`  \nModel `{run.model_id}` on `{run.device}`  \n"
                   f"Context {int(run.context_rows):,} rows ({int(run.context_mules)} mules)  \n"
                   f"Storage: {data.storage().name}")
if pd.notna(run.get("capture_top1")):
    st.sidebar.metric("Top 1% of the book catches", pct(run.capture_top1), f"rules alone {pct(run.rules_capture_top1)}",
                      delta_color="off", help="Holdout on the latest labelled weeks, scored with a context from "
                                              "older weeks only.")
if pd.notna(run.get("gate_passed")):
    (st.sidebar.success if bool(run.gate_passed) else st.sidebar.error)(
        f"KPI gate: {'PASS' if bool(run.gate_passed) else 'FAIL'} · alerts published: {bool(run.alerts_published)}")
st.sidebar.divider()
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.divider()
st.sidebar.caption("Demo on synthetic data, not a validated fraud model. The model only ranks accounts for human "
                   "review; every tier is a recommended action, not an automatic one — freezing an account goes "
                   "through the maker-checker decision flow.")

alerts = data.for_run("mule_alerts", run_date).sort_values("risk_rank")
rings = data.for_run("mule_rings", run_date).sort_values("sum_p_mule_adj", ascending=False)
holdout = data.for_run("mule_holdout", run_date)

st.title("Mule account identifier: investigator console")
st.caption(f"Run {run_date:%d %b %Y}. {run.model_family} scores every active account (a credit or debit in the "
           "last 30 days, not already frozen or reported) for the chance of being a mule, learning in context "
           "from labelled history in the gold Iceberg table. Tiers and reasons are business rules on top of the "
           "score, not an explanation of it.")

tab_queue, tab_linked, tab_ring, tab_whatif, tab_trust, tab_decisions, tab_lineage = st.tabs(
    ["Alert queue", "Linked identities", "Ring view", "What-if", "Holdout & trust", "Decisions", "Lineage"])

# ---------------------------------------------------------------- alert queue
with tab_queue:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Accounts scored", f"{int(run.scored_accounts):,}")
    c2.metric("Alerts", f"{len(alerts):,}", f"{len(alerts) / max(int(run.scored_accounts), 1):.2%} of the book",
              delta_color="off")
    c3.metric("Rings with an alert", f"{len(rings):,}")
    c4.metric("Book mule rate (measured)", pct(run.book_mule_rate))

    left, right = st.columns([3, 2])
    with right:
        counts = alerts["tier"].value_counts().reindex(TIER_ORDER).fillna(0)
        fig = go.Figure(go.Bar(x=[TIER_LABEL[t] for t in counts.index], y=counts.values,
                               marker_color=[TIER_COLOR[t] for t in counts.index],
                               text=[f"{int(n):,}" for n in counts.values], textposition="outside"))
        fig.update_layout(title="Alerts by tier", height=340, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, width="stretch")
    with left:
        f1, f2, f3 = st.columns(3)
        tier_f = f1.multiselect("Tier", TIER_ORDER, default=TIER_ORDER, format_func=TIER_LABEL.get)
        branch_f = f2.multiselect("Branch", sorted(alerts["branch_code"].dropna().unique()))
        search = f3.text_input("Account or CIF contains")
        view = alerts[alerts["tier"].isin(tier_f)]
        if branch_f:
            view = view[view["branch_code"].isin(branch_f)]
        if search:
            s = search.strip().lower()
            view = view[view["account_id"].str.lower().str.contains(s) | view["cif"].str.lower().str.contains(s)]
        st.dataframe(
            view.assign(tier=view["tier"].map(TIER_LABEL).fillna(view["tier"]))[
                ["risk_rank", "account_id", "cif", "branch_code", "product_code", "tier", "p_mule_adj", "ring_size",
                 "hops_to_known_mule", "reasons"]],
            hide_index=True, width="stretch", height=430, column_config={
                "risk_rank": st.column_config.NumberColumn("#", format="%d"),
                "account_id": "Account", "cif": "CIF", "branch_code": "Branch", "product_code": "Product",
                "tier": "Tier", "p_mule_adj": st.column_config.ProgressColumn("P(mule)", min_value=0, max_value=1,
                                                                              format="%.3f"),
                "ring_size": st.column_config.NumberColumn("Ring size"),
                "hops_to_known_mule": st.column_config.NumberColumn("Hops"),
                "reasons": st.column_config.TextColumn("Reasons", width="large")})
        st.download_button("Download alert queue (CSV)", view.to_csv(index=False).encode(),
                           f"alerts_{run_date:%Y%m%d}.csv", "text/csv")
    st.caption("Reasons are business rules on the inputs to help the investigator open the case; they are not an "
               "explanation of the model's score.")

# ---------------------------------------------------------------- linked identities
with tab_linked:
    st.markdown("Identifiers shared with other customers: a non-hub device, mobile, PAN hash or address hash "
               "(bank-wide hubs like branch tablets and BC-agent terminals are excluded, so every edge here is "
               "person-to-person).")
    options = alerts["account_id"].tolist()
    if not options:
        st.info("No alerts on this run.")
    else:
        account_id = st.selectbox("Account", options, key="linked_account",
                                  format_func=lambda a: f"{a} · {TIER_LABEL.get(alerts.set_index('account_id').loc[a, 'tier'], '?')}")
        row = alerts.set_index("account_id").loc[account_id]
        cif = row["cif"]
        st.caption(f"CIF `{cif}` · branch {row['branch_code']} · product {row['product_code']} · "
                   f"person cluster `{row['person_cluster_id']}` · ring size {int(row['ring_size'])}")
        edges = data.identity_edges_for((cif,))
        if edges.empty:
            st.info("No shared identifiers on record for this customer.")
        else:
            tiers = tier_by_cif(alerts)
            st.plotly_chart(render_graph(identity_ego_graph(cif, edges, tiers)), width="stretch")
            other = edges.apply(lambda r: r["dst_cif"] if r["src_cif"] == cif else r["src_cif"], axis=1)
            view = edges.assign(other_cif=other)
            cust = data.customers(tuple(view["other_cif"].unique()))
            view = view.merge(cust[["cif", "customer_name", "occupation", "home_branch"]], left_on="other_cif",
                              right_on="cif", how="left", suffixes=("", "_cust"))
            view["alerted_this_run"] = view["other_cif"].isin(alerts["cif"])
            view["other_tier"] = view["other_cif"].map(tiers).map(TIER_LABEL)
            st.dataframe(
                view[["other_cif", "customer_name", "link_type", "first_seen", "occupation", "home_branch",
                     "alerted_this_run", "other_tier"]]
                .sort_values(["alerted_this_run", "first_seen"], ascending=[False, True]),
                hide_index=True, width="stretch", column_config={
                    "other_cif": "CIF", "customer_name": "Name", "link_type": "Shared identifier",
                    "first_seen": "First shared", "occupation": "Occupation", "home_branch": "Home branch",
                    "alerted_this_run": "Alerted this run", "other_tier": "Their tier"})

# ---------------------------------------------------------------- ring view
with tab_ring:
    st.markdown("The wider money-flow ring: identity links plus own-bank transfers / shared counterparties, "
               "point-in-time as of this run's snapshot. Money-flow edges are person-to-person only (bank-wide "
               "hubs are excluded).")
    ring_options = rings["ring_id"].tolist()
    if not ring_options:
        st.info("No rings with an alert on this run.")
    else:
        ring_lookup = rings.set_index("ring_id")
        ring_id = st.selectbox("Ring", ring_options, key="ring_select",
                               format_func=lambda r: f"{r} (size {int(ring_lookup.loc[r, 'ring_size'])}, "
                                                     f"{int(ring_lookup.loc[r, 'accounts_alerted'])} alerted)")
        members = data.ring_members(run.snapshot_date, ring_id)
        c1, c2, c3 = st.columns(3)
        c1.metric("Accounts in ring", f"{len(members):,}")
        c2.metric("Alerted this run", f"{members['account_id'].isin(alerts['account_id']).sum():,}")
        known = int(members["is_mule_90d"].fillna(0).sum()) if "is_mule_90d" in members else None
        c3.metric("Known mules (label)", f"{known}" if known is not None else "n/a")

        cifs = tuple(members["cif"].unique())
        id_edges = data.identity_edges_for(cifs)
        id_edges = id_edges[id_edges["src_cif"].isin(cifs) & id_edges["dst_cif"].isin(cifs)]
        money_edges = data.graph_edges_for(cifs, run.snapshot_date)
        money_edges = money_edges[money_edges["src_cif"].isin(cifs) & money_edges["dst_cif"].isin(cifs)]

        st.plotly_chart(render_graph(ring_graph(members, id_edges, money_edges, tier_by_cif(alerts))), width="stretch")
        view = members.merge(alerts[["account_id", "tier", "p_mule_adj", "risk_rank"]], on="account_id", how="left")
        view["tier"] = view["tier"].fillna(NO_TIER).map(TIER_LABEL)
        st.dataframe(
            view[["account_id", "cif", "branch_code", "product_code", "tier", "p_mule_adj", "hops_to_known_mule",
                 "accounts_on_same_device", "cifs_sharing_mobile"]].sort_values("p_mule_adj", ascending=False,
                                                                                na_position="last"),
            hide_index=True, width="stretch", column_config={
                "account_id": "Account", "cif": "CIF", "branch_code": "Branch", "product_code": "Product",
                "tier": "Tier", "p_mule_adj": st.column_config.ProgressColumn("P(mule)", min_value=0, max_value=1,
                                                                              format="%.3f"),
                "hops_to_known_mule": "Hops", "accounts_on_same_device": "Accounts / device",
                "cifs_sharing_mobile": "CIFs / mobile"})

# ---------------------------------------------------------------- what-if
with tab_whatif:
    st.markdown("Score one account live, then change a few things an investigator might ask about. "
               + ("Calls the **CAI model endpoint**." if endpoint_configured()
                  else "No endpoint configured: scoring runs in this app."))
    feats = data.features_on(run.snapshot_date).set_index("account_id")
    options = [a for a in alerts["account_id"] if a in feats.index] or feats.index.tolist()[:200]
    account_id = st.selectbox("Account", options, key="whatif_account")
    base = feats.loc[account_id, FEATURES].to_dict()
    c1, c2, c3, c4 = st.columns(4)
    dev = c1.slider("New device logins (30d)", 0, 5, min(int(base["new_device_logins_30d"]), 5))
    vpa = c2.slider("VPA / mobile changes (30d)", 0, 5, min(int(base["vpa_or_mobile_changes_30d"]), 5))
    pass_through = c3.slider("Pass-through ratio (7d)", 0.0, 1.0, float(base["pass_through_ratio_7d"]), step=0.05)
    hops = c4.slider("Hops to a known mule", 1, 3, min(max(int(base["hops_to_known_mule"]), 1), 3))
    st.json({k: (int(v) if float(v).is_integer() else v) for k, v in base.items()}, expanded=False)
    if st.button("Score", type="primary", key="whatif_score"):
        req = {"accounts": [{"account_id": account_id, **base}],
              "what_if": {"new_device_logins_30d": dev, "vpa_or_mobile_changes_30d": vpa,
                          "pass_through_ratio_7d": pass_through, "hops_to_known_mule": hops}}
        with st.spinner("Scoring..."):
            try:
                resp, src = score_accounts(req)
            except Exception as e:
                st.error(f"Scoring failed: {e}")
                st.stop()
        if "error" in resp:
            st.error(resp["error"])
            st.stop()
        s = resp["scores"][0]
        c1, c2, c3 = st.columns(3)
        c1.metric("P(mule) as is", f"{s['p_mule_adj']:.2%}", TIER_LABEL.get(s["tier"], "not alerted"),
                  delta_color="off")
        c2.metric("P(mule) with changes", f"{s['p_mule_adj_what_if']:.2%}",
                  f"{(s['p_mule_adj_what_if'] - s['p_mule_adj']) * 100:+.2f} pts", delta_color="inverse")
        c3.metric("Tier with changes", TIER_LABEL.get(s.get("tier_what_if"), "not alerted"))
        st.caption(f"Scored by: {src} · reasons as is: {s['reasons'] or 'none'} · reasons with changes: "
                   f"{s.get('reasons_what_if') or 'none'} · context: {resp.get('model', {}).get('context_rows', '?')} rows")

# ---------------------------------------------------------------- holdout & trust
with tab_trust:
    if pd.isna(run.get("capture_top1")):
        st.warning("This run skipped the holdout check.")
    else:
        st.markdown(f"**Holdout**: accounts on the {run.holdout_test_from:%d %b} to {run.holdout_test_to:%d %b} "
                    f"snapshots ({int(run.holdout_rows):,} accounts, {int(run.holdout_mules)} mules, "
                    f"{pct(run.holdout_mule_rate)} mule rate), scored with a context that ends before the first "
                    f"test week ({int(run.holdout_context_rows):,} rows), as if the model had been deployed then.")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Capture top 1%", pct(run.capture_top1), f"rules alone {pct(run.rules_capture_top1)}",
                  delta_color="off")
        c2.metric("Precision top 0.2% (T1)", pct(run.precision_top02))
        c3.metric("Lift over rules (top 1%)", f"{run.lift_over_rules:.2f}x" if pd.notna(run.lift_over_rules) else "n/a")
        c4.metric("Holdout AUC", f"{run.holdout_auc:.3f}" if pd.notna(run.holdout_auc) else "n/a")

        gate = policy()["gate"]
        checks = [("Capture top 1%", run.get("capture_top1"), gate.get("min_capture_top1pct")),
                 ("Precision top 0.2%", run.get("precision_top02"), gate.get("min_precision_top02pct")),
                 ("Lift over rules", run.get("lift_over_rules"), gate.get("min_lift_over_rules"))]
        for label, v, need in checks:
            if need is None:
                continue
            ok = v is not None and pd.notna(v) and v >= need
            msg = f"{'PASS' if ok else 'FAIL'} {label}: {v:.3f} (min {need:.3f})" if v is not None and pd.notna(v) \
                else f"n/a: {label}"
            (st.success if ok else st.error)(msg)

        if not holdout.empty:
            h = holdout.sort_values("upto_pct")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=h["band"], y=h["mule_rate"], name="Mule rate in band", marker_color="#e4572e"))
            fig.add_trace(go.Scatter(x=h["band"], y=h["cum_capture"], name="Cumulative capture (model)",
                                     mode="lines+markers", line=dict(color="#2e86ab")))
            fig.add_trace(go.Scatter(x=h["band"], y=h["rules_cum_capture"], name="Cumulative capture (rules only)",
                                     mode="lines+markers", line=dict(color="#9aa5b1", dash="dot")))
            fig.update_layout(title="Holdout by risk band (top 0.2% = highest)", height=400,
                              yaxis=dict(tickformat=".0%"), legend=dict(orientation="h", y=-0.25),
                              margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig, width="stretch")
        st.caption("Rules alone = the business rules behind the reason codes, counted. At this mule rate a model "
                   "that flags nobody is over 99.9% accurate, so the gate reads capture, precision and lift over "
                   "rules instead of accuracy.")

# ---------------------------------------------------------------- decisions
with tab_decisions:
    st.markdown(f"Maker-checker: record what an investigator decided about an alerted account. Decisions are "
               f"appended to `{table('investigator_decisions')}`; the next CDE silver load turns a CONFIRMED_MULE "
               "into a label (and a known mule for the graph) in tomorrow's features.")
    options = alerts["account_id"].tolist()
    if not options:
        st.info("No alerts on this run to decide on.")
    else:
        with st.form("decision"):
            c1, c2 = st.columns(2)
            d_account = c1.selectbox("Account", options)
            d_decision = c2.selectbox("Decision", DECISIONS)
            row = alerts.set_index("account_id").loc[d_account]
            c3, c4 = st.columns(2)
            d_action = c3.selectbox("Action taken", [row["action"], "Escalated to FRM", "No action"])
            d_maker = c4.text_input("Maker", value=os.environ.get("HADOOP_USER_NAME", "investigator"))
            d_checker = st.text_input("Checker (second approver)")
            d_notes = st.text_input("Notes")
            submitted = st.form_submit_button("Save decision", type="primary")
        if submitted:
            now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
            new_row = pd.DataFrame([{"decision_id": f"APP-{uuid.uuid4().hex[:12]}", "run_date": run_date,
                                    "account_id": d_account, "cif": row["cif"], "decision": d_decision,
                                    "action": d_action, "maker": d_maker, "checker": d_checker, "notes": d_notes,
                                    "decided_at": now, "ingested_at": now}])
            try:
                data.storage().append("investigator_decisions", new_row)
                st.success(f"Saved: {d_account} -> {d_decision}" + (f" (checked by {d_checker})" if d_checker else ""))
            except Exception as e:
                st.error(f"Could not save: {e}")
    recent = data.decisions()
    if not recent.empty:
        st.dataframe(recent.sort_values("decided_at", ascending=False).head(50), hide_index=True, width="stretch")

# ---------------------------------------------------------------- lineage
with tab_lineage:
    st.markdown("**Gate metrics by run**. Every daily run is kept, keyed by run date.")
    h = runs.sort_values("run_date")
    fig = go.Figure()
    for col, name, dash in (("capture_top1", "Capture top 1%", None), ("rules_capture_top1", "Rules alone", "dot"),
                            ("precision_top02", "Precision top 0.2%", None), ("holdout_auc", "AUC", "dash")):
        if col in h:
            fig.add_trace(go.Scatter(x=h["run_date"], y=h[col], name=name, mode="lines+markers", line=dict(dash=dash)))
    fig.update_layout(height=340, yaxis=dict(tickformat=".0%", range=[0, 1]), margin=dict(l=10, r=10, t=20, b=10),
                      legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, width="stretch")

    st.markdown("**Lineage**: each run records the model checkpoint, the context window and the Iceberg snapshot "
               "of the gold table it read.")
    cols = ["run_date", "run_id", "run_ts", "model_family", "model_id", "device", "scored_accounts", "context_rows",
           "context_from", "context_to", "source_snapshot_id", "gate_passed", "alerts_published", "triggered_by",
           "duration_s"]
    st.dataframe(runs[[c for c in cols if c in runs]], hide_index=True, width="stretch")
    snap = run.source_snapshot_id if isinstance(run.source_snapshot_id, str) and run.source_snapshot_id \
        else "<snapshot_id>"
    st.markdown("Rebuild the exact context of this run in Hue (CDW Impala):")
    st.code(f"SELECT * FROM {table('mule_features')}\n  FOR SYSTEM_VERSION AS OF {snap}\n"
           f"  WHERE is_mule_90d IS NOT NULL\n"
           f"    AND snapshot_date BETWEEN DATE '{run.context_from}' AND DATE '{run.context_to}'\n"
           f"  ORDER BY snapshot_date DESC, account_id LIMIT {int(run.context_rows)};", language="sql")
