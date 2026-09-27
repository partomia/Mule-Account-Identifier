"""Cached reads of the gold / silver tables for the Streamlit app."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from mule.schema import OUTPUT_TABLES
from mule.storage import get_storage

DATE_COLS = ("run_date", "snapshot_date", "context_from", "context_to", "holdout_test_from", "holdout_test_to",
             "first_seen", "decided_at")


@st.cache_resource
def storage():
    return get_storage()


def _dates(df: pd.DataFrame) -> pd.DataFrame:
    for c in DATE_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c]).dt.date
    return df


@st.cache_data(ttl=300, show_spinner="Reading gold tables...")
def read(key: str) -> pd.DataFrame:
    return _dates(storage().read(key))


def for_run(key: str, run_date) -> pd.DataFrame:
    try:
        df = read(key)
    except Exception:  # a run whose gate failed with no prior publish leaves this table missing
        return pd.DataFrame(columns=[c for c, _ in OUTPUT_TABLES[key]])
    return df[df["run_date"] == run_date].copy()


@st.cache_data(ttl=300, show_spinner="Reading features...")
def features_on(snapshot_date) -> pd.DataFrame:
    return storage().features(date_from=snapshot_date, date_to=snapshot_date)


@st.cache_data(ttl=300, show_spinner="Reading the ring...")
def ring_members(snapshot_date, ring_id: str) -> pd.DataFrame:
    feats = features_on(snapshot_date)
    return feats[feats["ring_id"] == ring_id].copy()


@st.cache_data(ttl=300, show_spinner="Reading linked identities...")
def identity_edges_for(cifs: tuple[str, ...]) -> pd.DataFrame:
    edges = _dates(storage().read("identity_edges"))
    if edges.empty:
        return edges
    cifs = set(cifs)
    return edges[edges["src_cif"].isin(cifs) | edges["dst_cif"].isin(cifs)].copy()


@st.cache_data(ttl=300, show_spinner="Reading money-flow links...")
def graph_edges_for(cifs: tuple[str, ...], snapshot_date) -> pd.DataFrame:
    edges = _dates(storage().read("graph_edges"))
    if edges.empty:
        return edges
    cifs = set(cifs)
    edges = edges[edges["snapshot_date"] == snapshot_date]
    return edges[edges["src_cif"].isin(cifs) | edges["dst_cif"].isin(cifs)].copy()


@st.cache_data(ttl=600, show_spinner="Reading customers...")
def customers(cifs: tuple[str, ...]) -> pd.DataFrame:
    cust = _dates(storage().read("customer"))
    return cust[cust["cif"].isin(set(cifs))].copy()


def decisions() -> pd.DataFrame:
    try:
        return _dates(storage().read("investigator_decisions"))
    except Exception:  # table not created until the first decision is recorded
        return pd.DataFrame()
