import streamlit as st
import pandas as pd
import plotly.express as px
import hashlib
import io
import json
import time
import requests
from datetime import datetime, timezone

st.set_page_config(page_title="Dashboard App", layout="wide", page_icon="📊")

@st.cache_resource
def get_http_session():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=8)
    s.mount("https://", adapter)
    return s

def http():
    return get_http_session()

def fb_sign_in_email(api_key, email, password):
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={api_key}"
    r = http().post(url, json={"email": email, "password": password, "returnSecureToken": True}, timeout=10)
    d = r.json()
    if "error" in d:
        return None, None, None, d["error"].get("message", "Auth failed")
    return d["idToken"], d["refreshToken"], d["localId"], None

def fb_sign_in_anonymous(api_key):
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={api_key}"
    r = http().post(url, json={"returnSecureToken": True}, timeout=10)
    d = r.json()
    if "error" in d:
        return None, None, None, d["error"].get("message", "Anonymous auth failed")
    return d["idToken"], d["refreshToken"], d["localId"], None

def fb_refresh_token(api_key, refresh_token):
    url = f"https://securetoken.googleapis.com/v1/token?key={api_key}"
    r = http().post(url, json={"grant_type": "refresh_token", "refresh_token": refresh_token}, timeout=10)
    d = r.json()
    if "error" in d:
        return None, d["error"].get("message", "Refresh failed")
    return d.get("id_token"), None

def fb_firestore_list_collections(project_id, id_token):
    headers = {"Authorization": f"Bearer {id_token}"}
    cols = set()
    warning = None
    try:
        url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
               f"/databases/(default)/documents:listCollectionIds")
        r = http().post(url, headers=headers, json={}, timeout=15)
        if r.status_code == 200 and r.text.strip():
            data = r.json()
            cols.update(data.get("collectionIds", []))
    except Exception:
        pass
    if not cols:
        try:
            url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
                   f"/databases/(default)/documents")
            r = http().get(url, headers=headers, params={"pageSize": 20}, timeout=15)
            if r.status_code == 200 and r.text.strip():
                for doc in r.json().get("documents", []):
                    name = doc.get("name", "")
                    parts = name.split("/documents/", 1)
                    if len(parts) == 2:
                        cols.add(parts[1].split("/")[0])
        except Exception:
            pass
    if not cols:
        warning = (
            "Could not auto-detect collections. "
            "Type your collection name manually below."
        )
    return sorted(cols), warning

def parse_firestore_value(v):
    if "stringValue" in v:    return v["stringValue"]
    if "integerValue" in v:   return int(v["integerValue"])
    if "doubleValue" in v:    return float(v["doubleValue"])
    if "booleanValue" in v:   return v["booleanValue"]
    if "nullValue" in v:      return None
    if "timestampValue" in v: return v["timestampValue"]
    if "arrayValue" in v:
        return [parse_firestore_value(i) for i in v["arrayValue"].get("values", [])]
    if "mapValue" in v:
        return {k: parse_firestore_value(val)
                for k, val in v["mapValue"].get("fields", {}).items()}
    if "geoPointValue" in v:
        gp = v["geoPointValue"]
        return {"latitude": gp.get("latitude"), "longitude": gp.get("longitude")}
    if "referenceValue" in v: return v["referenceValue"]
    if "bytesValue" in v:     return v["bytesValue"]
    return None

@st.cache_data(ttl=300, show_spinner=False)
def fb_firestore_load_collection(project_id, id_token, collection_id, limit=1000):
    base = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
            f"/databases/(default)/documents/{collection_id}")
    headers = {"Authorization": f"Bearer {id_token}"}
    rows, page_token = [], None
    while len(rows) < limit:
        params = {"pageSize": min(300, limit - len(rows))}
        if page_token:
            params["pageToken"] = page_token
        r = http().get(base, headers=headers, params=params, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"Firestore {r.status_code}: {r.json().get('error',{}).get('message', r.text)}")
        data = r.json()
        for doc in data.get("documents", []):
            row = {k: parse_firestore_value(v) for k, v in doc.get("fields", {}).items()}
            row["_doc_id"] = doc["name"].rsplit("/", 1)[-1]
            rows.append(row)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return pd.json_normalize(rows) if rows else pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def fb_rtdb_list_nodes(database_url, path, id_token):
    path = "/" + path.strip("/")
    url = f"{database_url.rstrip('/')}{path}.json?shallow=true&auth={id_token}"
    r = http().get(url, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"RTDB {r.status_code}: {r.text[:200]}")
    val = r.json()
    if isinstance(val, dict):
        return list(val.keys())
    return []

@st.cache_data(ttl=300, show_spinner=False)
def fb_rtdb_load_node(database_url, path, id_token):
    path = "/" + path.strip("/")
    url = f"{database_url.rstrip('/')}{path}.json?auth={id_token}"
    r = http().get(url, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"RTDB {r.status_code}: {r.text[:200]}")
    val = r.json()
    if val is None:
        return pd.DataFrame()
    if isinstance(val, dict):
        rows = []
        for k, v in val.items():
            if isinstance(v, dict):
                row = dict(v); row["_key"] = k
            else:
                row = {"_key": k, "value": v}
            rows.append(row)
        return pd.json_normalize(rows) if rows else pd.DataFrame()
    if isinstance(val, list):
        return pd.DataFrame(val)
    return pd.DataFrame([{"value": val}])


CHART_SAMPLE_ROWS = 10000
CHART_MAX_PIE_SLICES = 30
CHART_MAX_BAR_CATS = 50

def apply_trim(df, col, trim_pct):
    if trim_pct <= 0 or col not in df.columns:
        return df
    numeric = pd.to_numeric(df[col], errors="coerce")
    lo = numeric.quantile(trim_pct / 100)
    hi = numeric.quantile(1 - trim_pct / 100)
    mask = (numeric >= lo) & (numeric <= hi)
    return df[mask]

@st.cache_data(max_entries=128, show_spinner=False)
def create_chart(data_hash, chart_type, x_var, y_var, data_dict,
                 pie_top_n=CHART_MAX_PIE_SLICES, sample_rows=CHART_SAMPLE_ROWS,
                 trim_pct=0.0,
                 agg='sum',
                 x_filter_min=None, x_filter_max=None,
                 y_filter_min=None, y_filter_max=None):
    df = pd.DataFrame(data_dict)
    sample_note = ""


    filter_notes = []
    if x_var and x_var in df.columns:
        x_num = pd.to_numeric(df[x_var], errors="coerce")
        mask = pd.Series([True] * len(df), index=df.index)
        if x_filter_min is not None:
            mask &= x_num >= x_filter_min
        if x_filter_max is not None:
            mask &= x_num <= x_filter_max
        if x_filter_min is not None or x_filter_max is not None:
            before = len(df)
            df = df[mask]
            removed = before - len(df)
            lo_str = f"{x_filter_min}" if x_filter_min is not None else "−∞"
            hi_str = f"{x_filter_max}" if x_filter_max is not None else "+∞"
            filter_notes.append(f"X filtered [{lo_str}, {hi_str}] — {removed:,} rows removed")
    if y_var and y_var in df.columns:
        y_num = pd.to_numeric(df[y_var], errors="coerce")
        mask = pd.Series([True] * len(df), index=df.index)
        if y_filter_min is not None:
            mask &= y_num >= y_filter_min
        if y_filter_max is not None:
            mask &= y_num <= y_filter_max
        if y_filter_min is not None or y_filter_max is not None:
            before = len(df)
            df = df[mask]
            removed = before - len(df)
            lo_str = f"{y_filter_min}" if y_filter_min is not None else "−∞"
            hi_str = f"{y_filter_max}" if y_filter_max is not None else "+∞"
            filter_notes.append(f"Y filtered [{lo_str}, {hi_str}] — {removed:,} rows removed")
    if filter_notes:
        sample_note = " · ".join(filter_notes)

    trim_col = None
    if chart_type in ("Bar Chart", "Line Chart", "Scatter Plot", "Area Chart") and y_var:
        trim_col = y_var
    elif chart_type in ("Box Plot", "Violin Plot") and y_var:
        trim_col = y_var
    elif chart_type == "Histogram" and x_var:
        trim_col = x_var

    if trim_pct > 0 and trim_col:
        before = len(df)
        df = apply_trim(df, trim_col, trim_pct)
        removed = before - len(df)
        if removed > 0:
            trim_note = f"Trimmed {removed:,} rows ({trim_pct:.1f}% each tail)"
            sample_note = trim_note

    try:
        if chart_type == "Pie Chart" and x_var:
            vc = df[x_var].value_counts()
            total_cats = len(vc)
            n = max(1, int(pie_top_n))
            if total_cats > n:
                top = vc.iloc[:n]
                other = vc.iloc[n:].sum()
                vc = pd.concat([top, pd.Series({"Other": other})])
                sample_note = f"Top {n} of {total_cats:,} categories shown"
            fig = px.pie(values=vc.values, names=vc.index)
        elif chart_type == "Bar Chart" and x_var and y_var:
            if agg == 'none':
                plot_df = df
                if len(plot_df) > sample_rows:
                    orig_len = len(plot_df)
                    plot_df = plot_df.sample(n=sample_rows, random_state=42)
                    note2 = f"Sampled {sample_rows:,} of {orig_len:,} rows"
                    sample_note = (sample_note + " · " + note2) if sample_note else note2
                fig = px.bar(plot_df, x=x_var, y=y_var)
            else:
                agg_fn = 'mean' if agg == 'mean' else 'sum'
                agg = df.groupby(x_var, observed=True)[y_var].agg(agg_fn).reset_index()
                if len(agg) > CHART_MAX_BAR_CATS:
                    agg = agg.nlargest(CHART_MAX_BAR_CATS, y_var)
                    note2 = f"Top {CHART_MAX_BAR_CATS} categories by {y_var} {agg_fn}"
                    sample_note = (sample_note + " · " + note2) if sample_note else note2
                fig = px.bar(agg, x=x_var, y=y_var)
        elif chart_type == "Histogram" and x_var:
            if len(df) > sample_rows:
                df = df[[x_var]].sample(n=sample_rows, random_state=42)
                note2 = f"Sampled {sample_rows:,} of {len(pd.DataFrame(_data_dict)):,} rows"
                sample_note = (sample_note + " · " + note2) if sample_note else note2
            fig = px.histogram(df, x=x_var)
        else:
            if chart_type == "Line Chart" and x_var and y_var:
                if agg in ('sum', 'mean'):
                    agg_fn = 'mean' if agg == 'mean' else 'sum'
                    plot_df = df.groupby(x_var, observed=True)[y_var].agg(agg_fn).reset_index().sort_values(x_var)
                else:
                    if len(df) > sample_rows:
                        orig_len = len(df)
                        df = df.sample(n=sample_rows, random_state=42)
                        note2 = f"Sampled {sample_rows:,} of {orig_len:,} rows"
                        sample_note = (sample_note + " · " + note2) if sample_note else note2
                    plot_df = df
                fig = px.line(plot_df, x=x_var, y=y_var)
            elif chart_type == "Area Chart" and x_var and y_var:
                if agg in ('sum', 'mean'):
                    agg_fn = 'mean' if agg == 'mean' else 'sum'
                    plot_df = df.groupby(x_var, observed=True)[y_var].agg(agg_fn).reset_index().sort_values(x_var)
                else:
                    if len(df) > sample_rows:
                        orig_len = len(df)
                        df = df.sample(n=sample_rows, random_state=42)
                        note2 = f"Sampled {sample_rows:,} of {orig_len:,} rows"
                        sample_note = (sample_note + " · " + note2) if sample_note else note2
                    plot_df = df
                fig = px.area(plot_df, x=x_var, y=y_var)
            elif chart_type == "Scatter Plot" and x_var and y_var:
                if len(df) > sample_rows:
                    orig_len = len(df)
                    df = df.sample(n=sample_rows, random_state=42)
                    note2 = f"Sampled {sample_rows:,} of {orig_len:,} rows"
                    sample_note = (sample_note + " · " + note2) if sample_note else note2
                fig = px.scatter(df, x=x_var, y=y_var)
            elif chart_type == "Box Plot" and y_var:
                if len(df) > sample_rows:
                    orig_len = len(df)
                    df = df.sample(n=sample_rows, random_state=42)
                    note2 = f"Sampled {sample_rows:,} of {orig_len:,} rows"
                    sample_note = (sample_note + " · " + note2) if sample_note else note2
                fig = px.box(df, x=x_var if x_var != "None" else None, y=y_var)
            elif chart_type == "Violin Plot" and y_var:
                if len(df) > sample_rows:
                    orig_len = len(df)
                    df = df.sample(n=sample_rows, random_state=42)
                    note2 = f"Sampled {sample_rows:,} of {orig_len:,} rows"
                    sample_note = (sample_note + " · " + note2) if sample_note else note2
                fig = px.violin(df, x=x_var if x_var != "None" else None, y=y_var)
            else:
                return None, ""
        title = sample_note if sample_note else ""
        fig.update_layout(
            height=320,
            margin=dict(l=20, r=20, t=40 if title else 30, b=20),
            title=dict(text=f"<i style='font-size:11px;color:orange'>{title}</i>",x=0, xanchor="left") if title else {},)
        return fig, sample_note
    except Exception:
        return None, ""

def get_data_hash(df):
    return hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()

@st.cache_data(max_entries=64, show_spinner=False)
def get_column_info(data_hash, df):
    all_cols = df.columns.tolist()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    return all_cols, numeric_cols

@st.cache_data(max_entries=64, show_spinner=False)
def get_cardinality(data_hash, df):
    return {col: int(df[col].nunique()) for col in df.columns}

def recommend_chart_types(x_col, y_col, df, numeric_cols, cardinality):

    if not x_col and not y_col:
        return []

    x_is_num = x_col in numeric_cols if x_col else False
    y_is_num = y_col in numeric_cols if y_col else False
    x_card = cardinality.get(x_col, 0) if x_col else 0

    recs = []


    if x_col and not y_col:
        if x_is_num:
            recs.append(("Histogram",   "Good for seeing the spread and shape of a numeric column."))
            recs.append(("Box Plot",    "Shows median, IQR, and outliers at a glance."))
            recs.append(("Violin Plot", "Like a box plot but also shows the distribution shape."))
        else:
            recs.append(("Bar Chart", "Count of each category — simple and readable."))
            recs.append(("Pie Chart", "Part-of-whole view; works best with fewer than ~10 categories."))
        return recs

    if x_is_num and y_is_num:
        recs.append(("Scatter Plot", "Best for spotting correlations or clusters between two numeric columns."))
        recs.append(("Line Chart",   "Good when X is ordered (e.g. time or a ranked sequence)."))
        recs.append(("Bar Chart",    "Works if X is really acting as a discrete group rather than a true number."))
    elif not x_is_num and y_is_num:
        if x_card <= 20:
            recs.append(("Bar Chart",   "Standard choice for comparing a numeric value across categories."))
            recs.append(("Box Plot",    "Shows the distribution of Y within each X group, including outliers."))
            recs.append(("Violin Plot", "Like a box plot but exposes the full distribution shape per group."))
            recs.append(("Line Chart",  "Good if the categories have a natural order (e.g. months, stages)."))
        else:
            recs.append(("Bar Chart",    f"X has {x_card:,} unique values — chart will aggregate and show top categories."))
            recs.append(("Scatter Plot", "Lots of categories; consider filtering or encoding X numerically first."))
    elif x_is_num and not y_is_num:
        recs.append(("Bar Chart",    "Aggregates X per Y category."))
        recs.append(("Scatter Plot", "Tip: swap X and Y so the categorical column is on the X axis."))
    else:
        recs.append(("Bar Chart", "Counts of X, optionally broken down by Y."))
        recs.append(("Pie Chart", "Part-of-whole; keep the number of categories small."))

    return recs

@st.cache_data(max_entries=32, show_spinner=False)
def compute_stats(data_hash, df):
    num_df = df.select_dtypes(include="number")
    describe = num_df.describe().T if not num_df.empty else pd.DataFrame()
    miss = df.isnull().sum().reset_index()
    miss.columns = ["Column", "Missing"]
    miss["% Missing"] = (miss["Missing"] / len(df) * 100).round(2)
    miss = miss.sort_values("Missing", ascending=False)
    cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
    return describe, miss, cat_cols


def make_dashboard(did, name, dataset_id=None):
    return {
        'id': did, 'name': name, 'dataset_id': dataset_id,
        'visualizations': [], 'next_viz_id': 0, 'cols_per_row': 3,
    }

def make_dataset(did, name, df):
    h = get_data_hash(df)
    return {'id': did, 'name': name, 'df': df, 'data_dict': df.to_dict('list'), 'data_hash': h}

DEFAULTS = {
    'screen': 'config',
    'datasets': {},
    'next_dataset_id': 0,
    'active_dataset_id': None,
    'dashboards': None,
    'next_dashboard_id': 1,
    'active_dashboard_id': 0,
    'renaming_dashboard_id': None,
    'confirmed_imports': set(),
    'pending_file_names': set(),
    'fb_app_cache': {},
    'fb_config': None,
    'fb_id_token': None,
    'fb_refresh_token': None,
    'fb_uid': None,
    'fb_auth_method': 'email',
    'selected_viz_id': None,
    'auto_refresh_enabled': False,
    'auto_refresh_interval': 30,
    'last_firebase_refresh': 0.0,
    'auto_refresh_status': 'idle',
    'auto_refresh_last_error': '',
    'auto_refresh_rows_changed': {},
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v
if st.session_state.dashboards is None:
    st.session_state.dashboards = [make_dashboard(0, 'Dashboard 1')]


def export_config() -> dict:
    datasets_meta = {}
    for did, ds in st.session_state.datasets.items():
        df = ds["df"]
        datasets_meta[str(did)] = {
            "id": did,
            "name": ds["name"],
            "columns": df.columns.tolist(),
            "dtypes": {col: str(df[col].dtype) for col in df.columns},
            "row_count": len(df),
            "fb_source": ds.get("fb_source"),
        }

    def ser_viz(v):
        return {
            "id": v["id"],
            "name": v.get("name", ""),
            "chart_type": v.get("chart_type", "Bar Chart"),
            "x_var": v.get("x_var"),
            "y_var": v.get("y_var"),
            "position": v.get("position", 0),
            "dataset_id": v.get("dataset_id"),
            "pie_top_n": v.get("pie_top_n", CHART_MAX_PIE_SLICES),
            "sample_rows": v.get("sample_rows", CHART_SAMPLE_ROWS),
            "trim_pct": v.get("trim_pct", 0.0),
            "agg": v.get("agg", "sum"),
            "x_filter_min": v.get("x_filter_min"),
            "x_filter_max": v.get("x_filter_max"),
            "y_filter_min": v.get("y_filter_min"),
            "y_filter_max": v.get("y_filter_max"),
        }

    def ser_dash(d):
        return {
            "id": d["id"],
            "name": d.get("name", "Dashboard"),
            "dataset_id": d.get("dataset_id"),
            "cols_per_row": d.get("cols_per_row", 3),
            "next_viz_id": d.get("next_viz_id", 0),
            "visualizations": [ser_viz(v) for v in d.get("visualizations", [])],
        }

    return {
        "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "active_dataset_id": st.session_state.active_dataset_id,
        "active_dashboard_id": st.session_state.active_dashboard_id,
        "next_dataset_id": st.session_state.next_dataset_id,
        "next_dashboard_id": st.session_state.next_dashboard_id,
        "datasets": datasets_meta,
        "dashboards": [ser_dash(d) for d in st.session_state.dashboards],
    }

def import_config(cfg: dict) -> tuple:

    existing_by_name = {ds["name"]: did for did, ds in st.session_state.datasets.items()}
    old_to_new_id = {}
    new_placeholder_entries = {}
    placeholders_created = []
    rewired = []

    max_existing_id = max(st.session_state.datasets.keys(), default=-1)
    next_safe_id = max_existing_id + 1

    for old_id_str, meta in cfg.get("datasets", {}).items():
        old_id = int(old_id_str)
        ds_name = meta["name"]

        if ds_name in existing_by_name:
            new_id = existing_by_name[ds_name]
            old_to_new_id[old_id] = new_id
            rewired.append(ds_name)
        else:
            cols = meta.get("columns", [])
            placeholder_df = pd.DataFrame(columns=cols)
            for col, dtype_str in meta.get("dtypes", {}).items():
                if col in placeholder_df.columns:
                    try:
                        if "datetime" in dtype_str:
                            placeholder_df[col] = pd.to_datetime(placeholder_df[col], errors="coerce")
                        elif dtype_str == "category":
                            placeholder_df[col] = placeholder_df[col].astype("category")
                        else:
                            placeholder_df[col] = placeholder_df[col].astype(dtype_str)
                    except Exception:
                        pass

            new_id = next_safe_id
            next_safe_id += 1
            ds_entry = make_dataset(new_id, ds_name, placeholder_df)
            ds_entry["_placeholder"] = True
            ds_entry["_expected_rows"] = meta.get("row_count", 0)
            if meta.get("fb_source"):
                ds_entry["fb_source"] = meta["fb_source"]
            new_placeholder_entries[new_id] = ds_entry
            old_to_new_id[old_id] = new_id
            placeholders_created.append(ds_name)


    merged = dict(st.session_state.datasets)
    merged.update(new_placeholder_entries)
    st.session_state.datasets = merged
    st.session_state.next_dataset_id = max(merged.keys(), default=-1) + 1

    def remap(old_id):
        if old_id is None:
            return None
        return old_to_new_id.get(int(old_id), old_id)

    def load_viz(v):
        return {
            "id": v["id"],
            "name": v.get("name", f"Chart {v['id']+1}"),
            "chart_type": v.get("chart_type", "Bar Chart"),
            "x_var": v.get("x_var"),
            "y_var": v.get("y_var"),
            "position": v.get("position", 0),
            "dataset_id": remap(v.get("dataset_id")),
            "pie_top_n": v.get("pie_top_n", CHART_MAX_PIE_SLICES),
            "sample_rows": v.get("sample_rows", CHART_SAMPLE_ROWS),
            "trim_pct": float(v.get("trim_pct", 0.0)),
            "agg": v.get("agg", "sum"),
            "x_filter_min": v.get("x_filter_min"),
            "x_filter_max": v.get("x_filter_max"),
            "y_filter_min": v.get("y_filter_min"),
            "y_filter_max": v.get("y_filter_max"),
        }

    def load_dash(d):
        return {
            "id": d["id"],
            "name": d.get("name", "Dashboard"),
            "dataset_id": remap(d.get("dataset_id")),
            "cols_per_row": d.get("cols_per_row", 3),
            "next_viz_id": d.get("next_viz_id", 0),
            "visualizations": [load_viz(v) for v in d.get("visualizations", [])],
        }

    loaded_dashboards = [load_dash(d) for d in cfg.get("dashboards", [])]
    st.session_state.dashboards = loaded_dashboards or [make_dashboard(0, "Dashboard 1")]
    st.session_state.next_dashboard_id = cfg.get("next_dashboard_id",max(d["id"] for d in st.session_state.dashboards) + 1)

    raw_active_ds = cfg.get("active_dataset_id")
    st.session_state.active_dataset_id = (
        remap(raw_active_ds) if raw_active_ds is not None
        else next(iter(st.session_state.datasets), None)
    )
    st.session_state.active_dashboard_id = cfg.get(
        "active_dashboard_id", st.session_state.dashboards[0]["id"]
    )
    st.session_state.selected_viz_id = None


    n_dash = len(st.session_state.dashboards)
    n_viz = sum(len(d["visualizations"]) for d in st.session_state.dashboards)
    parts = [f"Restored **{n_dash}** dashboard(s) with **{n_viz}** chart(s)."]
    if rewired:
        parts.append(f"Auto-linked: {', '.join(f'`{n}`' for n in rewired)}.")
    if placeholders_created:
        parts.append(
            f"⚠️ **{len(placeholders_created)}** dataset(s) created as empty placeholders "
            f"({', '.join(f'`{n}`' for n in placeholders_created)}) — re-upload those files to restore charts."
        )
    return True, " ".join(parts)

def get_config_download_bytes() -> bytes:
    """
    Cache the serialised config bytes in session_state so the value passed to
    st.download_button is identical on every rerun unless the config actually
    changed.  Without this, export_config() embeds datetime.now() on every call,
    which produces a new bytes object → Streamlit registers a new media-file ID
    → the old URL is immediately invalidated → MediaFileStorageError.
    """
    cfg = export_config()
   
    cfg_no_ts = {k: v for k, v in cfg.items() if k != "exported_at"}
    fingerprint = json.dumps(cfg_no_ts, sort_keys=True, ensure_ascii=False)

    cached = st.session_state.get("_cfg_dl_cache")
    if cached and cached["fingerprint"] == fingerprint:
        return cached["bytes"]

    
    new_bytes = json.dumps(cfg, indent=2, ensure_ascii=False).encode("utf-8")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    st.session_state["_cfg_dl_cache"] = {
        "fingerprint": fingerprint,
        "bytes": new_bytes,
        "ts": ts,
    }
    return new_bytes

def render_config_save_load_ui(location: str = "sidebar"):

    compact = (location == "sidebar")


    has_content = bool(st.session_state.datasets or st.session_state.dashboards)
    if has_content:
        # Warm the cache and retrieve the stable timestamp frozen with the bytes
        get_config_download_bytes()
        ts = st.session_state["_cfg_dl_cache"]["ts"]
        n_dash = len(st.session_state.dashboards)
        n_ds = len(st.session_state.datasets)
        n_viz = sum(len(d["visualizations"]) for d in st.session_state.dashboards)

        if not compact:
            st.write("#### 💾 Export Configuration")
            st.caption(
                f"Saves **{n_dash}** dashboard(s), **{n_viz}** chart(s), and **{n_ds}** dataset "
                "schema(s) to a JSON file. Dataset *data* is not included — you will need to "
                "re-upload your files when loading this config in a fresh session."
            )

        st.download_button(
            label="⬇️ Download Config" if compact else "⬇️ Download Configuration File",
            data=get_config_download_bytes(),
            file_name=f"bc_config_{ts}.json",
            mime="application/json",
            key=f"cfg_dl_{location}",
            help=f"{n_dash} dashboard(s) · {n_viz} chart(s) · {n_ds} dataset(s)",
            width="stretch" if compact else "content",
        )
        if compact:
            st.caption(f"{n_dash} dash · {n_viz} charts · {n_ds} datasets")
    else:
        st.caption("Nothing to export yet.")

    if not compact:
        st.divider()
        st.write("#### 📂 Load Configuration")
        st.caption(
            "Upload a previously exported config file. Dashboards and chart settings will be "
            "restored immediately. Datasets matched by name are auto-linked; others become "
            "empty placeholders until you re-upload the source files."
        )

    cfg_file = st.file_uploader(
        "📂 Load Config" if compact else "Upload config file (.json)",
        type=["json"],
        key=f"cfg_up_{location}",
        label_visibility="collapsed" if compact else "visible",
    )

    if cfg_file is not None:
        try:
            cfg_data = json.loads(cfg_file.read().decode("utf-8"))
        except Exception as e:
            st.error(f"Could not parse config file: {e}")
            cfg_data = None

        if cfg_data:
            n_d = len(cfg_data.get("dashboards", []))
            n_v = sum(len(d.get("visualizations", [])) for d in cfg_data.get("dashboards", []))
            n_ds_cfg = len(cfg_data.get("datasets", {}))

            st.info(
                f"**{cfg_data.get('app','?')}** · v{cfg_data.get('version','?')} · "
                f"exported {cfg_data.get('exported_at','?')}\n\n"
                f"{n_d} dashboard(s) · {n_v} chart(s) · {n_ds_cfg} dataset(s)"
            )

            if not compact:
                merge_mode = st.checkbox(
                    "Keep existing datasets (merge)",
                    value=True,
                    key=f"cfg_merge_{location}",
                    help="Preserves currently loaded data and auto-links it to the imported config.",
                )
            else:
                merge_mode = True

            if st.button("✅ Apply Config", type="primary", key=f"cfg_apply_{location}",
                         width="stretch" if compact else "content"):
                if not merge_mode:
                    st.session_state.datasets = {}
                    st.session_state.active_dataset_id = None

                ok, msg = import_config(cfg_data)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)


def seconds_until_next_refresh():
    elapsed = time.time() - st.session_state.last_firebase_refresh
    return max(0.0, st.session_state.auto_refresh_interval - elapsed)

def do_firebase_refresh():
    fb_firestore_load_collection.clear()
    fb_rtdb_load_node.clear()
    fb_rtdb_list_nodes.clear()

    cfg = st.session_state.fb_config or {}
    id_token = st.session_state.fb_id_token
    proj_id = cfg.get("projectId", "")
    db_url = cfg.get("databaseURL", "")

    n_updated = 0
    errors = []
    rows_changed = {}

    for did, ds in list(st.session_state.datasets.items()):
        src = ds.get("fb_source")
        if not src:
            continue
        try:
            old_len = len(ds["df"])
            if src["type"] == "firestore":
                new_df = fb_firestore_load_collection(
                    proj_id, id_token, src["collection"], int(src.get("limit", 1000))
                )
            elif src["type"] == "rtdb":
                new_df = fb_rtdb_load_node(db_url, src["path"], id_token)
            else:
                continue

            if not new_df.empty:
                ds["df"] = new_df
                ds["data_dict"] = new_df.to_dict("list")
                ds["data_hash"] = get_data_hash(new_df)
                rows_changed[did] = len(new_df) - old_len
                n_updated += 1
        except Exception as exc:
            errors.append(f"{ds['name']}: {exc}")

    st.session_state.auto_refresh_rows_changed = rows_changed
    return n_updated, "; ".join(errors) if errors else ""

def clear_auto_refresh_state():
    st.session_state.auto_refresh_enabled = False
    st.session_state.auto_refresh_status = 'idle'
    st.session_state.auto_refresh_rows_changed = {}
    st.session_state.auto_refresh_last_error = ''
    for wk in ('ar_toggle', 'ar_interval_slider'):
        st.session_state.pop(wk, None)

def handle_auto_refresh():
    if not st.session_state.auto_refresh_enabled:
        return
    if not st.session_state.fb_id_token:
        clear_auto_refresh_state()
        return
    fb_linked = [ds for ds in st.session_state.datasets.values() if ds.get("fb_source")]
    if not fb_linked:
        return

    interval = st.session_state.auto_refresh_interval

    @st.fragment(run_every=interval)
    def firebase_poller():
        if seconds_until_next_refresh() > 5:
            return
        st.session_state.auto_refresh_status = 'refreshing'
        st.session_state.last_firebase_refresh = time.time()
        n, err = do_firebase_refresh()
        if err:
            st.session_state.auto_refresh_status = 'error'
            st.session_state.auto_refresh_last_error = err
        else:
            st.session_state.auto_refresh_status = 'ok'
            st.session_state.auto_refresh_last_error = ''
        st.rerun()

    firebase_poller()


def get_active_dataset_for_dashboard(dash):
    did = dash.get('dataset_id') if dash else None
    if did is None:
        did = st.session_state.active_dataset_id
    return st.session_state.datasets.get(did)

def get_active_dataset_for_viz(viz, dash):
    did = viz.get('dataset_id')
    if did is None:
        did = dash.get('dataset_id') if dash else None
    if did is None:
        did = st.session_state.active_dataset_id
    return st.session_state.datasets.get(did)

def add_dataset(name, df, fb_source=None):
    did = st.session_state.next_dataset_id
    ds = make_dataset(did, name, df)
    if fb_source:
        ds["fb_source"] = fb_source
    st.session_state.datasets[did] = ds
    st.session_state.next_dataset_id += 1
    if st.session_state.active_dataset_id is None:
        st.session_state.active_dataset_id = did
    return did

def delete_dataset(did):
    if did in st.session_state.datasets:
        del st.session_state.datasets[did]
    if st.session_state.active_dataset_id == did:
        rem = list(st.session_state.datasets.keys())
        st.session_state.active_dataset_id = rem[0] if rem else None
    for d in st.session_state.dashboards:
        if d.get('dataset_id') == did:
            d['dataset_id'] = None
        for viz in d['visualizations']:
            if viz.get('dataset_id') == did:
                viz['dataset_id'] = None

def rename_dataset(did, new_name):
    if did in st.session_state.datasets:
        st.session_state.datasets[did]['name'] = new_name.strip() or st.session_state.datasets[did]['name']

def update_dataset_df(did, df):
    ds = st.session_state.datasets.get(did)
    if ds:
        ds['df'] = df
        ds['data_dict'] = df.to_dict('list')
        ds['data_hash'] = get_data_hash(df)

def dataset_options():
    return [(did, ds['name']) for did, ds in st.session_state.datasets.items()]

def dataset_selectbox(label, key, current_did, include_inherit=True, inherit_label="Inherit"):
    opts_raw = dataset_options()
    opts = ([(-1, inherit_label)] + opts_raw) if include_inherit else opts_raw
    ids = [o[0] for o in opts]
    labels = [o[1] for o in opts]
    sel_id = current_did if current_did is not None else -1
    idx = ids.index(sel_id) if sel_id in ids else 0
    chosen = st.selectbox(label, labels, index=idx, key=key)
    chosen_id = ids[labels.index(chosen)]
    return None if chosen_id == -1 else chosen_id


def get_active_dashboard():
    target = st.session_state.active_dashboard_id
    for d in st.session_state.dashboards:
        if d['id'] == target:
            return d
    if st.session_state.dashboards:
        st.session_state.active_dashboard_id = st.session_state.dashboards[0]['id']
        return st.session_state.dashboards[0]
    return None

def add_dashboard():
    did = st.session_state.next_dashboard_id
    st.session_state.dashboards.append(make_dashboard(did, f'Dashboard {did + 1}', st.session_state.active_dataset_id))
    st.session_state.next_dashboard_id += 1
    st.session_state.active_dashboard_id = did

def delete_dashboard(did):
    if len(st.session_state.dashboards) <= 1:
        return
    st.session_state.dashboards = [d for d in st.session_state.dashboards if d['id'] != did]
    if st.session_state.active_dashboard_id == did:
        st.session_state.active_dashboard_id = st.session_state.dashboards[0]['id']

def rename_dashboard(did, new_name):
    for d in st.session_state.dashboards:
        if d['id'] == did:
            d['name'] = new_name.strip() or d['name']
            break
    st.session_state.renaming_dashboard_id = None

def add_visualization():
    dash = get_active_dashboard()
    if not dash:
        return
    vid = dash['next_viz_id']
    dash['visualizations'].append({
        'id': vid, 'chart_type': 'Bar Chart', 'x_var': None, 'y_var': None,
        'position': len(dash['visualizations']), 'name': f'Chart {vid + 1}',
        'dataset_id': None,
        'pie_top_n': CHART_MAX_PIE_SLICES,
        'sample_rows': CHART_SAMPLE_ROWS,
        'trim_pct': 0.0,
        'agg': 'sum',
        'x_filter_min': None,
        'x_filter_max': None,
        'y_filter_min': None,
        'y_filter_max': None,
    })
    dash['next_viz_id'] += 1
    st.session_state.selected_viz_id = vid

def delete_visualization(viz_id):
    dash = get_active_dashboard()
    if not dash:
        return
    dash['visualizations'] = [v for v in dash['visualizations'] if v['id'] != viz_id]
    for idx, v in enumerate(dash['visualizations']):
        v['position'] = idx
    if st.session_state.selected_viz_id == viz_id:
        vids = [v['id'] for v in dash['visualizations']]
        st.session_state.selected_viz_id = vids[0] if vids else None

def move_visualization(viz_id, direction):
    dash = get_active_dashboard()
    if not dash:
        return
    vl = dash['visualizations']
    try:
        ci = next(i for i, v in enumerate(vl) if v['id'] == viz_id)
        if direction == 'left' and ci > 0:
            vl[ci], vl[ci - 1] = vl[ci - 1], vl[ci]
        elif direction == 'right' and ci < len(vl) - 1:
            vl[ci], vl[ci + 1] = vl[ci + 1], vl[ci]
        for idx, v in enumerate(vl):
            v['position'] = idx
    except StopIteration:
        pass


@st.cache_data(show_spinner=False)
def detect_sep(first_line):
    best, best_n = ',', 0
    for s in [',', ';', '\t', '|']:
        n = first_line.count(s)
        if n > best_n:
            best_n, best = n, s
    return best

SEP_LABELS = {',': 'Comma (,)', ';': 'Semicolon (;)', '\t': 'Tab', '|': 'Pipe (|)', 'custom': 'Custom'}
CHART_TYPES = ["Bar Chart", "Line Chart", "Scatter Plot", "Histogram",
               "Box Plot", "Pie Chart", "Area Chart", "Violin Plot"]


def render_sidebar():
    st.markdown("""
        <style>
        [data-testid="stSidebar"] .stButton button {
            text-align: center;
            padding-left: 0 !important;
            padding-right: 0 !important;
        }
        </style>
    """, unsafe_allow_html=True)

    active_dash = get_active_dashboard()
    with st.sidebar:

        st.subheader("Navigation")
        nc = st.columns(2)
        with nc[0]:
            if st.button("Charts", width="stretch", key="nav_charts",
                         type="primary" if st.session_state.screen == 'config' else "secondary"):
                st.session_state.screen = 'config'
                st.rerun()
        with nc[1]:
            if st.button("Data", width="stretch", key="nav_data",
                         type="primary" if st.session_state.screen == 'data' else "secondary"):
                st.session_state.screen = 'data'
                st.rerun()

        if st.session_state.datasets:
            st.divider()
            st.subheader("Global Dataset")
            ds_opts = dataset_options()
            ds_labels = [o[1] for o in ds_opts]
            ds_ids = [o[0] for o in ds_opts]
            cur = st.session_state.active_dataset_id
            gi = ds_ids.index(cur) if cur in ds_ids else 0
            chosen = st.selectbox("Active dataset", ds_labels, index=gi, key="global_ds_select",
                                  label_visibility="collapsed")
            new_id = ds_ids[ds_labels.index(chosen)]
            if new_id != st.session_state.active_dataset_id:
                st.session_state.active_dataset_id = new_id
                st.rerun()

        if st.session_state.fb_id_token and st.session_state.screen in ('config', 'data'):
            st.divider()
            render_auto_refresh_sidebar()

        if st.session_state.screen == 'config':
            st.divider()
            st.subheader("Dashboards")

            for dash in st.session_state.dashboards:
                is_active = dash['id'] == st.session_state.active_dashboard_id
                is_renaming = st.session_state.renaming_dashboard_id == dash['id']

                if is_renaming:
                    new_name = st.text_input("New name", value=dash['name'],
                                             key=f"rename_input_{dash['id']}", label_visibility="collapsed")
                    rc1, rc2 = st.columns(2)
                    with rc1:
                        if st.button("Save", key=f"save_rename_{dash['id']}", width="stretch", type="primary"):
                            rename_dashboard(dash['id'], new_name)
                            st.rerun()
                    with rc2:
                        if st.button("Cancel", key=f"cancel_rename_{dash['id']}", width="stretch"):
                            st.session_state.renaming_dashboard_id = None
                            st.rerun()
                else:
                    bc = st.columns([5, 1, 1], vertical_alignment="center")
                    with bc[0]:
                        if st.button(f"{'▸ ' if is_active else ''}{dash['name']}",
                                     key=f"switch_{dash['id']}", width="stretch",
                                     type="primary" if is_active else "secondary"):
                            st.session_state.active_dashboard_id = dash['id']
                            st.session_state.selected_viz_id = None
                            st.rerun()
                    with bc[1]:
                        if st.button("✏️", key=f"rename_btn_{dash['id']}", width="stretch", help="Rename"):
                            st.session_state.renaming_dashboard_id = dash['id']
                            st.rerun()
                    with bc[2]:
                        if st.button("🗑️", key=f"del_dash_{dash['id']}", width="stretch", help="Delete",
                                     disabled=len(st.session_state.dashboards) <= 1):
                            delete_dashboard(dash['id'])
                            st.rerun()

                if is_active and len(st.session_state.datasets) > 1:
                    with st.expander("Dataset override", expanded=False):
                        ddr = get_active_dataset_for_dashboard(dash)
                        inh = f"Global · {ddr['name']}" if ddr else "Global"
                        new_did = dataset_selectbox("Dataset", f"dash_ds_{dash['id']}",
                                                    dash.get('dataset_id'), include_inherit=True, inherit_label=inh)
                        if new_did != dash.get('dataset_id'):
                            dash['dataset_id'] = new_did
                            st.rerun()

            if st.button("＋ New Dashboard", width="stretch", key="new_dash"):
                add_dashboard()
                st.session_state.selected_viz_id = None
                st.rerun()

            st.divider()
            st.subheader("Grid Layout")
            cpr = st.radio("Columns per row", [2, 3, 4], horizontal=True,
                           index=[2, 3, 4].index(active_dash['cols_per_row']) if active_dash else 1,
                           key=f"cpr_{st.session_state.active_dashboard_id}")
            if active_dash:
                active_dash['cols_per_row'] = cpr


        st.divider()
        st.subheader("Save / Load")
        with st.expander("Configuration", expanded=False):
            render_config_save_load_ui(location="sidebar")

        st.divider()
        if st.button("← Upload / Manage Files", width="stretch", key="back_upload_btn"):
            st.session_state.screen = 'upload'
            st.rerun()

    return get_active_dashboard()

def render_auto_refresh_sidebar():
    st.subheader("🔄 Auto-Refresh")

    enabled = st.toggle(
        "Enable live refresh",
        value=st.session_state.auto_refresh_enabled,
        key="ar_toggle",
        help="Periodically pull fresh data from Firebase and update all charts.",
    )
    if enabled != st.session_state.auto_refresh_enabled:
        st.session_state.auto_refresh_enabled = enabled
        if enabled:
            st.session_state.last_firebase_refresh = time.time()
            st.session_state.auto_refresh_status = 'idle'
        st.rerun()

    INTERVAL_OPTIONS = {"10 s": 10, "30 s": 30, "1 min": 60, "2 min": 120, "5 min": 300, "10 min": 600}

    labels = list(INTERVAL_OPTIONS.keys())
    values = list(INTERVAL_OPTIONS.values())
    cur_val = st.session_state.auto_refresh_interval
    cur_idx = values.index(cur_val) if cur_val in values else 1

    chosen_label = st.select_slider(
        "Interval", options=labels, value=labels[cur_idx],
        key="ar_interval_slider", disabled=not enabled,
    )
    new_interval = INTERVAL_OPTIONS[chosen_label]
    if new_interval != st.session_state.auto_refresh_interval:
        st.session_state.auto_refresh_interval = new_interval
        st.session_state.last_firebase_refresh = time.time()

    if enabled:
        status = st.session_state.auto_refresh_status
        if status == 'refreshing':
            st.caption("⏳ Refreshing…")
        elif status == 'error':
            st.caption("⚠️ Last refresh had errors")
            with st.expander("Error details", expanded=False):
                st.error(st.session_state.auto_refresh_last_error)
        elif status == 'ok':
            deltas = st.session_state.auto_refresh_rows_changed
            if deltas:
                parts = []
                for did, delta in deltas.items():
                    name = st.session_state.datasets.get(did, {}).get("name", f"#{did}")
                    sign = "+" if delta >= 0 else ""
                    parts.append(f"{name}: {sign}{delta} rows")
                st.caption("✅ Updated · " + ", ".join(parts))
            else:
                st.caption("✅ Live · no changes on last poll")
        else:
            st.caption("⏸ Waiting for first refresh…")

        interval = st.session_state.auto_refresh_interval
        mins, secs = divmod(interval, 60)
        st.caption(f"Polling every {f'{mins}m {secs:02d}s' if mins else f'{secs}s'}")

        if st.button("↺ Refresh Now", key="ar_refresh_now", width="stretch"):
            st.session_state.last_firebase_refresh = 0.0
            st.rerun()

    fb_linked = [(did, ds["name"]) for did, ds in st.session_state.datasets.items()
                 if ds.get("fb_source")]
    if fb_linked:
        with st.expander(f"Tracked datasets ({len(fb_linked)})", expanded=False):
            for did, name in fb_linked:
                src = st.session_state.datasets[did].get("fb_source", {})
                src_desc = src.get("collection") or src.get("path") or "?"
                st.caption(f"📁 **{name}** ← `{src_desc}`")
    elif st.session_state.datasets:
        st.caption("ℹ️ No Firebase-linked datasets.")


if st.session_state.screen == 'upload':
    st.caption("Load datasets from CSV or JSON files, connect to Firebase, or restore a saved configuration.")


    placeholders = [(did, ds) for did, ds in st.session_state.datasets.items()
                    if ds.get("_placeholder")]
    if placeholders:
        with st.container(border=True):
            st.warning(
                f"**{len(placeholders)} dataset(s) are empty placeholders** from a loaded config. "
                "Upload the matching files in the CSV/JSON tab to restore chart data."
            )
            for did, ds in placeholders:
                exp_rows = ds.get("_expected_rows", "?")
                cols_preview = ", ".join(f"`{c}`" for c in ds["df"].columns[:6])
                if len(ds["df"].columns) > 6:
                    cols_preview += f" … (+{len(ds['df'].columns)-6} more)"
                st.caption(f"**{ds['name']}** — expected ~{exp_rows:,} rows · columns: {cols_preview}")

    if st.session_state.datasets:
        st.subheader("Loaded Datasets")
        header = st.columns([4, 1, 1, 1])
        header[0].write("**Name**")
        header[1].write("**Rows**")
        header[2].write("**Cols**")
        header[3].write("**Remove**")

        for did, ds in list(st.session_state.datasets.items()):
            is_placeholder = ds.get("_placeholder", False)
            row = st.columns([4, 1, 1, 1])
            with row[0]:
                new_name = st.text_input(
                    "Name", value=ds['name'], key=f"ds_name_{did}",
                    label_visibility="collapsed",
                    help="Placeholder — upload matching file to populate" if is_placeholder else None,
                )
                if new_name != ds['name']:
                    rename_dataset(did, new_name)
            with row[1]:
                val = f"0 / ~{ds.get('_expected_rows','?')}" if is_placeholder else f"{len(ds['df']):,}"
                st.write(val)
            with row[2]:
                st.write(len(ds['df'].columns))
            with row[3]:
                if st.button("✕", key=f"del_ds_{did}"):
                    delete_dataset(did)
                    if not st.session_state.datasets:
                        st.session_state.screen = 'upload'
                        st.rerun()
        st.divider()

    tab_csv, tab_firebase, tab_config = st.tabs([
        "📄 CSV / JSON Upload", "🔥 Firebase", "💾 Configuration"
    ])

    with tab_csv:
        st.write("#### Upload CSV or JSON Files")
        uploaded_files = st.file_uploader(
            "Choose file(s)", type=['csv', 'json'], accept_multiple_files=True, key="csv_uploader"
        )

        @st.cache_data(show_spinner=False)
        def parse_file_bytes(file_bytes, file_name):
            raw = file_bytes.decode('utf-8')
            if file_name.lower().endswith('.json'):
                return raw, 'json', None
            return raw, 'csv', detect_sep(raw.split('\n')[0])

        def load_json_df(raw, orient):
            parsed = json.loads(raw)
            if orient == "auto":
                if isinstance(parsed, list):
                    return pd.json_normalize(parsed)
                if isinstance(parsed, dict):
                    try:
                        return pd.DataFrame(parsed)
                    except Exception:
                        return pd.json_normalize(parsed)
            return pd.read_json(io.StringIO(raw), orient=orient if orient != "auto" else None)

        pending = []
        for uf in (uploaded_files or []):
            raw, ftype, extra = parse_file_bytes(uf.read(), uf.name)
            pending.append((uf.name, raw, ftype, extra))

        if pending:
            current_names = {fname for fname, *_ in pending}
            if current_names != st.session_state.pending_file_names:
                st.session_state.confirmed_imports = set()
                st.session_state.pending_file_names = current_names

            st.write(f"**{len(pending)} file{'s' if len(pending) > 1 else ''} ready to configure**")

            for idx, (fname, raw, ftype, extra) in enumerate(pending):
                confirmed = idx in st.session_state.confirmed_imports
                icon = '✅' if confirmed else ('📋' if ftype == 'json' else '📄')
                clean_name = fname.rsplit('.', 1)[0]


                matching_placeholder = next(
                    (did for did, ds in st.session_state.datasets.items()
                     if ds.get("_placeholder") and ds["name"] == clean_name),
                    None
                )

                expander_label = f"{icon}  {fname}"
                if matching_placeholder is not None:
                    expander_label += "matches placeholder"

                with st.expander(expander_label, expanded=not confirmed):
                    if confirmed:
                        st.success("Imported successfully.")
                        continue

                    if matching_placeholder is not None:
                        st.info(
                            f"This file matches placeholder **'{clean_name}'** from your loaded config. "
                            "Importing will populate it automatically."
                        )

                    if ftype == 'csv':
                        r1c1, r1c2, r1c3 = st.columns([3, 2, 2])
                        with r1c1:
                            ds_name = st.text_input("Dataset name", value=clean_name, key=f"dsname_{idx}")
                        with r1c2:
                            sep_opt = st.selectbox("Delimiter", list(SEP_LABELS.keys()),
                                                   format_func=lambda x: SEP_LABELS[x],
                                                   index=list(SEP_LABELS.keys()).index(extra),
                                                   key=f"sep_{idx}")
                        with r1c3:
                            if sep_opt == 'custom':
                                sep_val = st.text_input("Custom character", value=",", max_chars=5, key=f"csep_{idx}")
                            else:
                                sep_val = sep_opt
                                st.info(f"Auto-detected: {SEP_LABELS[extra]}")
                        try:
                            preview_df = pd.read_csv(io.StringIO(raw), sep=sep_val)
                        except Exception as e:
                            st.error(f"Parse error: {e}")
                            continue
                    else:
                        JSON_ORIENTS = {
                            "auto":    "Auto-detect (recommended)",
                            "records": "Records  — [{col: val}, …]",
                            "columns": "Columns  — {col: {idx: val}}",
                            "index":   "Index    — {idx: {col: val}}",
                            "values":  "Values   — [[val, …]]",
                            "split":   "Split    — {index, columns, data}",
                        }
                        j1, j2 = st.columns([3, 2])
                        with j1:
                            ds_name = st.text_input("Dataset name", value=clean_name, key=f"dsname_{idx}")
                        with j2:
                            orient = st.selectbox(
                                "JSON structure", list(JSON_ORIENTS.keys()),
                                format_func=lambda x: JSON_ORIENTS[x],
                                key=f"json_orient_{idx}",
                            )
                        try:
                            preview_df = load_json_df(raw, orient)
                        except Exception as e:
                            st.error(f"JSON parse error: {e}. Try a different structure option.")
                            continue
                        if preview_df.empty:
                            st.warning("Parsed to an empty table — try a different structure.")
                            continue

                    st.write(f"**Preview** — {len(preview_df):,} rows × {len(preview_df.columns)} columns")
                    prev_r1, prev_r2 = st.columns([1, 3])
                    with prev_r1:
                        prev_mode = st.radio("Show", ["First 10 rows", "Last 10 rows"], key=f"prev_{idx}")
                    with prev_r2:
                        rows_to_show = preview_df.head(10) if prev_mode == "First 10 rows" else preview_df.tail(10)
                        st.dataframe(rows_to_show, width="stretch", height=220)

                    with st.expander("Set column types (optional)"):
                        dtype_opts = ["string", "int64", "float64", "datetime64", "bool", "category"]
                        dtype_changes = {}
                        dtc = st.columns(min(len(preview_df.columns), 5))
                        for ci, col in enumerate(preview_df.columns):
                            with dtc[ci % 5]:
                                cur = str(preview_df[col].dtype)
                                nd = st.selectbox(col, dtype_opts,
                                                  index=dtype_opts.index(cur) if cur in dtype_opts else 0,
                                                  key=f"dtype_{idx}_{col}")
                                if nd != cur:
                                    dtype_changes[col] = nd

                    _, btn_col = st.columns([3, 1])
                    with btn_col:
                        if st.button(f"Import '{ds_name}'", key=f"import_{idx}", type="primary", width="stretch"):
                            if ftype == 'csv':
                                df_final = pd.read_csv(io.StringIO(raw), sep=sep_val)
                            else:
                                df_final = load_json_df(raw, orient)
                            for col, nd in dtype_changes.items():
                                try:
                                    if nd == "datetime64":
                                        df_final[col] = pd.to_datetime(df_final[col], errors='coerce')
                                    elif nd == "bool":
                                        df_final[col] = df_final[col].astype(bool)
                                    elif nd == "category":
                                        df_final[col] = df_final[col].astype('category')
                                    else:
                                        df_final[col] = df_final[col].astype(nd)
                                except Exception:
                                    pass


                            if matching_placeholder is not None and ds_name == clean_name:
                                ph = st.session_state.datasets[matching_placeholder]
                                ph['df']        = df_final
                                ph['data_dict'] = df_final.to_dict('list')
                                ph['data_hash'] = get_data_hash(df_final)
                                ph.pop('_placeholder', None)
                                ph.pop('_expected_rows', None)
                                if st.session_state.active_dataset_id is None:
                                    st.session_state.active_dataset_id = matching_placeholder
                            else:
                                add_dataset(ds_name, df_final)

                            st.session_state.confirmed_imports.add(idx)
                            st.rerun()
        else:
            st.session_state.confirmed_imports = set()
            st.session_state.pending_file_names = set()

    with tab_firebase:
        st.write("#### Connect to Firebase")
        st.caption("Uses the Firebase Client SDK with your own credentials.")

        if st.session_state.fb_id_token:
            cfg = st.session_state.fb_config or {}
            method = st.session_state.fb_auth_method
            uid = st.session_state.fb_uid or "?"
            label = "anonymous" if method == "anonymous" else st.session_state.get("fb_user_email", "user")
            st.success(f"Connected to **{cfg.get('projectId', '?')}** as **{label}** (uid: `{uid}`)")
            col_rf, col_dc = st.columns([1, 1])
            with col_rf:
                if st.button("🔄 Refresh token", key="fb_refresh_btn"):
                    new_tok, err = fb_refresh_token(cfg["apiKey"], st.session_state.fb_refresh_token)
                    if err:
                        st.error(f"Refresh failed: {err}")
                    else:
                        st.session_state.fb_id_token = new_tok
                        st.success("Token refreshed.")
                        st.rerun()
            with col_dc:
                if st.button("Disconnect", key="fb_disconnect"):
                    for k in ("fb_config","fb_id_token","fb_refresh_token","fb_uid",
                              "fb_auth_method","fb_user_email","fb_fs_collections",
                              "fb_fs_preview","fb_rtdb_nodes","fb_rtdb_preview"):
                        st.session_state.pop(k, None)
                    st.session_state.fb_config = None
                    st.session_state.fb_id_token = None
                    st.session_state.fb_refresh_token = None
                    st.session_state.fb_uid = None
                    clear_auto_refresh_state()
                    st.rerun()
            st.divider()

        else:
            with st.expander("Firebase Credentials", expanded=True):
                st.caption(
                    "Get your **Web API Key** and **Project ID** from Firebase Console → "
                    "Project Settings → General."
                )
                fc1, fc2 = st.columns(2)
                with fc1:
                    fb_api_key = st.text_input("Web API Key", key="fb_api_key_input",
                                               placeholder="Web API Key", type="password")
                    fb_project_id = st.text_input("Project ID", key="fb_project_id_input",
                                                   placeholder="my-project-id")
                    fb_db_url = st.text_input("Realtime Database URL (optional)", key="fb_db_url_input",
                                              placeholder="https://my-project-id-default-rtdb.firebaseio.com")
                with fc2:
                    st.write("**Sign-in method**")
                    auth_method = st.radio("Auth method", ["Email / Password", "Anonymous"],
                                           key="fb_auth_method_radio", label_visibility="collapsed")
                    if auth_method == "Email / Password":
                        fb_email = st.text_input("Email", key="fb_email_input", placeholder="you@example.com")
                        fb_password = st.text_input("Password", key="fb_password_input", type="password")
                    else:
                        fb_email, fb_password = None, None
                        st.info("Anonymous sign-in creates a temporary guest session.")

                _, btn_col = st.columns([3, 1])
                with btn_col:
                    if st.button("Connect", type="primary", width="stretch", key="fb_connect_btn"):
                        if not fb_api_key or not fb_project_id:
                            st.error("API Key and Project ID are required.")
                        else:
                            with st.spinner("Authenticating…"):
                                if auth_method == "Email / Password":
                                    tok, ref, uid, err = fb_sign_in_email(fb_api_key, fb_email, fb_password)
                                    method_key = "email"
                                else:
                                    tok, ref, uid, err = fb_sign_in_anonymous(fb_api_key)
                                    method_key = "anonymous"
                            if err:
                                st.error(f"Authentication failed: {err}")
                            else:
                                st.session_state.fb_config = {
                                    "apiKey": fb_api_key,
                                    "projectId": fb_project_id,
                                    "databaseURL": fb_db_url.strip() if fb_db_url else "",
                                }
                                st.session_state.fb_id_token = tok
                                st.session_state.fb_refresh_token = ref
                                st.session_state.fb_uid = uid
                                st.session_state.fb_auth_method = method_key
                                if fb_email:
                                    st.session_state.fb_user_email = fb_email
                                st.rerun()

        if st.session_state.fb_id_token:
            fb_cfg = st.session_state.fb_config
            id_token = st.session_state.fb_id_token
            proj_id = fb_cfg["projectId"]
            db_url = fb_cfg.get("databaseURL", "")

            db_type = st.radio("Database type", ["Firestore", "Realtime Database"],
                               horizontal=True, key="fb_db_type")

            if db_type == "Firestore":
                st.write("#### Firestore Collections")
                if st.button("🔄 Auto-detect collections", key="fb_fs_refresh"):
                    st.session_state.pop("fb_fs_collections", None)
                    st.session_state.pop("fb_fs_col_warning", None)
                    st.rerun()

                if "fb_fs_collections" not in st.session_state:
                    with st.spinner("Fetching collections…"):
                        cols, warn = fb_firestore_list_collections(proj_id, id_token)
                        st.session_state.fb_fs_collections = cols
                        st.session_state.fb_fs_col_warning = warn

                collections = st.session_state.get("fb_fs_collections", [])
                col_warning = st.session_state.get("fb_fs_col_warning")

                if col_warning:
                    st.warning(col_warning)
                elif collections:
                    st.success(f"Found {len(collections)} collection(s): " +
                               ", ".join(f"`{c}`" for c in collections))

                fs_col1, fs_col2 = st.columns([2, 1])
                with fs_col1:
                    if collections:
                        dropdown_choice = st.selectbox("Collection", collections, key="fb_fs_dropdown")
                        manual_name = st.text_input("Or type a collection name", value="",
                                                    key="fb_fs_manual", placeholder="Leave blank to use dropdown")
                        chosen_col = manual_name.strip() if manual_name.strip() else dropdown_choice
                    else:
                        chosen_col = st.text_input("Collection name", key="fb_fs_manual_only",
                                                   placeholder="e.g. users").strip()
                with fs_col2:
                    fs_limit = st.number_input("Row limit", min_value=1, max_value=10000,
                                                value=1000, step=100, key="fb_fs_limit")

                if chosen_col:
                    if st.button(f"Preview  `{chosen_col}`", key="fb_fs_preview_btn"):
                        with st.spinner(f"Loading '{chosen_col}'…"):
                            try:
                                preview = fb_firestore_load_collection(proj_id, id_token,
                                                                        chosen_col, int(fs_limit))
                                st.session_state["fb_fs_preview"] = (chosen_col, preview)
                            except Exception as e:
                                st.error(f"Failed to load collection: {e}")
                else:
                    st.info("Enter a collection name above to preview it.")

                if "fb_fs_preview" in st.session_state:
                    p_col, p_df = st.session_state["fb_fs_preview"]
                    if p_df.empty:
                        st.warning(f"Collection `{p_col}` is empty or your Security Rules block access.")
                    else:
                        st.write(f"**Preview: `{p_col}`** — {len(p_df):,} rows × {len(p_df.columns)} columns")
                        st.dataframe(p_df.head(10), width="stretch", height=220)
                        imp1, imp2 = st.columns([3, 1])
                        with imp1:
                            fs_ds_name = st.text_input("Dataset name", value=p_col, key="fb_fs_ds_name")
                        with imp2:
                            st.write("")
                            if st.button("Import dataset", type="primary", width="stretch", key="fb_fs_import"):
                                add_dataset(fs_ds_name, p_df, fb_source={
                                    "type": "firestore", "collection": chosen_col, "limit": int(fs_limit),
                                })
                                st.session_state.pop("fb_fs_preview", None)
                                st.success(f"Imported '{fs_ds_name}' ({len(p_df):,} rows).")
                                st.rerun()
            else:
                if not db_url:
                    st.warning("No **Realtime Database URL** was provided. Disconnect and reconnect with the URL.")
                else:
                    st.write("#### Realtime Database Browser")
                    rtdb_path = st.text_input("Path", value="/", key="fb_rtdb_path")
                    btn1, btn2 = st.columns(2)
                    with btn1:
                        if st.button("🔄 List child nodes", key="fb_rtdb_list"):
                            with st.spinner("Fetching nodes…"):
                                try:
                                    nodes = fb_rtdb_list_nodes(db_url, rtdb_path, id_token)
                                    st.session_state["fb_rtdb_nodes"] = nodes
                                except Exception as e:
                                    st.error(f"Failed: {e}")
                    with btn2:
                        if st.button("Preview path as dataset", key="fb_rtdb_preview_btn"):
                            with st.spinner(f"Loading `{rtdb_path}`…"):
                                try:
                                    preview = fb_rtdb_load_node(db_url, rtdb_path, id_token)
                                    st.session_state["fb_rtdb_preview"] = (rtdb_path, preview)
                                except Exception as e:
                                    st.error(f"Failed to load: {e}")

                    nodes = st.session_state.get("fb_rtdb_nodes", [])
                    if nodes:
                        st.caption("Child nodes at `{}`: {}{}".format(
                            rtdb_path,
                            ",  ".join(f"`{n}`" for n in nodes[:20]),
                            "  …" if len(nodes) > 20 else ""
                        ))

                    if "fb_rtdb_preview" in st.session_state:
                        p_path, p_df = st.session_state["fb_rtdb_preview"]
                        st.write(f"**Preview: `{p_path}`** — {len(p_df):,} rows × {len(p_df.columns)} columns")
                        st.dataframe(p_df.head(10), width="stretch", height=220)
                        imp1, imp2 = st.columns([3, 1])
                        with imp1:
                            rtdb_ds_name = st.text_input(
                                "Dataset name",
                                value=p_path.strip("/").replace("/", "_") or "rtdb_data",
                                key="fb_rtdb_ds_name",
                            )
                        with imp2:
                            st.write("")
                            if st.button("Import dataset", type="primary", width="stretch", key="fb_rtdb_import"):
                                add_dataset(rtdb_ds_name, p_df, fb_source={"type": "rtdb", "path": rtdb_path})
                                st.session_state.pop("fb_rtdb_preview", None)
                                st.success(f"Imported '{rtdb_ds_name}' ({len(p_df):,} rows).")
                                st.rerun()


    with tab_config:
        st.write("#### 💾 Configuration Save / Load")
        st.caption(
            "Export your dashboard layout and chart settings as a portable JSON file, "
            "then load it in any future session to restore your workspace."
        )

        left_col, right_col = st.columns([3, 2])
        with left_col:
            render_config_save_load_ui(location="page")
        with right_col:
            st.write("##### How it works")
            st.markdown("""
**What gets saved:**
- All dashboard names and layouts
- Every chart's type, axis settings, display options, and grid position
- Dataset names, column schemas, and Firebase source paths
- Grid layout preferences

**What is NOT saved:**
- The actual data rows from your CSV/JSON files
- Firebase credentials or auth tokens

**Restoring a session:**
1. Load your `.json` config file in the **Load Configuration** section
2. Re-upload your data files in the CSV/JSON tab
3. For Firebase datasets, reconnect in the Firebase tab and re-import """)

    if st.session_state.datasets:
        st.divider()
        _, cta_col, _ = st.columns([2, 3, 2])
        with cta_col:
            if st.button("Continue to Visualizations →", type="primary", width="stretch"):
                st.session_state.screen = 'config'
                st.rerun()


elif st.session_state.screen == 'data':
    if not st.session_state.datasets:
        st.warning("No data loaded.")
        if st.button("← Back to Upload"):
            st.session_state.screen = 'upload'
            st.rerun()
    else:
        render_sidebar()
        handle_auto_refresh()

        top_l, top_r = st.columns([3, 3])
        with top_l:
            st.subheader("Dataset Editor")
        with top_r:
            ds_opts = dataset_options()
            ds_labels = [o[1] for o in ds_opts]
            ds_ids = [o[0] for o in ds_opts]
            if 'data_editor_dataset_id' not in st.session_state:
                st.session_state.data_editor_dataset_id = st.session_state.active_dataset_id
            cur_edit_id = st.session_state.data_editor_dataset_id
            if cur_edit_id not in ds_ids:
                cur_edit_id = ds_ids[0]
            _, sel_col = st.columns([1, 3])
            with sel_col:
                chosen_label = st.selectbox("Editing dataset", ds_labels,
                                            index=ds_ids.index(cur_edit_id), key="data_editor_ds_pick")
                edit_id = ds_ids[ds_labels.index(chosen_label)]
                if edit_id != st.session_state.data_editor_dataset_id:
                    st.session_state.data_editor_dataset_id = edit_id
                    st.rerun()

        ds = st.session_state.datasets[edit_id]
        df = ds['df']

        if ds.get("_placeholder"):
            st.warning(f"**'{ds['name']}'** is a placeholder from a loaded config — no data yet. ""Go to **← Upload / Manage Files** → CSV/JSON tab to upload the matching file.")

        all_cols, numeric_cols = get_column_info(ds['data_hash'], df)

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Rows", f"{len(df):,}")
        m2.metric("Columns", len(df.columns))
        describe, miss_df, cat_cols = compute_stats(ds['data_hash'], df)
        m3.metric("Missing Values", int(miss_df["Missing"].sum()))
        m4.metric("Duplicate Rows", int(df.duplicated().sum()))
        m5.metric("Memory", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")

        st.write("")

        tab_view, tab_clean, tab_types, tab_rename, tab_filter, tab_stats = st.tabs([ "View & Edit", "Clean", "Column Types", "Rename Columns", "Filter & Sort", "Statistics"])

        with tab_view:
            st.caption("Click any cell to edit. Use the controls below the table to add or remove rows.")
            edited_df = st.data_editor(df, width="stretch", num_rows="dynamic", key="data_editor_main")
            st.write("")
            ac1, ac2, ac3 = st.columns([2, 2, 3])
            with ac1:
                if st.button("💾 Save Edits", type="primary", width="stretch"):
                    update_dataset_df(edit_id, edited_df.reset_index(drop=True))
                    st.success("Saved — charts will use updated data.")
                    st.rerun()
            with ac2:
                if st.button("Discard Changes", width="stretch"):
                    st.rerun()
            with ac3:
                st.download_button("⬇️ Download as CSV", data=df.to_csv(index=False).encode(),
                                   file_name=f"{ds['name']}.csv", mime="text/csv", width="stretch")

        with tab_clean:
            st.write("#### Remove Empty Rows")
            c1, c2 = st.columns([3, 1])
            with c1:
                how_empty = st.radio("Remove a row if:", ["Any cell is empty", "All cells are empty"],
                                     horizontal=True, key="empty_strat")
            with c2:
                st.write("")
                if st.button("Remove Empty Rows", width="stretch", key="rm_empty"):
                    before = len(df)
                    df_new = df.dropna(how='any' if "Any" in how_empty else 'all')
                    update_dataset_df(edit_id, df_new)
                    st.success(f"Removed {before - len(df_new)} row(s).")
                    st.rerun()

            st.write("#### Remove Duplicate Rows")
            c1, c2 = st.columns([3, 1])
            with c1:
                dup_cols = st.multiselect("Check duplicates using these columns (blank = all):",
                                          all_cols, key="dup_cols")
            with c2:
                st.write("")
                if st.button("Remove Duplicates", width="stretch", key="rm_dups"):
                    before = len(df)
                    df_new = df.drop_duplicates(subset=dup_cols if dup_cols else None)
                    update_dataset_df(edit_id, df_new)
                    st.success(f"Removed {before - len(df_new)} duplicate(s).")
                    st.rerun()

            st.write("#### Fill Missing Values")
            c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
            with c1:
                fill_col = st.selectbox("Column", ["— all columns —"] + all_cols, key="fill_col")
            with c2:
                fill_method = st.selectbox("Method", ["Mean", "Median", "Mode",
                                                       "Forward fill", "Backward fill", "Custom value"], key="fill_method")
            with c3:
                custom_val = st.text_input("Custom value", key="fill_custom") if fill_method == "Custom value" else ""
            with c4:
                st.write("")
                if st.button("Fill", width="stretch", key="do_fill"):
                    cols_f = all_cols if fill_col == "— all columns —" else [fill_col]
                    df_new = df.copy()
                    try:
                        for c in cols_f:
                            if fill_method == "Mean":            df_new[c] = df_new[c].fillna(df_new[c].mean())
                            elif fill_method == "Median":        df_new[c] = df_new[c].fillna(df_new[c].median())
                            elif fill_method == "Mode":          df_new[c] = df_new[c].fillna(df_new[c].mode()[0])
                            elif fill_method == "Forward fill":  df_new[c] = df_new[c].ffill()
                            elif fill_method == "Backward fill": df_new[c] = df_new[c].bfill()
                            elif fill_method == "Custom value":  df_new[c] = df_new[c].fillna(custom_val)
                        update_dataset_df(edit_id, df_new)
                        st.success("Missing values filled.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

            st.write("#### Drop Columns")
            c1, c2 = st.columns([3, 1])
            with c1:
                drop_cols = st.multiselect("Select columns to drop:", all_cols, key="drop_cols")
            with c2:
                st.write("")
                if st.button("Drop Selected", width="stretch", key="do_drop", disabled=not drop_cols):
                    update_dataset_df(edit_id, df.drop(columns=drop_cols))
                    st.success(f"Dropped {len(drop_cols)} column(s).")
                    st.rerun()

        with tab_types:
            st.caption("Change how each column is interpreted. Click Apply when done.")
            dtype_options = ["string", "int64", "float64", "datetime64", "bool", "category"]
            dtype_changes = {}
            ncols = min(len(df.columns), 4)
            dtc = st.columns(ncols)
            for i, col_name in enumerate(all_cols):
                with dtc[i % ncols]:
                    cur = str(df[col_name].dtype)
                    nd = st.selectbox(col_name, dtype_options,
                                      index=dtype_options.index(cur) if cur in dtype_options else 0,
                                      key=f"dt_editor_{col_name}")
                    if nd != cur:
                        dtype_changes[col_name] = nd
            st.write("")
            if st.button("Apply Type Changes", type="primary" if dtype_changes else "secondary",
                         disabled=not dtype_changes, key="apply_types"):
                df_new = df.copy()
                try:
                    for col_name, nd in dtype_changes.items():
                        if nd == "datetime64": df_new[col_name] = pd.to_datetime(df_new[col_name], errors='coerce')
                        elif nd == "bool":     df_new[col_name] = df_new[col_name].astype(bool)
                        elif nd == "category": df_new[col_name] = df_new[col_name].astype('category')
                        else:                  df_new[col_name] = df_new[col_name].astype(nd)
                    update_dataset_df(edit_id, df_new)
                    st.success("Types updated.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

        with tab_rename:
            st.caption("Edit any column name below, then click Apply.")
            rename_map = {}
            ncols = min(len(df.columns), 4)
            rnc = st.columns(ncols)
            for i, col in enumerate(all_cols):
                with rnc[i % ncols]:
                    new_col = st.text_input(col, value=col, key=f"rename_{col}")
                    if new_col != col:
                        rename_map[col] = new_col
            st.write("")
            if st.button("Apply Renames", type="primary" if rename_map else "secondary",
                         disabled=not rename_map, key="apply_renames"):
                update_dataset_df(edit_id, df.rename(columns=rename_map))
                st.success("Columns renamed.")
                st.rerun()

        with tab_filter:
            st.caption("Filter and sort the view. View-only — use View & Edit to save changes.")
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                filter_col = st.selectbox("Filter column", ["— none —"] + all_cols, key="filter_col")
            with fc2:
                filter_op = st.selectbox("Condition", ["contains", "equals", ">", "<", ">=", "<=",
                                                        "is empty", "is not empty"], key="filter_op")
            with fc3:
                filter_val = st.text_input("Value", key="filter_val") if filter_op not in ("is empty", "is not empty") else ""
            sc1, sc2 = st.columns(2)
            with sc1:
                sort_col = st.selectbox("Sort by", ["— none —"] + all_cols, key="sort_col")
            with sc2:
                sort_asc = st.radio("Order", ["Ascending", "Descending"], horizontal=True, key="sort_asc")

            if filter_col != "— none —" or sort_col != "— none —":
                view_df = df.copy()
                if filter_col != "— none —":
                    try:
                        if filter_op == "contains":       view_df = view_df[view_df[filter_col].astype(str).str.contains(filter_val, na=False)]
                        elif filter_op == "equals":       view_df = view_df[view_df[filter_col].astype(str) == filter_val]
                        elif filter_op == ">":            view_df = view_df[pd.to_numeric(view_df[filter_col], errors='coerce') > float(filter_val)]
                        elif filter_op == "<":            view_df = view_df[pd.to_numeric(view_df[filter_col], errors='coerce') < float(filter_val)]
                        elif filter_op == ">=":           view_df = view_df[pd.to_numeric(view_df[filter_col], errors='coerce') >= float(filter_val)]
                        elif filter_op == "<=":           view_df = view_df[pd.to_numeric(view_df[filter_col], errors='coerce') <= float(filter_val)]
                        elif filter_op == "is empty":     view_df = view_df[view_df[filter_col].isnull() | (view_df[filter_col].astype(str).str.strip() == "")]
                        elif filter_op == "is not empty": view_df = view_df[view_df[filter_col].notnull() & (view_df[filter_col].astype(str).str.strip() != "")]
                    except Exception:
                        st.warning("Could not apply filter.")
                if sort_col != "— none —":
                    try:
                        view_df = view_df.sort_values(sort_col, ascending=(sort_asc == "Ascending"))
                    except Exception:
                        pass
            else:
                view_df = df

            st.caption(f"Showing {len(view_df):,} of {len(df):,} rows")
            st.dataframe(view_df, width="stretch")

        with tab_stats:
            if not describe.empty:
                st.write("#### Numeric Summary")
                st.dataframe(describe.style.format("{:.3f}"), width="stretch")
            else:
                st.info("No numeric columns to summarize.")
            st.write("#### Missing Values per Column")
            st.dataframe(miss_df, width="stretch", hide_index=True)
            st.write("#### Value Counts")
            if cat_cols:
                vc_col = st.selectbox("Column to inspect:", cat_cols, key="vc_col")
                vc_df = df[vc_col].value_counts().reset_index()
                vc_df.columns = [vc_col, "Count"]
                vc_df["% of Total"] = (vc_df["Count"] / len(df) * 100).round(2)
                st.dataframe(vc_df, width="stretch", hide_index=True)
            else:
                st.info("No categorical columns found.")


elif st.session_state.screen == 'config':
    if not st.session_state.datasets:
        st.warning("No data loaded.")
        if st.button("← Upload Data"):
            st.session_state.screen = 'upload'
            st.rerun()
    else:
        active_dash = render_sidebar()
        handle_auto_refresh()
        cols_per_row = active_dash['cols_per_row'] if active_dash else 3

        tb1, tb3 = st.columns([5, 1])
        with tb1:
            if st.session_state.renaming_dashboard_id == active_dash['id']:
                new_name = st.text_input("Dashboard name", value=active_dash['name'],
                                         key="inline_rename_title", label_visibility="collapsed")
                rc1, rc2, _ = st.columns([1, 1, 4])
                with rc1:
                    if st.button("Save", key="save_inline_rename", type="primary"):
                        rename_dashboard(active_dash['id'], new_name)
                        st.rerun()
                with rc2:
                    if st.button("Cancel", key="cancel_inline_rename"):
                        st.session_state.renaming_dashboard_id = None
                        st.rerun()
            else:
                dash_ds = get_active_dataset_for_dashboard(active_dash)
                ds_info = f"  ·  📁 {dash_ds['name']}" if dash_ds and len(st.session_state.datasets) > 1 else ""
                st.subheader(f"{active_dash['name']}{ds_info}")
        with tb3:
            if st.button("＋ Add Chart", key="add_chart_topbar", type="primary"):
                add_visualization()
                st.rerun()

        st.divider()

        if not active_dash or len(active_dash['visualizations']) == 0:
            st.info("Click **＋ Add Chart** above to get started.")
        else:
            visualizations = active_dash['visualizations']
            viz_ids = [v['id'] for v in visualizations]

            if st.session_state.selected_viz_id is not None and st.session_state.selected_viz_id not in viz_ids:
                st.session_state.selected_viz_id = viz_ids[0] if viz_ids else None
            selected_id = st.session_state.selected_viz_id

            def viz_widget_keys(aid, vid):
                return [
                    (f"sp_name_{aid}_{vid}",         'name'),
                    (f"sp_ct_{aid}_{vid}",            'chart_type'),
                    (f"sp_x_{aid}_{vid}",             'x_var'),
                    (f"sp_y_{aid}_{vid}",             'y_var'),
                    (f"sp_bar_agg_{aid}_{vid}",       'agg'),
                    (f"sp_pie_top_n_{aid}_{vid}",     'pie_top_n'),
                    (f"sp_sample_rows_{aid}_{vid}",   'sample_rows'),
                    (f"sp_trim_pct_{aid}_{vid}",      'trim_pct'),
                    (f"sp_x_min_{aid}_{vid}",         'x_filter_min'),
                    (f"sp_x_max_{aid}_{vid}",         'x_filter_max'),
                    (f"sp_y_min_{aid}_{vid}",         'y_filter_min'),
                    (f"sp_y_max_{aid}_{vid}",         'y_filter_max'),
                ]

            def confirm_viz_changes(viz, aid):
                vid = viz['id']
                for key, field in viz_widget_keys(aid, vid):
                    if key in st.session_state:
                        viz[field] = st.session_state[key]
                if viz['chart_type'] in ("Pie Chart", "Histogram"):
                    viz['y_var'] = None

            def revert_viz_changes(aid, vid):
                for key, _ in viz_widget_keys(aid, vid):
                    st.session_state.pop(key, None)

            def has_pending_changes(viz, aid):
                vid = viz['id']
                for key, field in viz_widget_keys(aid, vid):
                    if key in st.session_state:
                        if st.session_state[key] != viz.get(field):
                            return True
                return False

            grid_col, settings_col = st.columns([3, 1])

            with settings_col:
                if selected_id is not None:
                    sel_viz = next((v for v in visualizations if v['id'] == selected_id), None)
                    if sel_viz:
                        aid = active_dash['id']
                        vid = sel_viz['id']
                        viz_ds = get_active_dataset_for_viz(sel_viz, active_dash)

                        st.subheader("Chart Settings")
                        st.caption("Click the highlighted chart again to close.")

                        st.text_input("Name", value=sel_viz['name'], key=f"sp_name_{aid}_{vid}")

                        if len(st.session_state.datasets) > 1:
                            ddr = get_active_dataset_for_dashboard(active_dash)
                            inh = f"Dashboard · {ddr['name']}" if ddr else "Dashboard default"
                            new_viz_did = dataset_selectbox(
                                "Data source", f"sp_ds_{aid}_{vid}",
                                sel_viz.get('dataset_id'), include_inherit=True, inherit_label=inh)
                            if new_viz_did != sel_viz.get('dataset_id'):
                                sel_viz['dataset_id'] = new_viz_did
                                st.rerun()
                            viz_ds = get_active_dataset_for_viz(sel_viz, active_dash)


                        _pending_ct_key = f"sp_ct_pending_{aid}_{vid}"
                        if _pending_ct_key in st.session_state:
                            _new_ct = st.session_state.pop(_pending_ct_key)
                            if _new_ct in CHART_TYPES:
                                sel_viz['chart_type'] = _new_ct

                                st.session_state.pop(f"sp_ct_{aid}_{vid}", None)

                        st.selectbox("Chart type", CHART_TYPES,
                                     index=CHART_TYPES.index(sel_viz['chart_type']) if sel_viz['chart_type'] in CHART_TYPES else 0,
                                     key=f"sp_ct_{aid}_{vid}")
                        chart_type = st.session_state.get(f"sp_ct_{aid}_{vid}", sel_viz['chart_type'])

                        if viz_ds:
                            df_viz = viz_ds['df']
                            s_all_cols, s_numeric_cols = get_column_info(viz_ds['data_hash'], df_viz)
                            cardinality = get_cardinality(viz_ds['data_hash'], df_viz)
                            total_rows = len(df_viz)

                            if sel_viz.get('x_var') not in s_all_cols:
                                sel_viz['x_var'] = None
                            if sel_viz.get('y_var') not in (s_numeric_cols or s_all_cols):
                                sel_viz['y_var'] = None

                            sel_viz.setdefault('pie_top_n', CHART_MAX_PIE_SLICES)
                            sel_viz.setdefault('sample_rows', CHART_SAMPLE_ROWS)
                            sel_viz.setdefault('trim_pct', 0.0)
                            sel_viz.setdefault('x_filter_min', None)
                            sel_viz.setdefault('x_filter_max', None)
                            sel_viz.setdefault('y_filter_min', None)
                            sel_viz.setdefault('y_filter_max', None)

                            if chart_type == "Pie Chart":
                                st.selectbox("Category column", s_all_cols,
                                    index=s_all_cols.index(sel_viz['x_var']) if sel_viz['x_var'] in s_all_cols else 0,
                                    key=f"sp_x_{aid}_{vid}")
                                chosen_x = st.session_state.get(f"sp_x_{aid}_{vid}", sel_viz['x_var'])
                                if chosen_x:
                                    n_cats = cardinality.get(chosen_x, 0)
                                    if n_cats > CHART_MAX_PIE_SLICES:
                                        st.warning(f"⚠️ **{n_cats:,} unique values** in `{chosen_x}` — pie chart capped to top N slices.")
                                    elif n_cats > 10:
                                        st.caption(f"ℹ️ {n_cats} unique values — consider fewer categories for readability.")
                                st.number_input("Max slices (rest → Other)", min_value=2, max_value=100,
                                    value=int(sel_viz['pie_top_n']), step=5, key=f"sp_pie_top_n_{aid}_{vid}",
                                    help="Categories beyond this limit are merged into an 'Other' slice.")

                            elif chart_type == "Histogram":
                                ac = s_numeric_cols or s_all_cols
                                st.selectbox("Variable", ac,
                                    index=ac.index(sel_viz['x_var']) if sel_viz['x_var'] in ac else 0,
                                    key=f"sp_x_{aid}_{vid}")
                                if total_rows > CHART_SAMPLE_ROWS:
                                    st.caption(f"ℹ️ {total_rows:,} rows — histogram will sample {CHART_SAMPLE_ROWS:,}.")

                            elif chart_type in ["Box Plot", "Violin Plot"]:
                                x_opts = ["None"] + s_all_cols
                                st.selectbox("Category (optional)", x_opts,
                                    index=x_opts.index(sel_viz['x_var']) if sel_viz['x_var'] in x_opts else 0,
                                    key=f"sp_x_{aid}_{vid}")
                                ac = s_numeric_cols or s_all_cols
                                st.selectbox("Values", ac,
                                    index=ac.index(sel_viz['y_var']) if sel_viz['y_var'] in ac else 0,
                                    key=f"sp_y_{aid}_{vid}")
                                if total_rows > CHART_SAMPLE_ROWS:
                                    st.caption(f"ℹ️ {total_rows:,} rows — chart will sample {CHART_SAMPLE_ROWS:,}.")

                            elif chart_type == "Bar Chart":
                                sel_viz.setdefault('agg', 'sum')
                                _agg_opts = ['sum', 'mean', 'none']
                                _agg_labels = {'sum': 'Sum', 'mean': 'Mean (average)', 'none': 'No aggregation (raw rows)'}
                                _cur_agg = sel_viz.get('agg', 'sum')
                                st.selectbox("Aggregation",
                                    options=_agg_opts,
                                    format_func=lambda o: _agg_labels[o],
                                    index=_agg_opts.index(_cur_agg) if _cur_agg in _agg_opts else 0,
                                    key=f"sp_bar_agg_{aid}_{vid}",
                                    help="How to combine multiple rows with the same X value. Choose 'No aggregation' to plot each row individually.")
                                st.selectbox("X axis", s_all_cols,
                                    index=s_all_cols.index(sel_viz['x_var']) if sel_viz['x_var'] in s_all_cols else 0,
                                    key=f"sp_x_{aid}_{vid}")
                                ac = s_numeric_cols or s_all_cols
                                st.selectbox("Y axis", ac,
                                    index=ac.index(sel_viz['y_var']) if sel_viz['y_var'] in ac else 0,
                                    key=f"sp_y_{aid}_{vid}")
                                chosen_x = st.session_state.get(f"sp_x_{aid}_{vid}", sel_viz['x_var'])
                                chosen_agg = st.session_state.get(f"sp_bar_agg_{aid}_{vid}", _cur_agg)
                                if chosen_agg != 'none' and chosen_x and cardinality.get(chosen_x, 0) > CHART_MAX_BAR_CATS:
                                    st.caption(
                                        f"ℹ️ {cardinality[chosen_x]:,} unique values — "
                                        f"bar chart shows top {CHART_MAX_BAR_CATS} by {st.session_state.get(f'sp_y_{aid}_{vid}', sel_viz['y_var']) or 'Y'} {chosen_agg}."
                                    )

                            else:
                                _agg_opts = ['sum', 'mean', 'none']
                                _agg_labels = {'sum': 'Sum', 'mean': 'Mean (average)', 'none': 'No aggregation (raw rows)'}
                                if chart_type in ("Line Chart", "Area Chart"):
                                    sel_viz.setdefault('agg', 'sum')
                                    _cur_agg = sel_viz.get('agg', 'sum')
                                    st.selectbox("Aggregation",
                                        options=_agg_opts,
                                        format_func=lambda o: _agg_labels[o],
                                        index=_agg_opts.index(_cur_agg) if _cur_agg in _agg_opts else 0,
                                        key=f"sp_bar_agg_{aid}_{vid}",
                                        help="How to combine multiple rows with the same X value. Choose 'No aggregation' to plot each row as-is.")
                                else:

                                    st.session_state.setdefault(f"sp_bar_agg_{aid}_{vid}", sel_viz.get('agg', 'none'))
                                st.selectbox("X axis", s_all_cols,
                                    index=s_all_cols.index(sel_viz['x_var']) if sel_viz['x_var'] in s_all_cols else 0,
                                    key=f"sp_x_{aid}_{vid}")
                                ac = s_numeric_cols or s_all_cols
                                st.selectbox("Y axis", ac,
                                    index=ac.index(sel_viz['y_var']) if sel_viz['y_var'] in ac else 0,
                                    key=f"sp_y_{aid}_{vid}")
                                if total_rows > CHART_SAMPLE_ROWS:
                                    st.caption(f"ℹ️ {total_rows:,} rows — chart will sample {CHART_SAMPLE_ROWS:,}.")


                            chart_confirmed_once = sel_viz.get('x_var') is not None
                            if chart_confirmed_once:
                                cur_x = st.session_state.get(f"sp_x_{aid}_{vid}", sel_viz.get('x_var'))
                                cur_y = st.session_state.get(f"sp_y_{aid}_{vid}", sel_viz.get('y_var'))

                                if cur_x == "None": cur_x = None
                                if cur_y == "None": cur_y = None
                                recs = recommend_chart_types(cur_x, cur_y, df_viz, s_numeric_cols, cardinality)
                                if recs:


                                    active_chart_type = sel_viz['chart_type']
                                    _rec_exp_key = f"sp_rec_exp_{aid}_{vid}"
                                    _rec_exp_open = st.session_state.pop(_rec_exp_key, False)
                                    with st.expander("Recommended chart types", expanded=_rec_exp_open):
                                        for rec_type, rec_reason in recs:
                                            is_current = rec_type == active_chart_type
                                            if is_current:
                                                st.markdown(f"**{rec_type}** ✓")
                                            else:
                                                st.markdown(f"**{rec_type}**")
                                            st.caption(rec_reason)
                                            if not is_current:
                                                if st.button(f"Switch to {rec_type}",
                                                             key=f"sp_rec_{aid}_{vid}_{rec_type}",
                                                             use_container_width=True):
                                                    st.session_state[f"sp_ct_pending_{aid}_{vid}"] = rec_type
                                                    st.session_state[_rec_exp_key] = True
                                                    st.rerun()

                            trim_applicable = chart_type not in ("Pie Chart",)
                            with st.expander("Trim Extreme Values", expanded=float(sel_viz['trim_pct']) > 0):
                                if trim_applicable:
                                    st.slider(
                                        "Percentile cutoff",
                                        min_value=0.0, max_value=25.0,
                                        value=float(sel_viz['trim_pct']),
                                        step=0.5, key=f"sp_trim_pct_{aid}_{vid}", format="%.1f%%",
                                        help="0% = no trimming. 5% removes the bottom and top 5% of values.",
                                    )
                                    cur_trim = st.session_state.get(f"sp_trim_pct_{aid}_{vid}", sel_viz['trim_pct'])
                                    if cur_trim > 0:
                                        st.caption(f"Keeping values between the **{cur_trim:.1f}th** and **{100-cur_trim:.1f}th** percentile.")
                                    else:
                                        st.caption("No trimming applied.")
                                else:
                                    st.caption("Not available for Pie Charts.")
                                    if f"sp_trim_pct_{aid}_{vid}" not in st.session_state:
                                        st.session_state[f"sp_trim_pct_{aid}_{vid}"] = float(sel_viz['trim_pct'])


                            xy_filter_applicable = chart_type not in ("Pie Chart", "Histogram")
                            _xfmin = sel_viz.get('x_filter_min')
                            _xfmax = sel_viz.get('x_filter_max')
                            _yfmin = sel_viz.get('y_filter_min')
                            _yfmax = sel_viz.get('y_filter_max')
                            _xy_active = any(v is not None for v in [_xfmin, _xfmax, _yfmin, _yfmax])
                            with st.expander("Axis Range Filter", expanded=_xy_active):
                                if xy_filter_applicable:
                                    cur_x_col = st.session_state.get(f"sp_x_{aid}_{vid}", sel_viz.get('x_var'))
                                    cur_y_col = st.session_state.get(f"sp_y_{aid}_{vid}", sel_viz.get('y_var'))
                                    if cur_x_col and cur_x_col in s_numeric_cols:
                                        st.caption(f"**X axis** — `{cur_x_col}`")
                                        xc1, xc2 = st.columns(2)
                                        with xc1:
                                            st.number_input("X min", value=_xfmin,
                                                key=f"sp_x_min_{aid}_{vid}",
                                                help="Leave blank for no lower bound on X.")
                                        with xc2:
                                            st.number_input("X max", value=_xfmax,
                                                key=f"sp_x_max_{aid}_{vid}",
                                                help="Leave blank for no upper bound on X.")
                                    else:

                                        for _k in (f"sp_x_min_{aid}_{vid}", f"sp_x_max_{aid}_{vid}"):
                                            st.session_state.setdefault(_k, None)
                                        if cur_x_col and cur_x_col not in s_numeric_cols:
                                            st.caption("X axis filter only available for numeric columns.")
                                    if cur_y_col and cur_y_col in s_numeric_cols:
                                        st.caption(f"**Y axis** — `{cur_y_col}`")
                                        yc1, yc2 = st.columns(2)
                                        with yc1:
                                            st.number_input("Y min", value=_yfmin,
                                                key=f"sp_y_min_{aid}_{vid}",
                                                help="Leave blank for no lower bound on Y.")
                                        with yc2:
                                            st.number_input("Y max", value=_yfmax,
                                                key=f"sp_y_max_{aid}_{vid}",
                                                help="Leave blank for no upper bound on Y.")
                                    else:
                                        for _k in (f"sp_y_min_{aid}_{vid}", f"sp_y_max_{aid}_{vid}"):
                                            st.session_state.setdefault(_k, None)
                                    if _xy_active:
                                        def _reset_xy_filters(_aid=aid, _vid=vid, _viz=sel_viz):
                                            for _fk in ('x_filter_min', 'x_filter_max', 'y_filter_min', 'y_filter_max'):
                                                _viz[_fk] = None
                                            for _sk in (f"sp_x_min_{_aid}_{_vid}", f"sp_x_max_{_aid}_{_vid}",
                                                        f"sp_y_min_{_aid}_{_vid}", f"sp_y_max_{_aid}_{_vid}"):
                                                st.session_state.pop(_sk, None)
                                        st.button("🔄 Reset Filters", key=f"sp_xy_reset_{aid}_{vid}",
                                                  on_click=_reset_xy_filters, use_container_width=True)
                                    else:
                                        st.caption("Enter values above to filter rows by axis range before rendering.")
                                else:
                                    st.caption("Not available for Pie Charts or Histograms.")
                                    for _k in (f"sp_x_min_{aid}_{vid}", f"sp_x_max_{aid}_{vid}",
                                               f"sp_y_min_{aid}_{vid}", f"sp_y_max_{aid}_{vid}"):
                                        st.session_state.setdefault(_k, None)

                            if chart_type != "Pie Chart":
                                with st.expander("Performance", expanded=False):
                                    st.number_input("Max rows rendered", min_value=100, max_value=100000,
                                        value=int(sel_viz['sample_rows']), step=1000,
                                        key=f"sp_sample_rows_{aid}_{vid}",
                                        help="If the dataset exceeds this limit, a random sample is used.")
                            else:
                                if f"sp_sample_rows_{aid}_{vid}" not in st.session_state:
                                    st.session_state[f"sp_sample_rows_{aid}_{vid}"] = int(sel_viz['sample_rows'])

                            if len(st.session_state.datasets) > 1:
                                st.caption(f"📁 {viz_ds['name']}")
                        else:
                            st.warning("No dataset available.")

                        st.write("")
                        st.divider()

                        pending = has_pending_changes(sel_viz, aid)
                        confirm_col, revert_col = st.columns(2)
                        with confirm_col:
                            if st.button("✅ Confirm", key=f"sp_confirm_{vid}", width="stretch",
                                         type="primary" if pending else "secondary", disabled=not pending):
                                confirm_viz_changes(sel_viz, aid)
                                st.rerun()
                        with revert_col:
                            if st.button("↩ Revert", key=f"sp_revert_{vid}", width="stretch",
                                         disabled=not pending):
                                revert_viz_changes(aid, vid)
                                st.rerun()

                        if pending:
                            st.caption("⚠️ You have unsaved changes.")

                        st.write("")
                        if st.button("🗑️ Delete This Chart", key=f"sp_del_{vid}", width="stretch"):
                            delete_visualization(vid)
                            st.rerun()

            with grid_col:
                num_viz = len(visualizations)
                num_rows = (num_viz + cols_per_row - 1) // cols_per_row
                viz_index = 0

                for row in range(num_rows):
                    row_cols = st.columns(cols_per_row)
                    for ci in range(cols_per_row):
                        if viz_index >= num_viz:
                            break
                        viz = visualizations[viz_index]
                        viz_ds = get_active_dataset_for_viz(viz, active_dash)
                        is_selected = viz['id'] == selected_id

                        with row_cols[ci]:
                            with st.container(border=True):
                                hc = st.columns([3, 1, 1, 1])
                                with hc[0]:
                                    label = f"{'● ' if is_selected else ''}{viz['name']}"
                                    if st.button(label, key=f"sel_{active_dash['id']}_{viz['id']}",
                                                 width="stretch",
                                                 type="primary" if is_selected else "secondary"):
                                        st.session_state.selected_viz_id = None if is_selected else viz['id']
                                        st.rerun()
                                with hc[1]:
                                    st.button("←", key=f"left_{active_dash['id']}_{viz['id']}",
                                              help="Move left", width="stretch",
                                              on_click=move_visualization, args=(viz['id'], 'left'))
                                with hc[2]:
                                    st.button("→", key=f"right_{active_dash['id']}_{viz['id']}",
                                              help="Move right", width="stretch",
                                              on_click=move_visualization, args=(viz['id'], 'right'))
                                with hc[3]:
                                    st.button("🗑️", key=f"delete_{active_dash['id']}_{viz['id']}",
                                              help="Delete", width="stretch",
                                              on_click=delete_visualization, args=(viz['id'],))

                                if viz_ds:
                                    viz.setdefault('pie_top_n', CHART_MAX_PIE_SLICES)
                                    viz.setdefault('sample_rows', CHART_SAMPLE_ROWS)
                                    viz.setdefault('trim_pct', 0.0)
                                    viz.setdefault('agg', 'sum')
                                    result = create_chart(
                                        viz_ds['data_hash'],
                                        viz['chart_type'],
                                        viz.get('x_var'),
                                        viz.get('y_var'),
                                        viz_ds['data_dict'],
                                        pie_top_n=int(viz['pie_top_n']),
                                        sample_rows=int(viz['sample_rows']),
                                        trim_pct=float(viz['trim_pct']),
                                        agg=viz.get('agg', 'sum'),
                                        x_filter_min=viz.get('x_filter_min'),
                                        x_filter_max=viz.get('x_filter_max'),
                                        y_filter_min=viz.get('y_filter_min'),
                                        y_filter_max=viz.get('y_filter_max'),
                                    )
                                    fig, note = result if isinstance(result, tuple) else (result, "")
                                    if fig:
                                        if len(st.session_state.datasets) > 1:
                                            st.caption(f"📁 {viz_ds['name']}")
                                        st.plotly_chart(fig, width="stretch",
                                                        key=f"plot_{active_dash['id']}_{viz['id']}")
                                    else:
                                        st.info("Click chart name to configure")
                                else:
                                    st.warning("No dataset assigned.")

                        viz_index += 1
