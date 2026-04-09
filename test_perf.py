import io
import json
import time
import hashlib
import pathlib
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd


class _SessionState(dict):
    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k)
    def __setattr__(self, k, v):
        self[k] = v

_ss = _SessionState({
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
})

def _cache_data(*args, **kwargs):
    def decorator(fn):
        return fn
    return args[0] if args and callable(args[0]) else decorator

def _cache_resource(*args, **kwargs):
    def decorator(fn):
        return fn
    return args[0] if args and callable(args[0]) else decorator

st_mock = MagicMock()
st_mock.session_state = _ss
st_mock.cache_data = _cache_data
st_mock.cache_resource = _cache_resource
st_mock.set_page_config = MagicMock()

import sys
sys.modules['streamlit'] = st_mock

import importlib
import plotly.express as _px

spec = importlib.util.spec_from_file_location("viz", pathlib.Path(__file__).parent / "viz.py")
viz = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(viz)
except Exception:
    pass

viz.px = _px


@contextmanager
def timed(label, limit_s):
    t0 = time.perf_counter()
    yield
    elapsed = time.perf_counter() - t0
    status = "PASS" if elapsed < limit_s else "FAIL"
    print(f"[{status:<4}] {label:<45} {elapsed:>8.3f}s  (limit {limit_s}s)")


def make_df(n_rows, n_cat=10):
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "category": rng.choice([f"cat_{i}" for i in range(n_cat)], size=n_rows),
        "value": rng.random(n_rows) * 1000,
        "value2": rng.integers(0, 500, size=n_rows).astype(float),
    })


def df_to_csv_bytes(df):
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return buf


def firestore_response(n_docs):
    rng = np.random.default_rng(0)
    return {
        "documents": [
            {
                "name": f"doc{i}",
                "fields": {
                    "id": {"integerValue": str(i)},
                    "name": {"stringValue": f"item_{i}"},
                    "score": {"doubleValue": float(rng.random())},
                },
            }
            for i in range(n_docs)
        ]
    }


class TestCSVLoad(unittest.TestCase):

    def _load_csv(self, df, sep=','):
        buf = df_to_csv_bytes(df)
        raw = buf.read().decode('utf-8')
        loaded = viz.load_csv_df(raw, sep)
        hashlib.md5(pd.util.hash_pandas_object(loaded, index=True).values).hexdigest()
        return loaded

    def test_1k(self):
        df = make_df(1_000)
        with timed("CSV load 1k rows", 0.1):
            r = self._load_csv(df)
        self.assertEqual(len(r), 1_000)

    def test_50k(self):
        df = make_df(50_000)
        with timed("CSV load 50k rows", 0.5):
            r = self._load_csv(df)
        self.assertEqual(len(r), 50_000)

    def test_250k(self):
        df = make_df(250_000)
        with timed("CSV load 250k rows", 2.0):
            r = self._load_csv(df)
        self.assertEqual(len(r), 250_000)

    def test_500k(self):
        df = make_df(500_000)
        with timed("CSV load 500k rows", 4.0):
            r = self._load_csv(df)
        self.assertEqual(len(r), 500_000)

    def test_1m(self):
        df = make_df(1_000_000)
        with timed("CSV load 1M rows", 8.0):
            r = self._load_csv(df)
        self.assertEqual(len(r), 1_000_000)

    def test_2_5m(self):
        df = make_df(2_500_000)
        with timed("CSV load 2.5M rows", 15.0):
            r = self._load_csv(df)
        self.assertEqual(len(r), 2_500_000)


def make_json(n_rows, orient, n_cat=10):
    """Return a JSON string and expected row count for the given orient."""
    df = make_df(n_rows, n_cat)
    return df.to_json(orient=orient), len(df)


class TestJSONLoad(unittest.TestCase):
    """
    Performance benchmarks for viz.load_json_df.
    Three orientations are exercised:
      - records  (list of row-dicts, most common upload format)
      - auto     (sniffs list → json_normalize; dict → pd.DataFrame)
      - columns  ({col: {idx: val}})
      - split    ({index, columns, data} — compact pandas export)
    Limits are ~2-3x the CSV limits because JSON carries more
    per-character parsing overhead than CSV.
    """

    def _run(self, n_rows, orient, make_orient, limit_s):
        raw, expected = make_json(n_rows, make_orient)
        label = f"JSON/{orient:<8} {n_rows:>9,} rows"
        with timed(label, limit_s):
            df = viz.load_json_df(raw, orient)
            hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()
        self.assertEqual(len(df), expected)
        self.assertFalse(df.empty)

    # records ----------------------------------------------------------------
    def test_records_1k(self):    self._run(1_000,     "records", "records", 0.1)
    def test_records_50k(self):   self._run(50_000,    "records", "records", 0.8)
    def test_records_250k(self):  self._run(250_000,   "records", "records", 4.0)
    def test_records_500k(self):  self._run(500_000,   "records", "records", 8.0)
    def test_records_1m(self):    self._run(1_000_000, "records", "records", 16.0)

    # auto — list input → json_normalize path --------------------------------
    def test_auto_list_1k(self):   self._run(1_000,     "auto", "records", 0.1)
    def test_auto_list_50k(self):  self._run(50_000,    "auto", "records", 0.8)
    def test_auto_list_250k(self): self._run(250_000,   "auto", "records", 4.0)
    def test_auto_list_500k(self): self._run(500_000,   "auto", "records", 8.0)
    def test_auto_list_1m(self):   self._run(1_000_000, "auto", "records", 16.0)

    # auto — dict input → pd.DataFrame path ---------------------------------
    def test_auto_dict_1k(self):   self._run(1_000,   "auto", "columns", 0.1)
    def test_auto_dict_50k(self):  self._run(50_000,  "auto", "columns", 1.0)
    def test_auto_dict_250k(self): self._run(250_000, "auto", "columns", 5.0)

    # columns ----------------------------------------------------------------
    def test_columns_1k(self):    self._run(1_000,   "columns", "columns", 0.1)
    def test_columns_50k(self):   self._run(50_000,  "columns", "columns", 1.0)
    def test_columns_250k(self):  self._run(250_000, "columns", "columns", 5.0)
    def test_columns_500k(self):  self._run(500_000, "columns", "columns", 10.0)

    # split ------------------------------------------------------------------
    def test_split_1k(self):    self._run(1_000,   "split", "split", 0.1)
    def test_split_50k(self):   self._run(50_000,  "split", "split", 0.8)
    def test_split_250k(self):  self._run(250_000, "split", "split", 4.0)
    def test_split_500k(self):  self._run(500_000, "split", "split", 8.0)


class TestChartRender(unittest.TestCase):

    def setUp(self):
        self.df = make_df(10_000)
        self.data_hash = hashlib.md5(
            pd.util.hash_pandas_object(self.df, index=True).values
        ).hexdigest()
        self.data_dict = self.df.to_dict("list")

    def test_scatter(self):
        with timed("Scatter 10k", 0.2):
            result = viz.create_chart(
                self.data_hash, "Scatter Plot",
                "value", "value2", self.data_dict
            )
        fig, _ = result if isinstance(result, tuple) else (result, "")
        self.assertIsNotNone(fig)

    def test_bar(self):
        with timed("Bar+agg 10k", 0.8):
            result = viz.create_chart(
                self.data_hash, "Bar Chart",
                "category", "value",
                self.data_dict, agg="sum"
            )
        fig, _ = result if isinstance(result, tuple) else (result, "")
        self.assertIsNotNone(fig)

class TestFirestoreLoad(unittest.TestCase):

    def _run(self, n, limit):
        payload = firestore_response(n)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload

        with patch.object(viz, "http") as mock_http:
            mock_http.return_value.get.return_value = mock_resp
            with timed(f"Firestore {n:,} docs", limit):
                df = viz.fb_firestore_load_collection(
                    "proj", "token", "col", limit=n
                )

        self.assertEqual(len(df), n)

    def test_1k(self):    self._run(1_000,     0.05)
    def test_50k(self):   self._run(50_000,    1.0)
    def test_100k(self):  self._run(100_000,   2.0)
    def test_250k(self):  self._run(250_000,   4.0)
    def test_1m(self):    self._run(1_000_000, 12.0)


class TestConfigExport(unittest.TestCase):

    def setUp(self):
        df = make_df(1_000)
        h = hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()

        _ss['datasets'] = {
            0: {'id': 0, 'name': 'DS1', 'df': df,
                'data_dict': df.to_dict('list'), 'data_hash': h},
        }
        _ss['next_dataset_id'] = 1
        _ss['active_dataset_id'] = 0

        def make_viz(vid):
            return {
                'id': vid, 'name': f'Chart {vid}', 'chart_type': 'Bar Chart',
                'x_var': 'category', 'y_var': 'value', 'position': vid,
                'dataset_id': 0, 'pie_top_n': 30, 'sample_rows': 10000,
                'trim_pct': 0.0, 'agg': 'sum',
                'x_filter_min': None, 'x_filter_max': None,
                'y_filter_min': None, 'y_filter_max': None,
            }

        dashboards = []
        for d in range(3):
            dashboards.append({
                'id': d,
                'name': f'Dashboard {d}',
                'dataset_id': 0,
                'cols_per_row': 3,
                'next_viz_id': 4,
                'visualizations': [make_viz(v) for v in range(4)],
            })

        _ss['dashboards'] = dashboards
        _ss['next_dashboard_id'] = 3
        _ss['active_dashboard_id'] = 0

    def test_export(self):
        with timed("Config export 3 dashboards / 12 charts", 0.1):
            data = viz.get_config_download_bytes()

        parsed = json.loads(data)
        total_viz = sum(len(d['visualizations']) for d in parsed['dashboards'])
        self.assertEqual(len(parsed['dashboards']), 3)
        self.assertEqual(total_viz, 12)
        self.assertIsInstance(data, bytes)



if __name__ == "__main__":
    print("\n=== viz.py Performance Benchmarks ===\n")
    unittest.main(verbosity=0)