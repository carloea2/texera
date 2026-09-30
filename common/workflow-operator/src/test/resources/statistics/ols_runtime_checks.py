# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""PythonWorkerPool checks with an optional real pytexera lifecycle.

The real NumPy, pandas, SciPy and statsmodels packages always execute. Without
--real only the pytexera import seam is stubbed. Neither mode replaces a full
worker/localhost test. Expected fits use normal equations and
explicit textbook formulas, not a second call to statsmodels. Each JSON request
gets fresh generated modules and a fresh unittest suite.
"""

import base64
import contextlib
import io
import json
import sys
import traceback
import types
import unittest
from typing import Iterator, Optional

import numpy as np
import pandas as pd
from scipy.stats import f, t


class UDFTableOperator:
    def decode_python_template(self, value):
        return base64.b64decode(value).decode("utf-8")


REAL = "--real" in sys.argv
if REAL:
    sys.argv.remove("--real")
else:
    stub = types.ModuleType("pytexera")
    stub.UDFTableOperator = UDFTableOperator
    stub.overrides = lambda fn: fn
    stub.Table = pd.DataFrame
    stub.TableLike = object
    stub.Iterator = Iterator
    stub.Optional = Optional
    sys.modules["pytexera"] = stub


class OLSChecks(unittest.TestCase):
    def setUp(self):
        self.frame = pd.DataFrame({"y": [1, 3, 2, 5, 4], "x": [0, 1, 2, 3, 4]})

    def run_fit(self, frame, config="default"):
        before = frame.copy(deep=True)
        rows = list(self.operators[config]().process_table(frame, 0))
        pd.testing.assert_frame_equal(frame, before)
        self.assertGreater(len(rows), 0)
        for row in rows:
            self.assertEqual(list(row), self.payload["columns"])
            # Reject NaN/Infinity and non-serializable NumPy scalar outputs.
            json.dumps(row, allow_nan=False)
        return rows

    def assert_reference(self, rows, frame, predictors, intercept=True):
        y = frame["y"].to_numpy(dtype=float)
        x = frame[predictors].to_numpy(dtype=float)
        if intercept:
            x = np.column_stack([np.ones(len(x)), x])
        covariance_base = np.linalg.inv(x.T @ x)
        beta = covariance_base @ x.T @ y
        residual = y - x @ beta
        df_residual = len(y) - x.shape[1]
        mse = residual @ residual / df_residual
        std_error = np.sqrt(np.diag(covariance_base) * mse)
        t_values = beta / std_error
        tss = ((y - y.mean()) ** 2).sum() if intercept else (y**2).sum()
        r_squared = 1 - residual @ residual / tss
        df_model = x.shape[1] - int(intercept)
        f_value = (tss - residual @ residual) / df_model / mse
        for i, row in enumerate(rows):
            expected = {
                "estimate": beta[i],
                "std_error": std_error[i],
                "t_statistic": t_values[i],
                "p_value": 2 * t.sf(abs(t_values[i]), df_residual),
                "ci_lower": beta[i] - t.ppf(0.975, df_residual) * std_error[i],
                "ci_upper": beta[i] + t.ppf(0.975, df_residual) * std_error[i],
                "residual_std_error": np.sqrt(mse),
                "r_squared": r_squared,
                "adj_r_squared": 1
                - (1 - r_squared) * (len(y) - int(intercept)) / df_residual,
                "f_statistic": f_value,
                "f_p_value": f.sf(f_value, df_model, df_residual),
            }
            for field, value in expected.items():
                self.assertAlmostEqual(row[field], value, places=9, msg=field)
            self.assertEqual(row["n_used"], len(frame))
            self.assertEqual(row["df_residual"], df_residual)
            self.assertEqual(row["df_model"], df_model)
            self.assertEqual(row["is_intercept"], intercept and i == 0)

    def test_textbook_simple_fit(self):
        rows = self.run_fit(self.frame)
        self.assertEqual([r["term"] for r in rows], ["(Intercept)", "x"])
        self.assertAlmostEqual(rows[0]["estimate"], 1.4)
        self.assertAlmostEqual(rows[1]["estimate"], 0.8)
        self.assertAlmostEqual(rows[0]["r_squared"], 0.64)
        self.assertEqual(rows[0]["n_input"], 5)
        self.assertEqual(rows[0]["n_omitted"], 0)
        self.assert_reference(rows, self.frame, ["x"])

    def test_invalid_config_refused_without_schema_propagation(self):
        with self.assertRaisesRegex(ValueError, "response"):
            self.run_fit(self.frame, "invalid")

    def test_no_intercept_uses_uncentered_statistics(self):
        rows = self.run_fit(self.frame, "no_intercept")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["term"], "x")
        self.assert_reference(rows, self.frame, ["x"], intercept=False)

    def test_multiple_predictors(self):
        frame = pd.DataFrame(
            {
                "y": [2, 4, 3, 8, 7, 8, 4, 11],
                "x": [0, 1, 2, 3, 4, 5, 6, 7],
                "z": [2, 0, 3, 1, 4, 3, 0, 5],
            }
        )
        rows = self.run_fit(frame, "multiple")
        self.assertEqual([r["term"] for r in rows], ["(Intercept)", "x", "z"])
        self.assert_reference(rows, frame, ["x", "z"])

    def test_omits_only_selected_missing_values(self):
        frame = pd.concat(
            [self.frame, pd.DataFrame({"y": [None, 8, None], "x": [6, None, None]})],
            ignore_index=True,
        )
        frame["unselected"] = None
        rows = self.run_fit(frame)
        self.assertEqual(rows[0]["n_input"], 8)
        self.assertEqual(rows[0]["n_omitted"], 3)
        self.assert_reference(rows, self.frame, ["x"])

    def test_strict_missing_mode(self):
        frame = self.frame.astype(float)
        frame.loc[0, "y"] = np.nan
        with self.assertRaisesRegex(ValueError, "missing"):
            self.run_fit(frame, "strict")
        self.run_fit(self.frame.assign(unselected=None), "strict")

    def test_nullable_pandas_types(self):
        frame = pd.DataFrame({"y": [1, 3, 2, 5, 4, pd.NA], "x": [0, 1, 2, 3, 4, 8]})
        frame = frame.astype({"x": "Int64", "y": "Float64"})
        rows = self.run_fit(frame)
        self.assertEqual(rows[0]["n_omitted"], 1)
        self.assert_reference(rows, self.frame, ["x"])

    def test_empty_and_all_missing(self):
        for frame in [pd.DataFrame(), self.frame.iloc[:0], self.frame * np.nan]:
            with self.subTest(frame=frame):
                with self.assertRaisesRegex(ValueError, "No complete rows"):
                    self.run_fit(frame)

    def test_positive_residual_degrees_of_freedom_required(self):
        for count in [1, 2]:
            with self.assertRaisesRegex(ValueError, "more complete rows"):
                self.run_fit(self.frame.iloc[:count])

    def test_rank_deficiency_and_constant_predictor_rejected(self):
        with self.assertRaisesRegex(ValueError, "rank-deficient"):
            self.run_fit(self.frame.assign(x=1))
        with self.assertRaisesRegex(ValueError, "rank-deficient"):
            self.run_fit(self.frame.assign(z=self.frame.x * 2), "multiple")
        with self.assertRaisesRegex(ValueError, "rank-deficient"):
            self.run_fit(self.frame.assign(x=0), "no_intercept")

    def test_constant_predictor_without_intercept_is_valid(self):
        frame = self.frame.assign(x=2)
        rows = self.run_fit(frame, "no_intercept")
        self.assert_reference(rows, frame, ["x"], intercept=False)

    def test_infinite_retained_values_rejected(self):
        for column in ["y", "x"]:
            for value in [np.inf, -np.inf]:
                frame = self.frame.astype(float)
                frame.loc[0, column] = value
                with self.assertRaisesRegex(ValueError, "finite"):
                    self.run_fit(frame)

    def test_invalid_runtime_column_types_rejected(self):
        for values in [["1", "3", "2", "5", "4"], [True] * 5, [1j] * 5]:
            for column in ["y", "x"]:
                with self.assertRaisesRegex(ValueError, "numeric"):
                    self.run_fit(self.frame.assign(**{column: values}))

    def test_absent_or_duplicate_input_columns(self):
        with self.assertRaisesRegex(ValueError, "column"):
            self.run_fit(self.frame.drop(columns=["x"]))
        with self.assertRaisesRegex(ValueError, "unique"):
            self.run_fit(pd.concat([self.frame, self.frame[["x"]]], axis=1))

    def test_constant_response_has_null_undefined_statistics(self):
        for value in [0, 5]:
            rows = self.run_fit(self.frame.assign(y=value))
            self.assertIsNone(rows[0]["r_squared"])
            self.assertIsNone(rows[0]["adj_r_squared"])
            self.assertIsNone(rows[0]["f_statistic"])
            self.assertIsNone(rows[0]["f_p_value"])

    def test_perfect_fit_serializable(self):
        rows = self.run_fit(self.frame.assign(y=2 * self.frame.x + 1))
        self.assertAlmostEqual(rows[0]["estimate"], 1)
        self.assertAlmostEqual(rows[1]["estimate"], 2)
        self.assertAlmostEqual(rows[0]["r_squared"], 1)

    def test_unicode_newlines_quotes_and_intercept_name(self):
        frame = self.frame.assign(z=[1, 0, 1, 0, 1]).rename(
            columns={"y": "réponse\n'\\", "x": "(Intercept)", "z": "x\n'\"\\λ"}
        )
        rows = self.run_fit(frame, "unusual")
        self.assertEqual(
            [r["term"] for r in rows], ["(Intercept)", "(Intercept)", "x\n'\"\\λ"]
        )
        self.assertEqual([r["is_intercept"] for r in rows], [True, False, False])

    def test_order_and_repeated_runs_independent(self):
        normal = self.run_fit(self.frame)
        reversed_rows = self.run_fit(self.frame.iloc[::-1])
        for a, b in zip(normal, reversed_rows):
            for field in ["estimate", "std_error", "r_squared", "f_p_value"]:
                self.assertAlmostEqual(a[field], b[field], places=10)
        self.assertEqual(normal, self.run_fit(self.frame))

    @unittest.skipUnless(REAL, "requires generated protobufs and real pytexera")
    def test_full_table_lifecycle_and_schema_finalization(self):
        from core.models import Schema, Tuple
        from core.models.table import all_output_to_tuple

        schema = Schema(raw_schema=self.payload["types"])
        op = self.operators["default"]()
        op.open()
        records = self.frame.to_dict("records") + [{"y": None, "x": 6}]
        for record in records:
            self.assertEqual(list(op.process_tuple(Tuple(record), 0)), [None])
        results = list(op.on_finish(0))
        self.assert_reference(results, self.frame, ["x"])
        self.assertEqual(results[0]["n_input"], 6)
        self.assertEqual(results[0]["n_omitted"], 1)
        for result in results:
            tuples = list(all_output_to_tuple(result))
            self.assertEqual(len(tuples), 1)
            tuples[0].finalize(schema)
            self.assertEqual(tuples[0].get_fields(), tuple(result.values()))
        op.close()
        empty_op = self.operators["default"]()
        with self.assertRaisesRegex(ValueError, "No complete rows"):
            list(empty_op.on_finish(0))
        print("REAL_TABLE_LIFECYCLE_OK")


def run_checks(payload):
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        try:
            operators = {}
            for name in (
                "default",
                "no_intercept",
                "strict",
                "multiple",
                "unusual",
                "invalid",
            ):
                namespace = {"__name__": "generated_ols_" + name}
                exec(compile(payload[name], name, "exec"), namespace)
                operators[name] = namespace["ProcessTableOperator"]
            # Bind state to this request's test class, never to a previous suite.
            checks = type(
                "RequestOLSChecks",
                (OLSChecks,),
                {"operators": operators, "payload": payload},
            )
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(checks)
            result = unittest.TextTestRunner(stream=stderr, verbosity=2).run(suite)
            exit_code = 0 if result.wasSuccessful() else 1
        except Exception:
            traceback.print_exc()
            exit_code = 1
    return {"exit": exit_code, "stdout": stdout.getvalue(), "stderr": stderr.getvalue()}


def main():
    print(json.dumps({"ready": True}), flush=True)
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            result = run_checks(json.loads(line))
        except Exception:
            result = {"exit": 1, "stdout": "", "stderr": traceback.format_exc()}
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
