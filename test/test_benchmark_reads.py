import unittest

from benchmark_reads import difference, parse_metrics, parse_runs


class BenchmarkMetricsTest(unittest.TestCase):
    def test_metrics_and_profile_control_do_not_replace_query_timing(self):
        stdout = (
            'BENCH_QUERY_0\n[{"n":10}]\nRun Time (s): real 1.25 user 0.1 sys 0.0\n'
            'Run Time (s): real 0.001 user 0.0 sys 0.0\n'
            'BENCH_METRICS_0\n[{"phase":"execution","metric":"rows","value":2048}]\n'
            'Run Time (s): real 0.002 user 0.0 sys 0.0\n'
            'BENCH_QUERY_1\n[{"n":10}]\nRun Time (s): real 0.75 user 0.1 sys 0.0\n'
            'BENCH_METRICS_1\n[{"phase":"execution","metric":"rows","value":4096}]\n'
            'BENCH_DONE\n'
        )
        runs = parse_runs(stdout)
        self.assertEqual([run["seconds"] for run in runs], [1.25, 0.75])
        self.assertEqual([run["result"] for run in runs], [[{"n": 10}], [{"n": 10}]])
        first = parse_metrics(stdout, "BENCH_METRICS_0")
        second = parse_metrics(stdout, "BENCH_METRICS_1")
        self.assertEqual(difference(second, first), {"execution.rows": 2048})

    def test_missing_query_timer_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_runs('BENCH_QUERY_0\n[{"n":10}]\nBENCH_DONE\n')


if __name__ == "__main__":
    unittest.main()
