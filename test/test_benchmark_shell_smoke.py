"""Optional offline shell smoke test: set ONELAKE_BENCH_SMOKE_SHELL to duckdb.exe."""

from decimal import Decimal
import os
from pathlib import Path
import unittest

from benchmark_parallel_reads import decode_section, isolated_extensions, parse_timer
from benchmark_process import MeasuredChild


@unittest.skipUnless(
    os.environ.get("ONELAKE_BENCH_SMOKE_SHELL"), "Set ONELAKE_BENCH_SMOKE_SHELL for offline SELECT smoke"
)
class ShellSmokeTest(unittest.TestCase):
    def test_prepared_select_timing_exact_decimal_and_live_final_resources(self):
        shell = Path(os.environ["ONELAKE_BENCH_SMOKE_SHELL"]).resolve()
        with isolated_extensions() as directory:
            with MeasuredChild([str(shell), "-init", os.devnull, "-unsigned", "-batch"], timeout=15) as child:
                child.send(
                    ".bail on\n.mode json\n.timer on\n"
                    "SET threads=4;\nSET memory_limit='4GB';\n"
                    "SET enable_external_file_cache=false;\n"
                    "SET extension_directory='" + str(directory).replace("'", "''") + "';\n"
                    ".print BENCH_PREPARE\n"
                    "PREPARE bench_query AS SELECT COUNT(*) AS row_count, "
                    "123456789012345678901234567890.12::DECIMAL(38,2) AS total_price_2dp "
                    "FROM range(10);\n.print BENCH_READY\n"
                )
                child.wait_marker("BENCH_READY")
                self.assertGreaterEqual(parse_timer(child.stdout, "BENCH_PREPARE")["seconds"], 0)
                child.start_measurement()
                child.send(".print BENCH_QUERY\nEXECUTE bench_query;\n.print BENCH_DONE\n")
                child.wait_marker("BENCH_DONE", measure=True)
                child.sample()
                self.assertIsNone(child.process.poll())
                result = decode_section(child.stdout, "BENCH_QUERY")
                self.assertEqual(
                    result,
                    [
                        {
                            "row_count": 10,
                            "total_price_2dp": Decimal("123456789012345678901234567890.12"),
                        }
                    ],
                )
                self.assertGreaterEqual(parse_timer(child.stdout, "BENCH_QUERY")["cpu_seconds"], 0)
                if os.name == "nt":
                    self.assertGreater(child.resources()["process_peak_working_set_bytes"], 0)
                    self.assertIsNotNone(child.resources()["query_final_alive_sample"])
                child.send(".quit\n")
                child.wait_exit()
            self.assertEqual(child.process.returncode, 0)


if __name__ == "__main__":
    unittest.main()
