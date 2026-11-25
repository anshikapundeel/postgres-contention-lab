import unittest

from run import report


class ReportTest(unittest.TestCase):
    def test_single_layout_reports_each_repeat(self):
        for layout in ("hot-row", "per-client"):
            with self.subTest(layout=layout):
                result = report({
                    (1, 4, 5): {layout: [100, 50, 10, 15, 30, 8, 12, 24]},
                    (2, 4, 5): {layout: [120, 60, 8, 14, 25, 0, 0, 0]},
                })
                self.assertIn("| 1 | 4 | 5 | 100 | 50.00 | 30.00 | 50.0% |", result)
                self.assertIn("| 2 | 4 | 5 | 120 | 60.00 | 25.00 | unobserved |", result)

    def test_paired_comparison(self):
        result = report({(1, 4, 5): {
            "hot-row": [100, 50, 10, 15, 30, 8, 12, 24],
            "per-client": [200, 100, 5, 8, 10, 8, 0, 24],
        }})
        self.assertIn("| 0.50 | 3.00 / 3.00 / 3.00 | 50.0% / 0.0% |", result)
