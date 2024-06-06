import unittest
import json

class TestVisualizationConfig(unittest.TestCase):

    def test_visualization_config(self):
        visualization_config = {
            "title": "Customer Dashboard",
            "visualizations": [
                {
                    "type": "pie",
                    "title": "Customer Segments",
                    "source": "customer_index",
                    "field": "segment"
                },
                {
                    "type": "bar",
                    "title": "Customer Distribution by Age",
                    "source": "customer_index",
                    "field": "age"
                }
            ]
        }
        self.assertEqual(visualization_config["title"], "Customer Dashboard")
        self.assertEqual(len(visualization_config["visualizations"]), 2)

if __name__ == '__main__':
    unittest.main()
