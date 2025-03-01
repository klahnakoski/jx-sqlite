from datetime import datetime
from unittest import TestCase

from dateutil.tz import tzutc

from jx_base.expressions import Literal
from jx_python import jx

from mo_dots import to_data
from mo_testing import add_error_reporting


@add_error_reporting
class TestJxImmediate(TestCase):
    def test_aws_complex(self):
        data = [
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015700",
                "Timestamp": datetime(2025, 2, 28, 22, 35, 56, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015700",
                "Timestamp": datetime(2025, 2, 27, 22, 35, 27, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015700",
                "Timestamp": datetime(2025, 2, 26, 22, 35, 3, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015600",
                "Timestamp": datetime(2025, 2, 26, 7, 2, 38, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015700",
                "Timestamp": datetime(2025, 2, 26, 5, 18, 35, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015700",
                "Timestamp": datetime(2025, 2, 25, 5, 17, 59, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015600",
                "Timestamp": datetime(2025, 2, 24, 11, 18, 53, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015700",
                "Timestamp": datetime(2025, 2, 24, 5, 19, 4, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015700",
                "Timestamp": datetime(2025, 2, 23, 5, 18, 47, tzinfo=tzutc()),
            },
            {
                "AvailabilityZone": "us-east-1d",
                "InstanceType": "m8g.medium",
                "ProductDescription": "Linux/UNIX",
                "SpotPrice": "0.015700",
                "Timestamp": datetime(2025, 2, 22, 5, 18, 38, tzinfo=tzutc()),
            },
        ]
        result = jx.run({
            "from": Literal(data),
            "window": [{
                "name": "expire",
                "value": {"coalesce": [{"rows": {"Timestamp": 1}}, {"date": "eod"}]},
                "edges": ["AvailabilityZone", "InstanceType"],
                "sort": "timestamp",
            }],
        })
        print(result)
