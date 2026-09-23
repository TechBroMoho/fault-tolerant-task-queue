"""The cost estimate shown before every apply (deploy/aws.py): which instances a
Terraform plan would run, and what they cost per hour. Pure logic on a synthetic plan."""

from typing import Any

import pytest

from deploy.aws import Box, Prices, estimate_lines, fleet_from_plan


def _change(rtype: str, name: str, after: dict[str, Any], index: str | None = None,
            actions: list[str] | None = None) -> dict[str, Any]:  # fmt: skip
    c: dict[str, Any] = {
        "type": rtype,
        "name": name,
        "change": {"actions": actions or ["create"], "after": after},
    }
    if index is not None:
        c["index"] = index
    return c


def _plan(worker_hosts: int, loadgen_hosts: int) -> dict[str, Any]:
    changes = [
        _change("aws_instance", "redis", {"instance_type": "m7i-flex.large"}),
        _change("aws_launch_template", "host", {"instance_type": "c7i-flex.large"}, "worker"),
        _change("aws_launch_template", "host", {"instance_type": "c7i-flex.large"}, "loadgen"),
        _change("aws_autoscaling_group", "host", {"desired_capacity": worker_hosts}, "worker"),
        _change("aws_security_group", "cluster", {}),
    ]
    if loadgen_hosts:
        changes.append(
            _change("aws_autoscaling_group", "host", {"desired_capacity": loadgen_hosts}, "loadgen")
        )
    return {"resource_changes": changes}


def test_small_footprint_is_one_redis_host_and_one_worker_host() -> None:
    assert fleet_from_plan(_plan(1, 0)) == [
        Box("redis", "m7i-flex.large"),
        Box("worker", "c7i-flex.large"),
    ]


def test_asgs_count_desired_capacity_times() -> None:
    boxes = fleet_from_plan(_plan(6, 1))
    assert len(boxes) == 8
    assert sum(b.role == "worker" for b in boxes) == 6
    assert sum(b.role == "loadgen" for b in boxes) == 1


def test_deleted_resources_cost_nothing() -> None:
    plan = {
        "resource_changes": [
            _change(
                "aws_instance", "redis", {"instance_type": "m7i-flex.large"}, actions=["delete"]
            )
        ]
    }
    assert fleet_from_plan(plan) == []


def test_estimate_matches_the_hand_computed_option_b() -> None:
    """The pre-flight's option B: $0.1971/h (PROGRESS, Phase 7 pre-flight)."""
    prices = Prices({"m7i-flex.large": 0.09576, "c7i-flex.large": 0.08479}, 0.08, 0.005)
    lines, total = estimate_lines(fleet_from_plan(_plan(1, 0)), prices)
    assert total == pytest.approx(0.09576 + 0.08479 + 2 * 30 * 0.08 / 730 + 2 * 0.005)
    assert round(total, 4) == 0.1971
    assert len(lines) == 4  # 2 instance types + EBS + IPv4
