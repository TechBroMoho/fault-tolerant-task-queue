"""`make aws-verify-clean` (deploy/aws.py): the check that ends every AWS session. The AWS
CLI and Terraform are faked (pure logic: what counts as clean). A check that can't look
must fail, never pass."""

import subprocess
from typing import Any

import pytest

from deploy import aws
from deploy.aws import state_resource_count

EMPTY: dict[str, Any] = {
    "describe-instances": {"Reservations": []},
    "describe-volumes": {"Volumes": []},
    "describe-network-interfaces": {"NetworkInterfaces": []},
    "describe-addresses": {"Addresses": []},
    "describe-nat-gateways": {"NatGateways": []},
    "describe-load-balancers": {"LoadBalancers": []},
    "describe-auto-scaling-groups": {"AutoScalingGroups": []},
    "list-clusters": {"clusterArns": []},
    "describe-repositories": {"repositories": []},
    "describe-log-groups": {"logGroups": []},
}


def _fake(
    monkeypatch: pytest.MonkeyPatch, answers: dict[str, Any], tf: tuple[int, str, str]
) -> None:
    monkeypatch.setattr(aws, "aws", lambda *args, region="": answers[args[1]])
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a[0], *tf))


def test_the_state_count_fails_closed() -> None:
    assert state_resource_count(0, "aws_instance.redis\naws_ecs_cluster.this\n", "") == 2
    assert state_resource_count(0, "", "") == 0  # applied, then destroyed
    assert state_resource_count(1, "", "No state file was found!\n...") == 0  # never applied
    assert state_resource_count(1, "", "Error acquiring the state lock") is None


def test_an_empty_account_is_clean(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    _fake(monkeypatch, EMPTY, (0, "", ""))
    aws.cmd_verify_clean()
    assert "CLEAN" in capsys.readouterr().out


def test_a_leftover_instance_is_not_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    answers = {**EMPTY, "describe-instances": {"Reservations": [{"Instances": [{}]}]}}
    _fake(monkeypatch, answers, (0, "", ""))
    with pytest.raises(SystemExit, match="NOT CLEAN: EC2 instances"):
        aws.cmd_verify_clean()


def test_an_unreadable_terraform_state_is_not_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fail-open found in the Phase 6-8 review: a state list that errored was 0."""
    _fake(monkeypatch, EMPTY, (1, "", "Error: Error acquiring the state lock"))
    with pytest.raises(SystemExit, match="Terraform state unreadable"):
        aws.cmd_verify_clean()
