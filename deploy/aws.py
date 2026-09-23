"""AWS helpers behind the `make aws-*` targets (SPEC §7 Phase 7).

    python -m deploy.aws estimate PLAN.json   # itemized $/hour of a terraform plan
    python -m deploy.aws smoke                # run `ftq bench` as an ECS task, check it
    python -m deploy.aws task -- CMD ...      # any one-off command (e.g. the loadgen)
    python -m deploy.aws verify-clean         # nothing billable left in the region

Everything shells out to the AWS CLI (profile and region from AWS_PROFILE / AWS_REGION),
so there's no boto3 dependency. Only `smoke` and `task` create anything (one ECS task on hosts
that are already running); `estimate` and `verify-clean` are read-only and free. The
Pricing API is free to call; Cost Explorer is not ($0.01 a request), so it's never used.
"""

import argparse
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REGION = "us-west-2"
STACK_DIR = Path(__file__).parent / "terraform" / "stack"
# Root volume of every host (deploy/terraform/stack/main.tf).
ROOT_VOLUME_GB = 30
HOURS_PER_MONTH = 730  # AWS's own convention for per-month prices


# ---------------------------------------------------------------- estimate (pure logic)


@dataclass(frozen=True)
class Box:
    """One EC2 instance the plan would run."""

    role: str
    instance_type: str


def fleet_from_plan(plan: dict[str, Any]) -> list[Box]:
    """Every instance a `terraform show -json` plan would leave running.

    Standalone `aws_instance`s count once each. An ASG counts `desired_capacity` times,
    with the instance type of the launch template that has the same for_each key (the
    stack pairs them by role). Resources being deleted don't count.
    """
    changes: list[dict[str, Any]] = plan.get("resource_changes", [])
    live = [c for c in changes if c["change"]["actions"] != ["delete"]]
    templates = {
        c.get("index"): c["change"]["after"]["instance_type"]
        for c in live
        if c["type"] == "aws_launch_template"
    }
    boxes: list[Box] = []
    for c in live:
        after = c["change"]["after"]
        if c["type"] == "aws_instance":
            boxes.append(Box(c["name"], after["instance_type"]))
        elif c["type"] == "aws_autoscaling_group":
            role = str(c.get("index", c["name"]))
            boxes += [Box(role, templates[c.get("index")])] * int(after["desired_capacity"])
    return boxes


@dataclass(frozen=True)
class Prices:
    """On-demand USD prices in the region."""

    per_instance_hour: dict[str, float]
    gp3_gb_month: float
    ipv4_hour: float


def estimate_lines(boxes: list[Box], prices: Prices) -> tuple[list[str], float]:
    """Itemized lines and the total $/hour. Every line is something that bills per hour
    while the stack is up; the per-session extras are printed separately."""
    lines: list[str] = []
    total = 0.0
    by_type: dict[str, int] = {}
    for b in boxes:
        by_type[b.instance_type] = by_type.get(b.instance_type, 0) + 1
    for itype, n in sorted(by_type.items()):
        cost = n * prices.per_instance_hour[itype]
        total += cost
        lines.append(
            f"{n:>3} x {itype:<16} @ ${prices.per_instance_hour[itype]:.5f}/h   ${cost:.4f}/h"
        )
    n = len(boxes)
    ebs = n * ROOT_VOLUME_GB * prices.gp3_gb_month / HOURS_PER_MONTH
    ipv4 = n * prices.ipv4_hour
    total += ebs + ipv4
    lines.append(
        f"{n:>3} x {ROOT_VOLUME_GB} GB gp3 root   @ ${prices.gp3_gb_month}/GB-month   ${ebs:.4f}/h"
    )
    lines.append(f"{n:>3} x public IPv4       @ ${prices.ipv4_hour}/h        ${ipv4:.4f}/h")
    return lines, total


# ---------------------------------------------------------------- AWS CLI


def aws(*args: str, region: str = REGION) -> Any:
    """Run an AWS CLI command and return its parsed JSON output."""
    out = subprocess.run(
        ["aws", *args, "--region", region, "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(out) if out.strip() else None


def _price(service: str, filters: dict[str, str]) -> float:
    """The single positive on-demand price matching `filters` (Pricing API, free)."""
    args = ["pricing", "get-products", "--service-code", service, "--filters"]
    args += [f"Type=TERM_MATCH,Field={k},Value={v}" for k, v in filters.items()]
    found: set[float] = set()
    for raw in aws(*args, region="us-east-1")["PriceList"]:
        product = json.loads(raw)
        for offer in product["terms"]["OnDemand"].values():
            for dim in offer["priceDimensions"].values():
                usd = float(dim["pricePerUnit"]["USD"])
                if usd > 0:
                    found.add(usd)
    if len(found) != 1:
        raise SystemExit(f"expected one price for {service} {filters}, got {sorted(found)}")
    return found.pop()


def fetch_prices(instance_types: set[str]) -> Prices:
    per_hour = {
        t: _price(
            "AmazonEC2",
            {
                "instanceType": t,
                "regionCode": REGION,
                "operatingSystem": "Linux",
                "tenancy": "Shared",
                "preInstalledSw": "NA",
                "capacitystatus": "Used",
            },
        )
        for t in sorted(instance_types)
    }
    gp3 = _price(
        "AmazonEC2", {"volumeApiName": "gp3", "regionCode": REGION, "productFamily": "Storage"}
    )
    ipv4 = _price("AmazonVPC", {"regionCode": REGION, "usagetype": "USW2-PublicIPv4:InUseAddress"})
    return Prices(per_hour, gp3, ipv4)


def cmd_estimate(plan_json: Path, hours: list[float]) -> None:
    boxes = fleet_from_plan(json.loads(plan_json.read_text()))
    prices = fetch_prices({b.instance_type for b in boxes})
    lines, total = estimate_lines(boxes, prices)
    print("Cost estimate (on-demand list prices, us-west-2, Pricing API; paid from credits):")
    print("  roles: " + ", ".join(f"{b.role}={b.instance_type}" for b in boxes))
    for line in lines:
        print("  " + line)
    print(f"  {'':-<60}\n  TOTAL while up: ${total:.4f}/hour")
    for h in hours:
        print(f"    {h:g} h session: ${total * h:.2f}")
    print(
        "  Plus, per session: CloudWatch Logs ingestion $0.50/GB (WARNING-level logs: well\n"
        "  under 0.1 GB, < $0.05); the image in ECR ~$0.10/GB-month until `make aws-down`\n"
        "  deletes it. No NAT gateway, no load balancer, no cross-AZ traffic. $0 until apply."
    )


# ---------------------------------------------------------------- smoke


def _stack_outputs() -> dict[str, Any]:
    out = subprocess.run(
        ["terraform", f"-chdir={STACK_DIR}", "output", "-json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return {k: v["value"] for k, v in json.loads(out).items()}


def _wait(what: str, done: Any, timeout: float, interval: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not done():
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(interval)


def _wait_for_services(cluster: str) -> None:
    def steady() -> bool:
        svc = aws("ecs", "describe-services", "--cluster", cluster, "--services", "redis", "worker")
        state = {s["serviceName"]: (s["runningCount"], s["desiredCount"]) for s in svc["services"]}
        print(f"  services (running, desired): {state}", flush=True)
        return all(r == d for r, d in state.values())

    _wait("services to reach their desired count", steady, timeout=600)


def _log_lines(task_id: str) -> list[str]:
    """Every line the task's container logged, following CloudWatch's pagination."""
    lines: list[str] = []
    token: str | None = None
    while True:
        args = [
            "logs", "get-log-events",
            "--log-group-name", "/ftq/loadgen",
            "--log-stream-name", f"loadgen/loadgen/{task_id}",
            "--start-from-head",
        ]  # fmt: skip
        if token:
            args += ["--next-token", token]
        page = aws(*args)
        lines += [e["message"] for e in page["events"]]
        # The same forward token twice means the end of the stream.
        if not page["events"] or page["nextForwardToken"] == token:
            return lines
        token = page["nextForwardToken"]


def run_one_off(command: list[str], timeout: float) -> int:
    """Run `command` as a one-off loadgen task where the stack places those, wait for it
    to stop, print its output, and return its exit code. Its own `timeout` wrapper
    (task_max_seconds) stops it even if this process dies meanwhile."""
    outputs = _stack_outputs()
    cluster = outputs["cluster"]
    _wait_for_services(cluster)
    overrides = {"containerOverrides": [{"name": "loadgen", "command": command}]}
    task = aws(
        "ecs", "run-task",
        "--cluster", cluster,
        "--task-definition", outputs["loadgen_task_definition"],
        "--launch-type", "EC2",
        "--placement-constraints", f"type=memberOf,expression={outputs['loadgen_placement']}",
        "--overrides", json.dumps(overrides),
    )  # fmt: skip
    if task["failures"] or not task["tasks"]:
        raise SystemExit(f"run-task failed: {task['failures']}")
    arn = task["tasks"][0]["taskArn"]
    task_id = arn.rsplit("/", 1)[-1]
    print(f"  started {task_id}: {' '.join(command)}", flush=True)

    def describe() -> dict[str, Any]:
        found: dict[str, Any] = aws("ecs", "describe-tasks", "--cluster", cluster, "--tasks", arn)
        return dict(found["tasks"][0])

    _wait("the task to stop", lambda: describe()["lastStatus"] == "STOPPED", timeout + 300)
    t = describe()
    print("  --- task output ---")
    for line in _log_lines(task_id):
        print("  " + line)
    code = t["containers"][0].get("exitCode")
    print(f"  exit code: {code} ({t.get('stoppedReason', '')})")
    return -1 if code is None else int(code)


def cmd_smoke(jobs: int, timeout: float) -> None:
    """End-to-end check of a deployed stack: every service at its desired count, then
    `ftq bench --jobs N` as a one-off task. It enqueues, waits for completion, and checks
    exactly-once (exit 0 only if every job completed once)."""
    command = ["ftq", "bench", "--jobs", str(jobs), "--timeout", str(int(timeout))]
    if run_one_off(command, timeout) != 0:
        raise SystemExit(1)


# ---------------------------------------------------------------- verify-clean


def _count(label: str, n: int, failures: list[str]) -> None:
    ok = n == 0
    print(f"  [{'ok' if ok else 'FAIL'}] {label}: {n}")
    if not ok:
        failures.append(label)


def cmd_verify_clean() -> None:
    """Region-wide: nothing that bills by the hour or by the GB may be left. Checks the
    whole region, not just tagged resources, so a resource that missed its tag is found
    too. (This account runs nothing else.)"""
    failures: list[str] = []
    live = "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down"
    instances = aws("ec2", "describe-instances", "--filters", live)
    n = sum(len(r["Instances"]) for r in instances["Reservations"])
    _count("EC2 instances (not terminated)", n, failures)
    _count("EBS volumes", len(aws("ec2", "describe-volumes")["Volumes"]), failures)
    enis = aws("ec2", "describe-network-interfaces")["NetworkInterfaces"]
    _count("network interfaces", len(enis), failures)
    _count("Elastic IPs", len(aws("ec2", "describe-addresses")["Addresses"]), failures)
    nat = aws("ec2", "describe-nat-gateways", "--filter", "Name=state,Values=pending,available")
    _count("NAT gateways", len(nat["NatGateways"]), failures)
    lbs = aws("elbv2", "describe-load-balancers")["LoadBalancers"]
    _count("load balancers", len(lbs), failures)
    asgs = aws("autoscaling", "describe-auto-scaling-groups")["AutoScalingGroups"]
    _count("auto scaling groups", len(asgs), failures)
    arns = aws("ecs", "list-clusters")["clusterArns"]
    active = 0
    if arns:
        clusters = aws("ecs", "describe-clusters", "--clusters", *arns)["clusters"]
        active = sum(1 for c in clusters if c["status"] == "ACTIVE")
    _count("active ECS clusters", active, failures)
    repos = aws("ecr", "describe-repositories")["repositories"]
    images = sum(
        len(aws("ecr", "list-images", "--repository-name", r["repositoryName"])["imageIds"])
        for r in repos
    )
    _count("ECR images", images, failures)
    groups = aws("logs", "describe-log-groups", "--log-group-name-prefix", "/ftq")["logGroups"]
    _count("CloudWatch log groups /ftq*", len(groups), failures)
    state = subprocess.run(
        ["terraform", f"-chdir={STACK_DIR}", "state", "list"],
        capture_output=True,
        text=True,
    )
    in_state = len(state.stdout.split()) if state.returncode == 0 else 0
    _count("resources in the stack's Terraform state", in_state, failures)
    if failures:
        raise SystemExit(f"NOT CLEAN: {', '.join(failures)}")
    print("CLEAN: nothing billable left in " + REGION)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m deploy.aws", description=__doc__.split("\n")[0]
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("estimate", help="itemized cost of a plan (terraform show -json)")
    p.add_argument("plan_json", type=Path)
    p.add_argument("--hours", type=float, nargs="+", default=[1, 3, 5])
    p = sub.add_parser("smoke", help="run ftq bench on the deployed stack (BILLABLE: stack up)")
    p.add_argument("--jobs", type=int, default=20000)
    p.add_argument("--timeout", type=float, default=300)
    p = sub.add_parser("task", help="run any command as a one-off task (BILLABLE: stack up)")
    p.add_argument("--timeout", type=float, default=900)
    p.add_argument("command", nargs=argparse.REMAINDER, help="after --")
    sub.add_parser("verify-clean", help="fail unless nothing billable is left")
    args = parser.parse_args(argv)
    if args.cmd == "estimate":
        cmd_estimate(args.plan_json, args.hours)
    elif args.cmd == "smoke":
        cmd_smoke(args.jobs, args.timeout)
    elif args.cmd == "task":
        # Only the leading separator is argparse's; a later "--" belongs to the command.
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        if not command:
            parser.error("task needs a command after --")
        if run_one_off(command, args.timeout) != 0:
            raise SystemExit(1)
    else:
        cmd_verify_clean()


if __name__ == "__main__":
    main()
