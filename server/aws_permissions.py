"""What the deployed app's IAM principal must be allowed to do, read out of its calls.

The defect this exists to prevent has already happened here. A permission
document named something *plausible* rather than the thing the code reaches for,
and every test agreed with the document because the test carried its own copy of
the same list. A hand-written expectation cannot catch a hand-written grant:
they are the same sentence written twice, and the second copy is not evidence.

So no action is written down anywhere in this file. The required set is
recovered from the source of the round that needs it, the same way
``_coordination_runtime_grants`` recovers its target from the accessor the
runtime uses. Rename a call, delete a lane, add a describe, and the required set
moves with it -- including in the direction that matters, where a new call
appears and no policy grants it.

What *is* written down is which modules implement which round. That is a
structural fact rather than a permission claim, it is short enough to read, and
two invariants keep it from rotting:

*   :func:`assert_entry_points_resolve` -- every module named here still exports
    the symbol recorded against it.
*   :func:`unclassified_aws_modules` -- every module under ``server/`` that
    opens an AWS client is claimed by exactly one of these lists. A new one
    cannot appear unnoticed, which is the hole a transitive import closure
    looked like it closed and did not: ``aws_credential_probe`` imports one enum
    from ``readiness`` and thereby reaches the entire tree, so a closure would
    have demanded the installer's permissions of the deployed app.

Run ``python -m server.aws_permissions`` to print the plan an operator would
have to satisfy.

Two honest limits, stated because a guard nobody distrusts is a guard nobody
reads:

*   This recovers *actions*, not resources or conditions. Whether
    ``rds:DeleteDBInstance`` is scoped to the per-bout ``adsc-``/``adrc-``
    prefixes is not knowable from a call site, and stays a review question.
*   An IAM action is not always spelled like the API operation that needs it.
    Every service reached here spells them alike, and :func:`operation_actions`
    is the single place to teach it otherwise.
"""

from __future__ import annotations

import ast
import importlib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from botocore import xform_name
from botocore.session import get_session as botocore_session

SERVER_DIRECTORY = Path(__file__).resolve().parent


@dataclass(frozen=True)
class RuntimeSurface:
    """One thing the deployed app does, and the modules that do it to AWS."""

    name: str
    modules: tuple[str, ...]
    #: A symbol that must still exist in the first module, so a rename or a
    #: deletion breaks this list rather than quietly emptying it.
    entry_point: str


#: The AWS-touching surface of each round the deployed app can run against AWS.
#: Round 4 and Round 6 are Databricks-only lanes and open no AWS client at all,
#: which is why they are absent rather than empty.
#:
#: ``aws_auth`` is in every one of them because `validate_app_aws_environment`
#: is the first AWS call the process makes and every round depends on it having
#: succeeded.
ROUND_SURFACES: Mapping[int, RuntimeSurface] = {
    1: RuntimeSurface("round1_wake", ("targets", "aws_auth"), "TargetResolver"),
    2: RuntimeSurface(
        "round2_safe_change", ("safe_change_live", "aws_auth"), "build_safe_change_engine"
    ),
    3: RuntimeSurface(
        # `recovery_live` subclasses the Round 2 adapters and reaches RDS
        # through them, so the restore lives in one file and the identity
        # assertion that guards it in another. A set naming only the first
        # would be wrong in the direction that costs a demo.
        "round3_recovery",
        ("recovery_live", "safe_change_live", "aws_auth"),
        "build_recovery_engine",
    ),
    5: RuntimeSurface(
        "round5_connection_spike",
        ("connection_spike_live", "aws_auth"),
        "connection_spike_live_config_from_manifest",
    ),
}

#: The AWS work the deployed app does for no round in particular: the startup
#: credential probe, and the orphan sweep that deletes what a round left behind.
#: Separate from any round because the sweep runs before a round is chosen -- and
#: because a round that measures correctly and then cannot delete what it created
#: leaks billable resources just the same.
APP_RUNTIME_SURFACES: tuple[RuntimeSurface, ...] = (
    RuntimeSurface("startup_credential_probe", ("aws_credential_probe",), "probe_once"),
    RuntimeSurface("startup_orphan_sweep", ("reap",), "AwsOrphanDeleter"),
    RuntimeSurface("installation_presence", ("reconcile",), "collect_observed"),
)

#: Modules that open an AWS client and never do so inside the deployed app.
#: `lifecycle` is the installer and the doctor, `cli` is their entry point; both
#: run on an operator's laptop as an Identity Center role. Listed rather than
#: ignored, so that `unclassified_aws_modules` stays exhaustive.
OPERATOR_ONLY_MODULES: tuple[str, ...] = ("lifecycle", "cli")

#: The rounds this reports on. Round 5 is included because its gaps are worth
#: knowing even where it cannot run.
AWS_ROUNDS: tuple[int, ...] = (1, 2, 3, 5)


@dataclass(frozen=True)
class AwsCall:
    """One AWS API operation, and the line of ours that issues it."""

    service: str
    operation: str
    module: str
    line: int

    @property
    def action(self) -> str:
        return f"{self.service}:{self.operation}"

    @property
    def call_site(self) -> str:
        return f"server/{self.module}.py:{self.line}"


#: Tagging a resource as it is created is authorized by a *second* action, named
#: after the service's own tag API and never mentioned at the call site. Round 2
#: and Round 3 both create tagged artifacts, so leaving this out would have
#: produced a required set that looked complete and would have failed on the
#: first ``Tags=`` a policy did not cover. A fact about IAM rather than about
#: this codebase -- what is derived is *which* calls tag, which is read below
#: from the arguments each one passes.
TAG_ON_CREATE_ACTIONS: Mapping[str, str] = {
    "rds": "rds:AddTagsToResource",
    "ec2": "ec2:CreateTags",
    "secretsmanager": "secretsmanager:TagResource",
}

#: The argument names that mean "tag this on the way in", per service.
TAG_ARGUMENTS: Mapping[str, str] = {
    "rds": "Tags",
    "ec2": "TagSpecifications",
    "secretsmanager": "Tags",
}


def operation_actions(service: str, operation: str) -> frozenset[str]:
    """The IAM actions one API operation requires.

    A function rather than an f-string because the identity mapping is a
    property of the services this project reaches, not of IAM. The moment
    something here needs ``iam:PassRole`` alongside its own operation, this is
    where that belongs -- and callers written against an f-string would have had
    nowhere to put it.
    """

    return frozenset({f"{service}:{operation}"})


@cache
def _available_services() -> frozenset[str]:
    return frozenset(botocore_session().get_available_services())


@cache
def _operations(service: str) -> Mapping[str, str]:
    """``describe_db_clusters`` -> ``DescribeDBClusters``, from botocore's model.

    botocore's own mapping rather than a hand-rolled case conversion, because
    ``DescribeDBInstanceAutomatedBackups`` and ``DescribeDBProxyTargetGroups``
    are exactly the names a naive substitution gets subtly wrong, and a required
    action that is misspelled is satisfied by nothing and reads as a gap that
    is not real.
    """

    model = botocore_session().get_service_model(service)
    return {xform_name(name): name for name in model.operation_names}


def _dotted(node: ast.expr) -> str | None:
    """``self._rds`` -> ``"self._rds"``; anything less simple -> ``None``."""

    parts: list[str] = []
    current: ast.expr = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    parts.append(current.id)
    return ".".join(reversed(parts))


def _service_of(node: ast.expr, bindings: Mapping[str, str]) -> str | None:
    """Which AWS service this expression is a client for, or ``None``.

    Four shapes, all of them ones this codebase actually writes:
    ``session.client("rds")`` inline, a name bound to one earlier, ``self._rds``
    bound to one earlier, and ``clients.rds`` where the attribute is named after
    the service it holds.
    """

    if isinstance(node, ast.Call):
        function = node.func
        if isinstance(function, ast.Attribute) and function.attr == "client" and node.args:
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                return first.value
        return None
    if isinstance(node, ast.Name):
        return bindings.get(node.id)
    if isinstance(node, ast.Attribute):
        bound = bindings.get(_dotted(node) or "")
        if bound is not None:
            return bound
        if node.attr in _available_services():
            return node.attr
    return None


def _client_bindings(tree: ast.AST) -> dict[str, str]:
    """Every name in this module that holds a client, and for which service."""

    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            service = _service_of(node.value, bindings)
            if service is None:
                continue
            for target in node.targets:
                name: str | None = None
                if isinstance(target, ast.Name):
                    name = target.id
                elif isinstance(target, ast.Attribute):
                    name = _dotted(target)
                if name:
                    bindings[name] = service
        elif isinstance(node, ast.keyword) and node.arg:
            service = _service_of(node.value, bindings)
            if service is not None:
                bindings[node.arg] = service
    return bindings


def _dispatch_helpers(tree: ast.AST) -> dict[str, tuple[str, int, str]]:
    """Helpers that take an operation *name* and issue it against a fixed service.

    ``_issue_delete("delete_db_instance", ...)`` is the whole reason this
    exists: the string is a call to RDS and nothing about the syntax says so.
    The helper is recognised by what its body does -- ``self._call("rds",
    method)`` where ``method`` is one of its own parameters -- so a second
    helper of the same shape is picked up without being named here.

    Returns helper name -> (service, positional index at the call site,
    keyword name).
    """

    helpers: dict[str, tuple[str, int, str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        parameters = [argument.arg for argument in node.args.args]
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Call):
                continue
            function = inner.func
            if not isinstance(function, ast.Attribute) or function.attr != "_call":
                continue
            if len(inner.args) < 2:
                continue
            service, operation = inner.args[0], inner.args[1]
            if not (isinstance(service, ast.Constant) and isinstance(service.value, str)):
                continue
            if not isinstance(operation, ast.Name) or operation.id not in parameters:
                continue
            index = parameters.index(operation.id)
            # A bound method drops `self`, so the caller's positional index is
            # one lower than the definition's.
            if parameters and parameters[0] == "self":
                index -= 1
            helpers[node.name] = (service.value, index, operation.id)
    return helpers


def _dict_literal_keys(tree: ast.AST) -> dict[str, set[str]]:
    """String keys of every dict this module builds and later splats into a call.

    ``restore_arguments = {... "Tags": ...}`` followed by ``**restore_arguments``
    is how both restore lanes pass their tags, and without reading the dict the
    tag-on-create action below would be invisible for exactly the two calls that
    most need it.
    """

    keys: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign | ast.AnnAssign):
            # `restore_arguments: dict[str, Any] = {...}` is an `AnnAssign` with
            # one target; a bare assignment is an `Assign` with several.
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name) and isinstance(node.value, ast.Dict):
                    keys.setdefault(target.id, set()).update(
                        key.value
                        for key in node.value.keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    )
                elif (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Name)
                    and isinstance(target.slice, ast.Constant)
                    and isinstance(target.slice.value, str)
                ):
                    keys.setdefault(target.value.id, set()).add(target.slice.value)
    return keys


def _argument_names(node: ast.Call, dict_keys: Mapping[str, set[str]]) -> set[str]:
    names: set[str] = set()
    for entry in node.keywords:
        if entry.arg is not None:
            names.add(entry.arg)
        elif isinstance(entry.value, ast.Name):
            names |= dict_keys.get(entry.value.id, set())
        elif isinstance(entry.value, ast.Dict):
            names |= {
                key.value
                for key in entry.value.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
    return names


@cache
def module_calls(module: str) -> tuple[AwsCall, ...]:
    """Every AWS operation the given ``server/`` module issues."""

    path = SERVER_DIRECTORY / f"{module}.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    bindings = _client_bindings(tree)
    helpers = _dispatch_helpers(tree)
    dict_keys = _dict_literal_keys(tree)
    found: set[AwsCall] = set()

    def record(
        service: str,
        method: str,
        line: int,
        arguments: set[str] | None = None,
    ) -> None:
        if service not in _available_services():
            return
        operation = _operations(service).get(method)
        if operation is None:
            return
        found.add(AwsCall(service, operation, module, line))
        tag_argument = TAG_ARGUMENTS.get(service)
        if arguments and tag_argument in arguments:
            tag_action = TAG_ON_CREATE_ACTIONS[service]
            found.add(AwsCall(service, tag_action.split(":", 1)[1], module, line))

    for node in ast.walk(tree):
        # `session.client("rds").describe_db_clusters(...)`, `self._rds.foo(...)`,
        # and `clients.rds.foo` handed to a runner rather than called in place.
        if isinstance(node, ast.Attribute):
            service = _service_of(node.value, bindings)
            if service is not None:
                record(service, node.attr, node.lineno)
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        arguments = _argument_names(node, dict_keys)
        if isinstance(function, ast.Attribute):
            # A direct client call: `session.client("rds").create_db_instance(...)`
            # or `self._rds.create_db_instance(...)`.
            service = _service_of(function.value, bindings)
            if service is not None:
                record(service, function.attr, node.lineno, arguments)
            # A paginator issues the operation it names, not one called
            # `get_paginator`.
            if function.attr == "get_paginator" and node.args:
                service = _service_of(function.value, bindings)
                first = node.args[0]
                if service and isinstance(first, ast.Constant) and isinstance(first.value, str):
                    record(service, first.value, node.lineno)
            if function.attr == "_call" and len(node.args) >= 2:
                service_argument, operation_argument = node.args[0], node.args[1]
                if (
                    isinstance(service_argument, ast.Constant)
                    and isinstance(service_argument.value, str)
                    and isinstance(operation_argument, ast.Constant)
                    and isinstance(operation_argument.value, str)
                ):
                    record(
                        service_argument.value,
                        operation_argument.value,
                        node.lineno,
                        arguments,
                    )
        called: str | None = None
        if isinstance(function, ast.Attribute):
            called = function.attr
        elif isinstance(function, ast.Name):
            called = function.id
        if called in helpers:
            service, index, keyword = helpers[str(called)]
            chosen: ast.expr | None = None
            if 0 <= index < len(node.args):
                chosen = node.args[index]
            for entry in node.keywords:
                if entry.arg == keyword:
                    chosen = entry.value
            if isinstance(chosen, ast.Constant) and isinstance(chosen.value, str):
                record(service, chosen.value, node.lineno, arguments)

    return tuple(sorted(found, key=lambda call: (call.service, call.operation, call.line)))


def surface_calls(surface: RuntimeSurface) -> tuple[AwsCall, ...]:
    calls: set[AwsCall] = set()
    for module in surface.modules:
        calls.update(module_calls(module))
    return tuple(sorted(calls, key=lambda call: (call.service, call.operation, call.module)))


def round_calls(round_number: int) -> tuple[AwsCall, ...]:
    return surface_calls(ROUND_SURFACES[round_number])


def app_runtime_calls() -> tuple[AwsCall, ...]:
    """The startup probe and the orphan sweep, which run for every round."""

    calls: set[AwsCall] = set()
    for surface in APP_RUNTIME_SURFACES:
        calls.update(surface_calls(surface))
    return tuple(sorted(calls, key=lambda call: (call.service, call.operation, call.module)))


def actions(calls: Iterable[AwsCall]) -> frozenset[str]:
    required: set[str] = set()
    for call in calls:
        required |= operation_actions(call.service, call.operation)
    return frozenset(required)


def round_actions(round_number: int) -> frozenset[str]:
    """Every IAM action one round's code path needs, arm through cleanup."""

    return actions(round_calls(round_number))


def service_actions(required: Iterable[str], service: str) -> frozenset[str]:
    return frozenset(action for action in required if action.startswith(f"{service}:"))


def call_sites(calls: Iterable[AwsCall], action: str) -> tuple[str, ...]:
    """Where a required action comes from, for a failure message worth reading."""

    return tuple(sorted({call.call_site for call in calls if call.action == action}))


def classified_modules() -> frozenset[str]:
    named: set[str] = set(OPERATOR_ONLY_MODULES)
    for surface in (*ROUND_SURFACES.values(), *APP_RUNTIME_SURFACES):
        named.update(surface.modules)
    return frozenset(named)


def aws_calling_modules() -> frozenset[str]:
    """Every module under ``server/`` that issues an AWS API call."""

    return frozenset(
        path.stem for path in sorted(SERVER_DIRECTORY.glob("*.py")) if module_calls(path.stem)
    )


def unclassified_aws_modules() -> tuple[str, ...]:
    """AWS-calling modules nobody has said whether the deployed app runs.

    The exhaustiveness half of this file. Without it, a new module that talks to
    AWS is invisible to every check here and its permissions are discovered in
    the room.
    """

    return tuple(sorted(aws_calling_modules() - classified_modules()))


def assert_entry_points_resolve() -> None:
    """Every module named above still exists and still exports what it claims."""

    for surface in (*ROUND_SURFACES.values(), *APP_RUNTIME_SURFACES):
        head = surface.modules[0]
        imported = importlib.import_module(f"server.{head}")
        if not hasattr(imported, surface.entry_point):
            raise RuntimeError(
                f"server.{head}.{surface.entry_point} is the recorded entry point for "
                f"{surface.name} and no longer exists; repoint it at whatever replaced "
                "it rather than deleting the entry"
            )


def _lines() -> Iterator[str]:
    surfaces: Sequence[tuple[str, tuple[AwsCall, ...]]] = [
        *(
            (f"Round {number} ({', '.join(ROUND_SURFACES[number].modules)})", round_calls(number))
            for number in AWS_ROUNDS
        ),
        ("Startup probe and orphan sweep", app_runtime_calls()),
    ]
    for title, calls in surfaces:
        yield title
        for action in sorted({call.action for call in calls}):
            sites = ", ".join(call_sites(calls, action))
            yield f"  {action:<40} {sites}"


if __name__ == "__main__":  # pragma: no cover - operator convenience
    for line in _lines():
        print(line)


__all__ = [
    "APP_RUNTIME_SURFACES",
    "AWS_ROUNDS",
    "OPERATOR_ONLY_MODULES",
    "ROUND_SURFACES",
    "AwsCall",
    "RuntimeSurface",
    "actions",
    "app_runtime_calls",
    "assert_entry_points_resolve",
    "aws_calling_modules",
    "call_sites",
    "classified_modules",
    "module_calls",
    "operation_actions",
    "round_actions",
    "round_calls",
    "service_actions",
    "surface_calls",
    "unclassified_aws_modules",
]
